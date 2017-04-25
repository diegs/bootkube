// Package recovery provides tooling to help with control plane disaster recovery. Recover() uses a
// Backend to extract the control plane from a store, such as etcd, and use those to write assets
// that can be used by `bootkube start` to reboot the control plane.
//
// The recovery tool assumes that the component names for the control plane elements are the same as
// what is output by `bootkube render`. The `bootkube start` command also makes this assumption.
// It also assumes that kubeconfig on the kubelet is located at /etc/kubernetes/kubeconfig, though
// that can be changed in the bootstrap manifests that are rendered.
package recovery

import (
	"context"
	"fmt"
	"io/ioutil"
	"path"
	"reflect"
	"strings"

	"github.com/ghodss/yaml"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	policyv1beta1 "k8s.io/client-go/pkg/apis/policy/v1beta1"

	"github.com/kubernetes-incubator/bootkube/pkg/asset"
)

const (
	kubeletKubeConfigPath = "/etc/kubernetes/kubeconfig"
)

var (
	// bootstrapComponents contains the names of the components that we will extract to construct the
	// temporary bootstrap control plane.
	bootstrapComponents = map[string]bool{
		"kube-apiserver":          true,
		"kube-controller-manager": true,
		"kube-scheduler":          true,
		"pod-checkpointer":        true,
	}
	// kubeConfigComponents contains the names of the bootstrap pods that need to add a --kubeconfig
	// flag to run in non-self-hosted mode.
	kubeConfigComponents = map[string]bool{
		"kube-controller-manager": true,
		"kube-scheduler":          true,
	}
	// typeMetas contains a mapping from API object types to the TypeMeta struct that should be
	// populated for them when they are serialized.
	typeMetas    = make(map[reflect.Type]metav1.TypeMeta)
	metaAccessor = meta.NewAccessor()
)

func init() {
	addTypeMeta := func(obj runtime.Object, gv schema.GroupVersion) {
		t := reflect.TypeOf(obj)
		typeMetas[t] = metav1.TypeMeta{
			APIVersion: gv.String(),
			Kind:       t.Elem().Name(),
		}
	}
	addTypeMeta(&v1.ConfigMap{}, v1.SchemeGroupVersion)
	addTypeMeta(&v1beta1.DaemonSet{}, v1beta1.SchemeGroupVersion)
	addTypeMeta(&v1beta1.Deployment{}, v1beta1.SchemeGroupVersion)
	addTypeMeta(&policyv1beta1.PodDisruptionBudget{}, policyv1beta1.SchemeGroupVersion)
	addTypeMeta(&v1.Pod{}, v1.SchemeGroupVersion)
	addTypeMeta(&v1.Secret{}, v1.SchemeGroupVersion)
	addTypeMeta(&v1.Service{}, v1.SchemeGroupVersion)
	addTypeMeta(&v1.ServiceAccount{}, v1.SchemeGroupVersion)
}

// Backend defines an interface for any backend that can populate a controlPlane struct.
type Backend interface {
	read(context.Context) (*controlPlane, error)
}

// controlPlane holds the control plane objects that are recovered from a backend.
type controlPlane struct {
	configMaps           v1.ConfigMapList
	daemonSets           v1beta1.DaemonSetList
	deployments          v1beta1.DeploymentList
	podDisruptionBudgets policyv1beta1.PodDisruptionBudgetList
	secrets              v1.SecretList
	services             v1.ServiceList
	serviceAccounts      v1.ServiceAccountList
}

// Recover recovers a control plane using the provided backend and kubeConfigPath, returning assets
// for the existing control plane and a bootstrap control plane that can be used with `bootkube
// start` to re-bootstrap the control plane.
func Recover(ctx context.Context, backend Backend, kubeConfigPath string) (asset.Assets, error) {
	cp, err := backend.read(ctx)
	if err != nil {
		return nil, err
	}

	as, err := cp.renderSelfHosted()
	if err != nil {
		return nil, err
	}

	bs, err := cp.renderBootstrap(kubeConfigPath)
	if err != nil {
		return nil, err
	}
	as = append(as, bs...)

	ks, err := renderKubeConfig(kubeConfigPath)
	if err != nil {
		return nil, err
	}
	as = append(as, ks...)

	return as, nil
}

// renderSelfHosted returns assets for the self-hosted control plane objects. These are output
// without modification from what the backend recovered.
func (cp *controlPlane) renderSelfHosted() (asset.Assets, error) {
	var as asset.Assets
	if objAs, err := serializeListObjToYAML(&cp.configMaps); err != nil {
		return nil, err
	} else {
		as = append(as, objAs...)
	}
	if objAs, err := serializeListObjToYAML(&cp.daemonSets); err != nil {
		return nil, err
	} else {
		as = append(as, objAs...)
	}
	if objAs, err := serializeListObjToYAML(&cp.deployments); err != nil {
		return nil, err
	} else {
		as = append(as, objAs...)
	}
	if objAs, err := serializeListObjToYAML(&cp.podDisruptionBudgets); err != nil {
		return nil, err
	} else {
		as = append(as, objAs...)
	}
	if objAs, err := serializeListObjToYAML(&cp.secrets); err != nil {
		return nil, err
	} else {
		as = append(as, objAs...)
	}
	if objAs, err := serializeListObjToYAML(&cp.services); err != nil {
		return nil, err
	} else {
		as = append(as, objAs...)
	}
	if objAs, err := serializeListObjToYAML(&cp.serviceAccounts); err != nil {
		return nil, err
	} else {
		as = append(as, objAs...)
	}
	return as, nil
}

// renderBootstrap returns assets for a bootstrap control plane that can be used with `bootkube
// start` to re-bootstrap a control plane. These assets are derived from the self-hosted control
// plane that was recovered by the backend, but modified for direct injection into a kubelet.
func (cp *controlPlane) renderBootstrap(kubeConfigPath string) (asset.Assets, error) {
	// Extract pod specs from daemonsets and deployments.
	var pods []*v1.Pod
	for _, ds := range cp.daemonSets.Items {
		if componentName := ds.Labels["component"]; bootstrapComponents[componentName] {
			pod := &v1.Pod{Spec: ds.Spec.Template.Spec}
			if err := setBootstrapPodMetadata(pod, ds.ObjectMeta); err != nil {
				return nil, err
			}
			pods = append(pods, pod)
		}
	}
	for _, ds := range cp.deployments.Items {
		if componentName := ds.Labels["component"]; bootstrapComponents[componentName] {
			pod := &v1.Pod{Spec: ds.Spec.Template.Spec}
			if err := setBootstrapPodMetadata(pod, ds.ObjectMeta); err != nil {
				return nil, err
			}
			pods = append(pods, pod)
		}
	}

	// Fix up the pods.
	var as asset.Assets
	requiredSecrets := make(map[string]bool)
	for _, pod := range pods {
		// Change secret volumes to point to file mounts.
		for i := range pod.Spec.Volumes {
			vol := &pod.Spec.Volumes[i]
			if vol.Secret != nil {
				requiredSecrets[vol.Secret.SecretName] = true
				secretPath := path.Join(asset.BootstrapSecretsDir, vol.Secret.SecretName)
				vol.HostPath = &v1.HostPathVolumeSource{Path: secretPath}
				vol.Secret = nil
			}
		}

		// Make sure the kubeconfig is in the commandline.
		for i, _ := range pod.Spec.Containers {
			cn := &pod.Spec.Containers[i]
			// Assumes the bootkube naming convention is used. Could also just make sure the image uses hyperkube.
			if kubeConfigComponents[cn.Name] {
				cn.Command = append(cn.Command, "--kubeconfig=/kubeconfig/kubeconfig")
				cn.VolumeMounts = append(cn.VolumeMounts, v1.VolumeMount{
					MountPath: "/kubeconfig",
					Name:      "kubeconfig",
					ReadOnly:  true,
				})
			}
		}

		// Add a mount for the kubeconfig.
		pod.Spec.Volumes = append(pod.Spec.Volumes, v1.Volume{
			VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: kubeletKubeConfigPath}},
			Name:         "kubeconfig",
		})

		// Output the pod definition.
		a, err := serializeYAML(path.Join(asset.AssetPathBootstrapManifests, pod.Name+".yaml"), pod)
		if err != nil {
			return nil, err
		}
		as = append(as, a)
	}

	// Output all the required secrets.
	for _, secret := range cp.secrets.Items {
		if requiredSecrets[secret.Name] {
			for key, data := range secret.Data {
				as = append(as, asset.Asset{
					Name: path.Join(asset.AssetPathSecrets, secret.Name, key),
					Data: data,
				})
			}
			delete(requiredSecrets, secret.Name)
		}
	}

	if len(requiredSecrets) > 0 {
		var missingSecrets []string
		for secret := range requiredSecrets {
			missingSecrets = append(missingSecrets, secret)
		}
		return nil, fmt.Errorf("failed to extract some required bootstrap secrets: %v", missingSecrets)
	}

	return as, nil
}

// renderKubeConfig outputs kubeconfig assets to ensure that the kubeconfig will be rendered to the
// assetDir for use by `bootkube start`.
func renderKubeConfig(kubeConfigPath string) (asset.Assets, error) {
	kubeConfig, err := ioutil.ReadFile(kubeConfigPath)
	if err != nil {
		return nil, err
	}
	return []asset.Asset{{
		Name: asset.AssetPathKubeConfig, // used by `bootkube start`.
		Data: kubeConfig,
	}}, nil
}

// setTypeMeta sets the TypeMeta for a runtime.Object.
// TODO(diegs): find the apimachinery code that does this, and use that instead.
func setTypeMeta(obj runtime.Object) error {
	typeMeta, ok := typeMetas[reflect.TypeOf(obj)]
	if !ok {
		return fmt.Errorf("don't know about type: %T", obj)
	}
	metaAccessor.SetAPIVersion(obj, typeMeta.APIVersion)
	metaAccessor.SetKind(obj, typeMeta.Kind)
	return nil
}

// setBootstrapPodMetadata creates valid metadata for a bootstrap pod. Currently it sets the
// TypeMeta and Name, Namespace, and Annotations on the ObjectMeta.
func setBootstrapPodMetadata(pod *v1.Pod, parent metav1.ObjectMeta) error {
	if err := setTypeMeta(pod); err != nil {
		return err
	}
	pod.ObjectMeta = metav1.ObjectMeta{
		Annotations: parent.Annotations,
		Name:        "bootstrap-" + parent.Name,
		Namespace:   parent.Namespace,
	}
	return nil
}

// serializeListObjToYAML takes a runtime.Object of type 'list', fixes up the metadata of each
// element, and serializes them to YAML assets using the naming convention `name-kind.yaml`.
func serializeListObjToYAML(outerObj runtime.Object) (asset.Assets, error) {
	objList, err := meta.ExtractList(outerObj)
	if err != nil {
		return nil, err
	}
	var as asset.Assets
	for _, obj := range objList {
		if err := setTypeMeta(obj); err != nil {
			return nil, err
		}
		name, err := metaAccessor.Name(obj)
		if err != nil {
			return nil, err
		}
		kind, err := metaAccessor.Kind(obj)
		if err != nil {
			return nil, err
		}
		a, err := serializeYAML(path.Join(asset.AssetPathManifests, name+"-"+strings.ToLower(kind)+".yaml"), obj)
		if err != nil {
			return nil, err
		}
		as = append(as, a)
	}
	return as, nil
}

// serializeYAML serializes a runtime.Object into a YAML asset.
func serializeYAML(assetName string, obj runtime.Object) (asset.Asset, error) {
	data, err := yaml.Marshal(obj)
	if err != nil {
		return asset.Asset{}, err
	}
	return asset.Asset{
		Name: assetName,
		Data: data,
	}, nil
}
