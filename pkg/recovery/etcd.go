// The etcd backend fetches control plane objects directly from etcd. This is adapted heavily from
// kubernetes/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go.

package recovery

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"strings"

	"github.com/coreos/etcd/clientv3"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/pkg/api"
)

// EtcdBackend is a backend that extracts a controlPlane from an etcd instance.
type EtcdBackend struct {
	client     *clientv3.Client
	decoder    runtime.Decoder
	pathPrefix string
}

// NewEtcdBackend constructs a new EtcdBackend for the given client and pathPrefix.
func NewEtcdBackend(client *clientv3.Client, pathPrefix string) Backend {
	return &EtcdBackend{
		client:     client,
		decoder:    api.Codecs.UniversalDecoder(),
		pathPrefix: pathPrefix,
	}
}

// read implements Backend.read().
func (s *EtcdBackend) read(ctx context.Context) (*controlPlane, error) {
	cp := &controlPlane{}
	for _, r := range []struct {
		etcdName string
		yamlName string
		obj      runtime.Object
	}{{
		etcdName: "configmaps",
		yamlName: "config-map",
		obj:      &cp.configMaps,
	}, {
		etcdName: "daemonsets",
		yamlName: "daemonset",
		obj:      &cp.daemonSets,
	}, {
		etcdName: "deployments",
		yamlName: "deployment",
		obj:      &cp.deployments,
	}, {
		etcdName: "poddisruptionbudgets",
		yamlName: "pod-disruption-budget",
		obj:      &cp.podDisruptionBudgets,
	}, {
		etcdName: "secrets",
		yamlName: "secret",
		obj:      &cp.secrets,
	}, {
		etcdName: "services/specs",
		yamlName: "service",
		obj:      &cp.services,
	}, {
		etcdName: "serviceaccounts",
		yamlName: "service-account",
		obj:      &cp.serviceAccounts,
	},
	} {
		if err := s.list(ctx, r.etcdName, r.obj); err != nil {
			return nil, err
		}
	}
	return cp, nil
}

// get fetches a single runtime.Object with key `key` from etcd.
func (s *EtcdBackend) get(ctx context.Context, key string, out runtime.Object, ignoreNotFound bool) error {
	key = path.Join(s.pathPrefix, key, api.NamespaceSystem)
	getResp, err := s.client.KV.Get(ctx, key)
	if err != nil {
		return err
	}

	if len(getResp.Kvs) == 0 {
		if ignoreNotFound {
			return runtime.SetZeroValue(out)
		}
		return fmt.Errorf("key not found: %s", key)
	}
	kv := getResp.Kvs[0]
	return decode(s.decoder, kv.Value, out)
}

// list fetches a list runtime.Object from etcd located at key prefix `key`.
func (s *EtcdBackend) list(ctx context.Context, key string, listObj runtime.Object) error {
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	key = path.Join(s.pathPrefix, key, api.NamespaceSystem)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	getResp, err := s.client.KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	elems := make([][]byte, len(getResp.Kvs))
	for i, kv := range getResp.Kvs {
		elems[i] = kv.Value
	}
	return decodeList(elems, listPtr, s.decoder)
}

// decode decodes value of bytes into object.
func decode(decoder runtime.Decoder, value []byte, objPtr runtime.Object) error {
	if _, err := conversion.EnforcePtr(objPtr); err != nil {
		return fmt.Errorf("objPtr must be pointer, got: %T", objPtr)
	}
	_, _, err := decoder.Decode(value, nil, objPtr)
	if err != nil {
		return err
	}
	return nil
}

// decodeList decodes a list of values into a list of objects.
func decodeList(elems [][]byte, listPtr interface{}, decoder runtime.Decoder) error {
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return fmt.Errorf("listPtr must be pointer to slice, got: %T", listPtr)
	}
	for _, elem := range elems {
		obj, _, err := decoder.Decode(elem, nil, reflect.New(v.Type().Elem()).Interface().(runtime.Object))
		if err != nil {
			return err
		}
		v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
	}
	return nil
}
