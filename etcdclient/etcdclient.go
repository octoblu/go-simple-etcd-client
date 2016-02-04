package etcdclient

import (
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
)

// EtcdClient interface lets your Get/Set from Etcd
type EtcdClient interface {
	Get(key string) (string, error)
	Set(key, value string) error
}

// SimpleEtcdClient implements EtcdClient
type SimpleEtcdClient struct {
	etcd client.Client
}

// Dial constructs a new EtcdClient
func Dial(etcdURI string) (EtcdClient, error) {
	etcd, err := client.New(client.Config{
		Endpoints: []string{etcdURI},
	})
	if err != nil {
		return nil, err
	}
	return &SimpleEtcdClient{etcd}, nil
}

// Get gets a value in Etcd
func (etcdClient *SimpleEtcdClient) Get(key string) (string, error) {
	api := client.NewKeysAPI(etcdClient.etcd)
	response, err := api.Get(context.Background(), key, nil)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return response.Node.Value, nil
}

// Set sets a value in Etcd
func (etcdClient *SimpleEtcdClient) Set(key, value string) error {
	api := client.NewKeysAPI(etcdClient.etcd)
	_, err := api.Set(context.Background(), key, value, nil)
	return err
}
