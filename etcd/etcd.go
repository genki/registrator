package etcd

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"os"
	"encoding/json"
	"fmt"

	etcd2 "github.com/coreos/go-etcd/etcd"
	"github.com/gliderlabs/registrator/bridge"
	etcd "gopkg.in/coreos/go-etcd.v0/etcd"
)

func init() {
  bridge.Register(&Factory{Scheme: "http"}, "etcd")
  bridge.Register(&Factory{Scheme: "https"}, "etcds")
}

type Factory struct{
  Scheme string
}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	urls := make([]string, 0)
	if uri.Host != "" {
		urls = append(urls, f.Scheme+"://"+uri.Host)
	} else {
		urls = append(urls, f.Scheme+"://127.0.0.1:4001")
	}

  if f.Scheme == "https" {
    cert := os.Getenv("ETCD_CERTFILE")
    key := os.Getenv("ETCD_KEYFILE")
    caCert := os.Getenv("ETCD_CAFILE")
    client, err := etcd2.NewTLSClient(urls, cert, key, caCert)
    if err != nil {
      log.Fatal("etcd: error creating tls client", err)
    }
    return &EtcdAdapter{client2: client, path: uri.Path}
  }

	res, err := http.Get(urls[0] + "/version")
	if err != nil {
		log.Fatal("etcd: error retrieving version", err)
	}

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	if match, _ := regexp.Match("0\\.4\\.*", body); match == true {
		log.Println("etcd: using v0 client")
		return &EtcdAdapter{client: etcd.NewClient(urls), path: uri.Path}
	}

	return &EtcdAdapter{client2: etcd2.NewClient(urls), path: uri.Path}
}

type EtcdAdapter struct {
	client  *etcd.Client
	client2 *etcd2.Client

	path string
}

func (r *EtcdAdapter) Ping() error {
	r.syncEtcdCluster()

	var err error
	if r.client != nil {
		rr := etcd.NewRawRequest("GET", "version", nil, nil)
		_, err = r.client.SendRequest(rr)
	} else {
		rr := etcd2.NewRawRequest("GET", "version", nil, nil)
		_, err = r.client2.SendRequest(rr)
	}

	if err != nil {
		return err
	}
	return nil
}

func (r *EtcdAdapter) syncEtcdCluster() {
	var result bool
	if r.client != nil {
		result = r.client.SyncCluster()
	} else {
		result = r.client2.SyncCluster()
	}

	if !result {
		log.Println("etcd: sync cluster was unsuccessful")
	}
}

func (r *EtcdAdapter) Register(service *bridge.Service) error {
	r.syncEtcdCluster()

	var err error
	path := r.path + "/" + service.Name + "/" + service.ID
  tags,err  := json.Marshal(service.Tags)
  if err != nil {log.Println("etcd: faild to marshal tags:", err)}
  attrs,err := json.Marshal(service.Attrs)
  if err != nil {log.Println("etcd: faild to marshal attrs:", err)}

  value := fmt.Sprintf(
    "{\"ID\":\"%s\",\"Name\":\"%s\",\"Port\":%d,\"IP\":\"%s\"," +
    "\"Tags\":%s,\"Attrs\":%s}",
    service.ID, service.Name, service.Port, service.IP,
    string(tags[:]), string(attrs[:]))

	if r.client != nil {
		_, err = r.client.Set(path, value, uint64(service.TTL))
	} else {
		_, err = r.client2.Set(path, value, uint64(service.TTL))
	}

	if err != nil {
		log.Println("etcd: failed to register service:", err)
	}
	return err
}

func (r *EtcdAdapter) Deregister(service *bridge.Service) error {
	r.syncEtcdCluster()

	path := r.path + "/" + service.Name + "/" + service.ID

	var err error
	if r.client != nil {
		_, err = r.client.Delete(path, false)
	} else {
		_, err = r.client2.Delete(path, false)
	}

	if err != nil {
		log.Println("etcd: failed to deregister service:", err)
	}
	return err
}

func (r *EtcdAdapter) Refresh(service *bridge.Service) error {
	return r.Register(service)
}
