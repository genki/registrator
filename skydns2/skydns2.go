package skydns2

import (
	"log"
	"net/url"
	"strconv"
	"strings"
	"os"

	"github.com/coreos/go-etcd/etcd"
	"github.com/gliderlabs/registrator/bridge"
)

func init() {
  bridge.Register(&Factory{Scheme: "http"}, "skydns2")
  bridge.Register(&Factory{Scheme: "https"}, "skydns2s")
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

	if len(uri.Path) < 2 {
		log.Fatal("skydns2: dns domain required e.g.: skydns2://<host>/<domain>")
	}

  if f.Scheme == "https" {
    cert := os.Getenv("ETCD_CERTFILE")
    key := os.Getenv("ETCD_KEYFILE")
    caCert := os.Getenv("ETCD_CAFILE")
    client, err := etcd.NewTLSClient(urls, cert, key, caCert)
    if err != nil {
      log.Fatal("etcd: error creating tls client", err)
    }
    return &Skydns2Adapter{client:client, path:domainPath(uri.Path[1:])}
  }

	return &Skydns2Adapter{client: etcd.NewClient(urls), path: domainPath(uri.Path[1:])}
}

type Skydns2Adapter struct {
	client *etcd.Client
	path   string
}

func (r *Skydns2Adapter) Ping() error {
	rr := etcd.NewRawRequest("GET", "version", nil, nil)
	_, err := r.client.SendRequest(rr)
	if err != nil {
		return err
	}
	return nil
}

func (r *Skydns2Adapter) Register(service *bridge.Service) error {
	port := strconv.Itoa(service.Port)
	record := `{"host":"` + service.IP + `","port":` + port + `}`
	_, err := r.client.Set(r.servicePath(service), record, uint64(service.TTL))
	if err != nil {
		log.Println("skydns2: failed to register service:", err)
	}
	return err
}

func (r *Skydns2Adapter) Deregister(service *bridge.Service) error {
	_, err := r.client.Delete(r.servicePath(service), false)
	if err != nil {
		log.Println("skydns2: failed to register service:", err)
	}
	return err
}

func (r *Skydns2Adapter) Refresh(service *bridge.Service) error {
	return r.Register(service)
}

func (r *Skydns2Adapter) servicePath(service *bridge.Service) string {
	return r.path + "/" + service.Name + "/" + service.ID
}

func domainPath(domain string) string {
	components := strings.Split(domain, ".")
	for i, j := 0, len(components)-1; i < j; i, j = i+1, j-1 {
		components[i], components[j] = components[j], components[i]
	}
	return "/skydns/" + strings.Join(components, "/")
}
