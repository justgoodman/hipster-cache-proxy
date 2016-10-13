package hipsterCacheProxy

import (
	"fmt"
	"io"
	"net/http"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"

	"hipster-cache-proxy/config"
)

type ILogger interface {
	Criticalf(message string, args ...interface{})
	Errorf(message string, args ...interface{})
	Warningf(message string, args ...interface{})
	Infof(message string, args ...interface{})
	Debugf(message string, args ...interface{})
	Tracef(message string, args ...interface{})
}

type Application struct {
	Config *config.Config
	Logger ILogger
}

func NewApplication(config *config.Config, logger ILogger) *Application {
	return &Application{Config: config, Logger: logger}
}

func (this *Application) Init() error {
	fmt.Printf("\n startInit")
	err := this.initDiscovery()
	if err != nil {
		return err
	}
	this.initRouting()
	return nil
}

func cacheClientChangedRegistration(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Ok")
}

func (this *Application) initRouting() {
	// Handler for Prometheus
	http.Handle("/metrics", prometheus.Handler())
	// Handler for checking recalculation hash distribution
	http.HandleFunc("/cache_client/changed_registration", cacheClientChangedRegistration)
}

func (this *Application) Run() error {
	http.ListenAndServe(fmt.Sprintf(":%d", this.Config.Port), nil)
	return nil
}

func (this *Application) initDiscovery() error {
	config := consulapi.DefaultConfig()
	config.Address = this.Config.ConsulAddress
	consul, err := consulapi.NewClient(config)
	if err != nil {
		return err
	}
	catalog := consul.Catalog()
	service := &consulapi.AgentService{
		ID:      "proxy1",
		Service: "hipster-cache-proxy",
		Port:    this.Config.Port,
	}

	reg := &consulapi.CatalogRegistration{
		Datacenter: "dc1",
		Node:       "proxy1",
		Address:    this.Config.Address,
		Service:    service,
	}

	_, err = catalog.Register(reg, nil)

	if err != nil {
		return err
	}

	return nil
}
