package hipsterCacheProxy

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"

	"hipster-cache-proxy/config"
	"hipster-cache-proxy/tcp"
	"hipster-cache-proxy/common"
)


type Application struct {
	config *config.Config
	logger common.ILogger
	consul *consulapi.Client
	mutexCacheServers sync.RWMutex
	proxyServer *tcp.ProxyServer
}

func NewApplication(config *config.Config, logger common.ILogger) *Application {
	return &Application{config: config, logger: logger,}
}

func (this *Application) Init() error {
	fmt.Printf("\n startInit")
	err := this.initDiscovery()
	if err != nil {
		return err
	}

	this.initTCP()
	this.initRouting()
	return nil
}

func (this *Application) cacheServerChangedRegistration(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("Enter")
	catalog := this.consul.Catalog()
	services, meta, err := catalog.Service("hipster-cache", "", nil)
	if err != nil {
		this.logger.Errorf(`Error getting services from consul: "%s"`, err.Error())
		return
	}

	if meta.LastIndex == 0 {
		this.logger.Errorf(`Error getting services from consul, incorrect metadata: "%#v"`, meta)
		return
	}
	if len(services) == 0 {
		fmt.Printf("No services")
		return
	}

	this.proxyServer.CacheServerChangedRegistration(services)
	io.WriteString(w, "Ok")
}

func (this *Application) initRouting() {
	// Handler for Prometheus
	http.Handle("/metrics", prometheus.Handler())
	// Handler for checking recalculation hash distribution
	http.HandleFunc("/cache_server/changed_registration", this.cacheServerChangedRegistration)
}

func (this *Application) Run() error {
	go http.ListenAndServe(fmt.Sprintf(":%d", this.config.MetricsPort), nil)
	this.proxyServer.Run()
	return nil
}


func (this *Application) registerService(catalog *consulapi.Catalog, id string, serviceName string, port int) error {
	service := &consulapi.AgentService{
		ID:      id,
		Service: serviceName,
		Port:    port,
	}

	reg := &consulapi.CatalogRegistration{
		Datacenter: "dc1",
		Node:       id,
		Address:    this.config.Address,
		Service:    service,
	}
	_, err := catalog.Register(reg, nil)
	return err
}


func (this *Application) initDiscovery() error {
	var err error
	config := consulapi.DefaultConfig()
	config.Address = this.config.ConsulAddress
	this.consul, err = consulapi.NewClient(config)
	if err != nil {
		return err
	}
	catalog :=  this.consul.Catalog()

	// Register for Applications
	err = this.registerService(catalog, "proxy1", "hipster-cache-proxy", this.config.ServerPort)
	if err != nil {
		return err
	}

	// Register for Prometheus
	return this.registerService(catalog, "proxy2", "hipster-cache-proxy-metrics", this.config.MetricsPort)
}

func (this *Application) initTCP() error {
	this.proxyServer = tcp.NewProxyServer(this.config.ServerPort, this.config.ClientPort, this.logger)
	return this.proxyServer.Init()
}
