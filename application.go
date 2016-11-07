package hipsterCacheProxy

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"

	"hipster-cache-proxy/common"
	"hipster-cache-proxy/config"
	"hipster-cache-proxy/tcp"
)

type Application struct {
	config            *config.Config
	logger            common.ILogger
	consul            *consulapi.Client
	mutexCacheServers sync.RWMutex
	proxyServer       *tcp.ProxyServer
}

func NewApplication(config *config.Config, logger common.ILogger) *Application {
	return &Application{config: config, logger: logger}
}

func (a *Application) Init() error {
	fmt.Printf("\n startInit")
	err := a.initDiscovery()
	if err != nil {
		return err
	}

	a.initTCP()
	a.initRouting()
	// If CacheServers registered in consul we can updateVirtualNodes
	a.updateVirtualNodes()
	go a.proxyServer.ServersSharding.HealthCheck()
	return nil
}

func (a *Application) cacheServerChangedRegistration(w http.ResponseWriter, r *http.Request) {
	a.updateVirtualNodes()
	io.WriteString(w, "Ok")
}

func (a *Application) updateVirtualNodes() {
	fmt.Printf("Enter")
	catalog := a.consul.Catalog()
	services, meta, err := catalog.Service("hipster-cache", "", nil)
	//	fmt.Printf("\n \n Consul Services %#v", services)
	if err != nil {
		a.logger.Errorf(`Error getting services from consul: "%s"`, err.Error())
		return
	}

	if meta.LastIndex == 0 {
		a.logger.Errorf(`Error getting services from consul, incorrect metadata: "%#v"`, meta)
		return
	}
	if len(services) == 0 {
		fmt.Printf("No services")
		return
	}

	a.proxyServer.ServersSharding.CacheServerChangedRegistration(services)
}

func (a *Application) initRouting() {
	// Handler for Prometheus
	http.Handle("/metrics", prometheus.Handler())
	// Handler for checking recalculation hash distribution
	http.HandleFunc("/cache_server/changed_registration", a.cacheServerChangedRegistration)
}

func (a *Application) Run() error {
	go http.ListenAndServe(fmt.Sprintf(":%d", a.config.MetricsPort), nil)
	a.proxyServer.Run()
	return nil
}

func (a *Application) registerService(catalog *consulapi.Catalog, id string, serviceName string, port int) error {
	service := &consulapi.AgentService{
		ID:      id,
		Service: serviceName,
		Port:    port,
	}

	reg := &consulapi.CatalogRegistration{
		Datacenter: "dc1",
		Node:       id,
		Address:    a.config.Address,
		Service:    service,
	}
	_, err := catalog.Register(reg, nil)
	return err
}

func (a *Application) registerHealthCheck(agent *consulapi.Agent, address string, port int) error {
	return nil
	/*
		reg := &consulapi.AgentCheckRegistration{
			ID: fmt.Sprintf("HealthCheckProxy_%s", address),
			Name: fmt.Sprintf("Health Check TCP for node: %s", address),
		}
		reg.TCP = fmt.Sprintf("%s:%d", address, port)
		reg.Interval = "30s"
		return agent.CheckRegister(reg)
	*/
}

func (a *Application) initDiscovery() error {
	var err error
	config := consulapi.DefaultConfig()
	config.Address = a.config.ConsulAddress
	a.consul, err = consulapi.NewClient(config)
	if err != nil {
		return err
	}
	catalog := a.consul.Catalog()

	// Register for Applications
	err = a.registerService(catalog, "proxy1", "hipster-cache-proxy", a.config.ServerPort)
	if err != nil {
		return err
	}

	agent := a.consul.Agent()
	// Register heath check
	err = a.registerHealthCheck(agent, a.config.Address, a.config.ServerPort)

	if err != nil {
		return err
	}

	// Register for Prometheus
	return a.registerService(catalog, "proxy2", "hipster-cache-proxy-metrics", a.config.MetricsPort)
}

func (a *Application) initTCP() error {
	a.proxyServer = tcp.NewProxyServer(a.config.ServerPort, a.config.CountVirtNodes, a.config.MaxKeyLenght, a.logger)
	return a.proxyServer.Init()
}
