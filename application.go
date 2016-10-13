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

type CacheServer struct {
	id string
	address string
	port int
}

func NewCacheServer(id, address string, port int) *CacheServer {
	return &CacheServer{id: id, address: address, port: port,}
}


type Application struct {
	config *config.Config
	logger common.ILogger
	consul *consulapi.Client
	cacheServers map[string]*CacheServer
	mutexCacheServers sync.RWMutex
	proxyServer *tcp.ProxyServer
}

func NewApplication(config *config.Config, logger common.ILogger) *Application {
	return &Application{config: config, logger: logger, cacheServers: make(map[string]*CacheServer)}
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
	servicesSlice, meta, err := catalog.Service("hipster-cache", "", nil)
	if err != nil {
		this.logger.Errorf(`Error getting services from consul: "%s"`, err.Error())
		return
	}

	if meta.LastIndex == 0 {
		this.logger.Errorf(`Error getting services from consul, incorrect metadata: "%#v"`, meta)
		return
	}
	if len(servicesSlice) == 0 {
		fmt.Printf("No services")
		return
	}

	servicesMap := make(map[string]*consulapi.CatalogService)

	registeredCacheServers := make([]*CacheServer,0,1)

	var ( 
	cacheServer *CacheServer
	ok bool
	service *consulapi.CatalogService )

	for _, service = range(servicesSlice) {
		this.mutexCacheServers.RLock()
		cacheServer,ok = this.cacheServers[service.ServiceID]
		this.mutexCacheServers.RUnlock()

		if !ok {
			cacheServer = NewCacheServer(service.ServiceID, service.ServiceAddress, service.ServicePort)
		        this.mutexCacheServers.Lock()
			this.cacheServers[service.ServiceID] = cacheServer
			this.mutexCacheServers.Unlock()
			registeredCacheServers = append(registeredCacheServers, cacheServer)
		}
		servicesMap[service.ServiceID] = service
		fmt.Printf(`Services : "%#v"`, service)
	}

	this.reCachingOnRegister(registeredCacheServers)

	this.mutexCacheServers.RLock()
	lenCacheServers := len(this.cacheServers)
	this.mutexCacheServers.RUnlock()

	if len(servicesMap) != lenCacheServers {
		unregisteredCacheServers := make([]*CacheServer,0,1)
		this.mutexCacheServers.Lock()
		for _, cacheServer = range(this.cacheServers) {
			service,ok = servicesMap[cacheServer.id]
			if !ok {
				delete(this.cacheServers,cacheServer.id)
				unregisteredCacheServers = append(unregisteredCacheServers, cacheServer)
			}
		}
		this.reCachingOnUnregister(unregisteredCacheServers)
	}

	fmt.Printf(`\n CacheServers: "%#v"`, this.cacheServers)

	io.WriteString(w, "Ok")
}

func (this *Application) reCachingOnUnregister(cacheServers []*CacheServer) {

}

func (this *Application) reCachingOnRegister(cacheServers []*CacheServer) {

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

func (this *Application) initDiscovery() error {
	var err error
	config := consulapi.DefaultConfig()
	config.Address = this.config.ConsulAddress
	this.consul, err = consulapi.NewClient(config)
	if err != nil {
		return err
	}
	catalog := this.consul.Catalog()
	service := &consulapi.AgentService{
		ID:      "proxy1",
		Service: "hipster-cache-proxy",
		Port:    this.config.MetricsPort,
	}

	reg := &consulapi.CatalogRegistration{
		Datacenter: "dc1",
		Node:       "proxy1",
		Address:    this.config.Address,
		Service:    service,
	}

	_, err = catalog.Register(reg, nil)

	if err != nil {
		return err
	}

	return nil
}

func (this *Application) initTCP() {
	server := tcp.NewProxyServer(this.config.TcpPort, this.logger)
	err := server.Init()
	if err != nil {
		this.logger.Errorf(`Error init proxy server:"%s"`, err)
		return
	}
	this.proxyServer = server
	fmt.Printf(`\n !=== Finished === \n `)
	return
/*
	fmt.Printf("TCP IP")
	ips,err := net.LookupIP("consul6")
	fmt.Printf("\n Finish \n")
	if err != nil {
		this.logger.Errorf(`Error get IP:"%s"`, err)
		return
	}
	if len(ips) == 0 {
	  this.logger.Errorf(`Error get IP`)
	  return
	}
*/
//	fmt.Printf("Ips: %#v", ips)
//	ip := ips[0]
//	fmt.Printf(`Ip: %s"`, string(ip))
//	listener, err := net.ListenTCP("tcp", tcpAddr)
}
