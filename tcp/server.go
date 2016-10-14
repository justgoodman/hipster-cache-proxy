package tcp

import (
	"net"
	"fmt"
	"sync"

	consulapi "github.com/hashicorp/consul/api"

        "hipster-cache-proxy/common"
)

type CacheServer struct {
        id string
        address string
        port int
        proxyClient *ProxyClient
}

func NewCacheServer(id, address string, port int) *CacheServer {
        return &CacheServer{id: id, address: address, port: port,}
}

type ProxyServer struct {
	serverPort int
	clientPort int
	logger common.ILogger
	listener *net.TCPListener
	cacheServers map[string]*CacheServer
	mutexCacheServers sync.RWMutex
}

func NewProxyServer(serverPort int, clientPort int, logger common.ILogger) *ProxyServer {
	return &ProxyServer{serverPort: serverPort, clientPort: clientPort, logger:logger, cacheServers: make(map[string]*CacheServer),}
}

func (this *ProxyServer) Init() error {
     tcpAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf(`:%d`,this.serverPort))
     if err != nil {
		return err
	}

     this.listener, err = net.ListenTCP("tcp", tcpAddr)
     return err
}

func (this *ProxyServer) CacheServerChangedRegistration(services []*consulapi.CatalogService) {
        var (
		cacheServer *CacheServer
		ok bool
		service *consulapi.CatalogService
	)

	servicesMap := make(map[string]*consulapi.CatalogService)

        registeredCacheServers := make([]*CacheServer,0,1)

        for _, service = range(services) {
                this.mutexCacheServers.RLock()
                cacheServer,ok = this.cacheServers[service.ServiceID]
                this.mutexCacheServers.RUnlock()

                if !ok {
                        cacheServer = NewCacheServer(service.ServiceID, service.Address, service.ServicePort)
                        this.mutexCacheServers.Lock()
                        this.cacheServers[service.ServiceID] = cacheServer
                        this.mutexCacheServers.Unlock()
                        registeredCacheServers = append(registeredCacheServers, cacheServer)

			fmt.Printf(`\n Services : "%#v"`, service)
                }
                servicesMap[service.ServiceID] = service
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
}

func (this *ProxyServer) reCachingOnUnregister(cacheServers []*CacheServer) {

}

func (this *ProxyServer) reCachingOnRegister(cacheServers []*CacheServer) {

}


func (this *ProxyServer) getCacheServer() *CacheServer {
	this.mutexCacheServers.RLock()
	fmt.Printf(`Cache Servers:"%#v"`, this.cacheServers)
	cacheServer, _ := this.cacheServers["cache1"]
	this.mutexCacheServers.RUnlock()
	return cacheServer
}

func (this *ProxyServer) Run() {
     for {
           conn, err := this.listener.Accept()
           if err != nil {
                this.logger.Errorf(`Connection error: "%s"`, err.Error())
                continue
           }
           go this.handleMessage(conn)
//      conn.Write([]byte("Bratish vse ok"))
//      conn.Close()
     }
}

func (this *ProxyServer) handleMessage(conn net.Conn) {
	var buf [512]byte
	for {
		n, err := conn.Read(buf[0:])
		if err != nil {
			this.logger.Errorf(`Read message error: "%s"`, err.Error())
		}
		fmt.Println(string(buf[0:n]))
		cacheServer := this.getCacheServer()
		fmt.Printf(`\n Cache Server: "%#v"`, cacheServer)
		if cacheServer != nil {
			if cacheServer.proxyClient == nil {
				cacheServer.proxyClient = NewProxyClient(this.clientPort,cacheServer.address,cacheServer.port,this.logger)
				// TODO: Repeat if connection false
				err := cacheServer.proxyClient.InitConnection()
				if err != nil {
					this.logger.Errorf(`Error init connection with the CacheServer "%s"r`, err.Error())
				}
			}
			response, err := cacheServer.proxyClient.SendMessage(string(buf[0:n]))
			if err != nil {
				this.logger.Errorf(`Error message: "%s"`, err.Error())
			}
			fmt.Printf(`Response: "%s"`, string(response))
		}
	//	time.Sleep(time.Second * 10)
//		conn.Close()
//		return
	}
	return
}

