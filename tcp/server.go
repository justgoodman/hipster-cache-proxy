package tcp

import (
	"fmt"
	"net"
	"sync"

	consulapi "github.com/hashicorp/consul/api"

	"hipster-cache-proxy/common"
)

type CacheServer struct {
	id          string
	address     string
	port        int
	proxyClient *ProxyClient
}

func NewCacheServer(id, address string, port int) *CacheServer {
	return &CacheServer{id: id, address: address, port: port}
}

type ProxyServer struct {
	serverPort        int
	clientPort        int
	logger            common.ILogger
	listener          *net.TCPListener
	cacheServers      map[string]*CacheServer
	mutexCacheServers sync.RWMutex
}

func NewProxyServer(serverPort int, clientPort int, logger common.ILogger) *ProxyServer {
	return &ProxyServer{serverPort: serverPort, clientPort: clientPort, logger: logger, cacheServers: make(map[string]*CacheServer)}
}

func (s *ProxyServer) Init() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf(`:%d`, s.serverPort))
	if err != nil {
		return err
	}

	s.listener, err = net.ListenTCP("tcp", tcpAddr)
	return err
}

func (s *ProxyServer) CacheServerChangedRegistration(services []*consulapi.CatalogService) {
	var (
		cacheServer *CacheServer
		ok          bool
		service     *consulapi.CatalogService
	)

	servicesMap := make(map[string]*consulapi.CatalogService)

	registeredCacheServers := make([]*CacheServer, 0, 1)

	for _, service = range services {
		s.mutexCacheServers.RLock()
		cacheServer, ok = s.cacheServers[service.ServiceID]
		s.mutexCacheServers.RUnlock()

		if !ok {
			cacheServer = NewCacheServer(service.ServiceID, service.Address, service.ServicePort)
			s.mutexCacheServers.Lock()
			s.cacheServers[service.ServiceID] = cacheServer
			s.mutexCacheServers.Unlock()
			registeredCacheServers = append(registeredCacheServers, cacheServer)

			fmt.Printf(`\n Services : "%#v"`, service)
		}
		servicesMap[service.ServiceID] = service
	}

	s.reCachingOnRegister(registeredCacheServers)

	s.mutexCacheServers.RLock()
	lenCacheServers := len(s.cacheServers)
	s.mutexCacheServers.RUnlock()

	if len(servicesMap) != lenCacheServers {
		unregisteredCacheServers := make([]*CacheServer, 0, 1)
		s.mutexCacheServers.Lock()
		for _, cacheServer = range s.cacheServers {
			service, ok = servicesMap[cacheServer.id]
			if !ok {
				delete(s.cacheServers, cacheServer.id)
				unregisteredCacheServers = append(unregisteredCacheServers, cacheServer)
			}
		}
		s.reCachingOnUnregister(unregisteredCacheServers)
	}

	fmt.Printf(`\n CacheServers: "%#v"`, s.cacheServers)
}

func (s *ProxyServer) reCachingOnUnregister(cacheServers []*CacheServer) {

}

func (s *ProxyServer) reCachingOnRegister(cacheServers []*CacheServer) {

}

func (s *ProxyServer) getCacheServer() *CacheServer {
	s.mutexCacheServers.RLock()
	fmt.Printf(`Cache Servers:"%#v"`, s.cacheServers)
	cacheServer, _ := s.cacheServers["cache1"]
	s.mutexCacheServers.RUnlock()
	return cacheServer
}

func (s *ProxyServer) Run() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.logger.Errorf(`Connection error: "%s"`, err.Error())
			continue
		}
		go s.handleMessage(conn)
		//      conn.Write([]byte("Bratish vse ok"))
		//      conn.Close()
	}
}

func (s *ProxyServer) handleMessage(conn net.Conn) {
	var buf [512]byte
	for {
		n, err := conn.Read(buf[0:])
		if err != nil {
			s.logger.Errorf(`Read message error: "%s"`, err.Error())
		}
		fmt.Println(string(buf[0:n]))
		cacheServer := s.getCacheServer()
		fmt.Printf(`\n Cache Server: "%#v"`, cacheServer)
		if cacheServer != nil {
			if cacheServer.proxyClient == nil {
				cacheServer.proxyClient = NewProxyClient(s.clientPort, cacheServer.address, cacheServer.port, s.logger)
				// TODO: Repeat if connection false
				err := cacheServer.proxyClient.InitConnection()
				if err != nil {
					s.logger.Errorf(`Error init connection with the CacheServer "%s"r`, err.Error())
				}
			}
			response, err := cacheServer.proxyClient.SendMessage(string(buf[0:n]))
			if err != nil {
				s.logger.Errorf(`Error message: "%s"`, err.Error())
			}
			fmt.Printf(`Response: "%s"`, string(response))
			conn.Write([]byte(response))
		}
		//	time.Sleep(time.Second * 10)
		//		conn.Close()
		//		return
	}
	return
}
