package tcp

import (
	"fmt"
	"net"
	"sync"
	"math/rand"
	"math"
	"strings"

	consulapi "github.com/hashicorp/consul/api"

	"hipster-cache-proxy/common"
)

type CacheServer struct {
	id          string
	address     string
	port        int
	proxyClient *ProxyClient
	virtualNodes []int
}

type ClientMessage struct {
	command string
	params  []string
}

func (s *CacheServer) addVirtualNode(nodeIndex int) {
	s.virtualNodes = append(s.virtualNodes, nodeIndex)
}

func (s *CacheServer) getVirtualNode() int {
	randIndex := rand.Intn(len(s.virtualNodes))
	nodeIndex := s.virtualNodes[randIndex]
	// Delete link to virtual node	
	s.virtualNodes = append(s.virtualNodes[:randIndex][(randIndex+1):])
	return nodeIndex
}

func NewCacheServer(id, address string, port int) *CacheServer {
	return &CacheServer{id: id, address: address, port: port}
}

type ProxyServer struct {
	serverPort        int
	clientPort        int
	logger            common.ILogger
	listener          *net.TCPListener
	cacheServersMap   map[string]*CacheServer
	cacheServers      []*CacheServer
	mutexCacheServers sync.RWMutex
	virtualNodes         []*CacheServer
	lastIndexServerAddNodes int
	lastIndexServerFreeNodes int
	hashFunction *common.ComplexStringHash
}

func NewProxyServer(serverPort int, clientPort int, countVirtNodes int, maxKeyLenght int, logger common.ILogger) *ProxyServer {
	coefP := uint64(int64(countVirtNodes)*int64(maxKeyLenght) + (rand.Int63n(math.MaxInt64-int64(countVirtNodes*maxKeyLenght))))
	return &ProxyServer{
	serverPort: serverPort,
	clientPort: clientPort,
	logger: logger,
	cacheServersMap: make(map[string]*CacheServer),
	virtualNodes: make([]*CacheServer, countVirtNodes, countVirtNodes),
	hashFunction: common.NewComplexStringHash(uint64(countVirtNodes), coefP, coefP),}
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
		cacheServer, ok = s.cacheServersMap[service.ServiceID]
		s.mutexCacheServers.RUnlock()

		if !ok {
			cacheServer = NewCacheServer(service.ServiceID, service.Address, service.ServicePort)
/*
			s.mutexCacheServers.Lock()
			s.cacheServers[service.ServiceID] = cacheServer
			s.mutexCacheServers.Unlock()
*/
			registeredCacheServers = append(registeredCacheServers, cacheServer)

			fmt.Printf(`\n Services : "%#v"`, service)
		}
		servicesMap[service.ServiceID] = service
	}

	s.reCachingOnRegister(registeredCacheServers)

	s.mutexCacheServers.RLock()
	lenCacheServers := len(s.cacheServersMap)
	s.mutexCacheServers.RUnlock()

	var unregisteredCacheServers []*CacheServer
	if len(servicesMap) != lenCacheServers {
		s.mutexCacheServers.Lock()
		newCacheServers := []*CacheServer{}
		for _, cacheServer = range s.cacheServersMap {
			service, ok = servicesMap[cacheServer.id]
			if !ok {
				unregisteredCacheServers = append(unregisteredCacheServers, cacheServer)
				delete(s.cacheServersMap, cacheServer.id)
			} else {
				// New array cache services
				newCacheServers = append(newCacheServers, cacheServer)
			}
		}
		s.cacheServers = newCacheServers
		s.reCachingOnUnregister(unregisteredCacheServers)
	}

	fmt.Printf(`\n CacheServers: "%#v"`, s.cacheServers)
}

func (s *ProxyServer) reCachingOnUnregister(freeCacheServers []*CacheServer) {
	i := s.lastIndexServerAddNodes
	lenServers := len(s.cacheServers)
	var cacheServer *CacheServer
	for _, freeCacheServer := range freeCacheServers {
		for _, nodeIndex := range freeCacheServer.virtualNodes {
			if i == lenServers {
				i = 0
			}
			cacheServer = s.cacheServers[i]
			s.virtualNodes[nodeIndex] = cacheServer
			cacheServer.addVirtualNode(nodeIndex)
			s.lastIndexServerAddNodes = i
			i++
		}
		freeCacheServer.virtualNodes = []int{}
	}
}


func (s *ProxyServer) initVirtualNodesDistribution(newCacheServers []*CacheServer) {
	freeVirtualNodes := []int{}
	for key, _ := range s.virtualNodes {
		freeVirtualNodes = append(freeVirtualNodes, key)
	}
	i := 0
	lenServices := len(newCacheServers)
	if lenServices == 0 {
		return
	}
	for {
		if i == lenServices {
			i = 0
		}
		randIndex := rand.Intn(len(freeVirtualNodes))
		cacheServer := newCacheServers[i]
		s.virtualNodes[randIndex] = cacheServer
		cacheServer.addVirtualNode(randIndex)
		s.lastIndexServerAddNodes = i
		i++

		// Remove this index from slice
		freeVirtualNodes = append(freeVirtualNodes[:randIndex], freeVirtualNodes[randIndex+1:]...)
		if len(freeVirtualNodes) == 0 {
			break
		}
	}
	for _, cacheServer := range newCacheServers {
		s.cacheServersMap[cacheServer.id] = cacheServer
	}
	s.cacheServers = newCacheServers

}
func (s *ProxyServer) reCachingOnRegister(newCacheServers []*CacheServer) {
	currentServerIndex := s.lastIndexServerFreeNodes
	newServerIndex := 0
	lenServers := len(s.cacheServers)
	lenNewServers := len(newCacheServers)
	if lenServers == 0 {
		s.initVirtualNodesDistribution(newCacheServers)
		return
	}
	if lenServers == 0 {
		return
	}
	// needed coount nodes for reCaching
	countNodesPerServer := len(s.virtualNodes)/(lenServers + lenNewServers)
	neededCountRecachingNodes := lenNewServers * countNodesPerServer
	countRecachingNodes := 0
	var cacheServer *CacheServer
	for {
		if currentServerIndex == lenServers {
			currentServerIndex = 0
		}
		if newServerIndex == lenNewServers {
			newServerIndex = 0
		}
		currentServer := s.cacheServers[currentServerIndex]
		newServer := newCacheServers[newServerIndex]

		nodeIndex := currentServer.getVirtualNode()
		s.virtualNodes[nodeIndex] = newServer
		newServer.addVirtualNode(nodeIndex)
		s.lastIndexServerFreeNodes = currentServerIndex

		currentServerIndex++
		newServerIndex++
		countRecachingNodes++

		if countRecachingNodes == neededCountRecachingNodes {
			break
		}
	}
	for _, cacheServer = range newCacheServers {
		s.cacheServersMap[cacheServer.id] = cacheServer
	}
	s.cacheServers = append(s.cacheServers,newCacheServers...)
}

func (s *ProxyServer) getCacheServer(key string) (*CacheServer, error) {
	hashKey := s.hashFunction.CalculateHash(key)
	s.mutexCacheServers.RLock()
	fmt.Printf(`Cache Servers:"%#v"`, s.cacheServers)
	cacheServer := s.cacheServers[hashKey]
	s.mutexCacheServers.RUnlock()
	if cacheServer == nil {
		return nil, fmt.Errorf(`Can't find cache server by index "%s"`, key)
	}
	return cacheServer, nil
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
		command := string(buf[0:n])
		fmt.Printf(`Response "%s"`, command)
		response, err := s.getResponse(command)
		if err != nil {
			response = err.Error()
		}
		if err != nil {
			s.logger.Errorf(`Error message: "%s"`, err.Error())
		}
		fmt.Printf(`Response: "%s"`, string(response))
		conn.Write([]byte(response))
	}
	return
}

func (s *ProxyServer) getResponse(command string) (string, error) {
	clientMessage := NewClientMessage()
	if err := clientMessage.Init(command); err != nil {
		return "", err
	}
	if len(clientMessage.params) == 0 {
			return "", fmt.Errorf(`Error: incorrect parametes count, it needs minimum 1, was sended "%d"`, len(clientMessage.params))
	}
	key := clientMessage.params[0]
	cacheServer,_ := s.getCacheServer(key)
	if cacheServer.proxyClient == nil {
		cacheServer.proxyClient = NewProxyClient(s.clientPort, cacheServer.address, cacheServer.port, s.logger)
		// TODO: Repeat if connection false
		err := cacheServer.proxyClient.InitConnection()
		if err != nil {
			s.logger.Errorf(`Error init connection with the CacheServer "%s"r`, err.Error())
		}
	}
	return cacheServer.proxyClient.SendMessage(command)
	}

func NewClientMessage() *ClientMessage {
	return &ClientMessage{}
}

func (m *ClientMessage) Init(value string) error {
	words := m.splitMessageBySpaceAndQuates(value)
	fmt.Printf(`\n Words :"%#v"`, words)
	if len(words) == 0 {
		return fmt.Errorf(`Error: you don't set the command`)
	}
	if len(words) < 2 {
		return fmt.Errorf(`You don't set any parameters`)
	}
	m.command = strings.ToUpper(words[0])
	m.params = words[1:]
	return nil
}

func (m *ClientMessage) splitMessageBySpaceAndQuates(message string) []string {
	words := []string{}
	var word string
	var character string
	delimeter := ""
	for _, characterCode := range message {
		character = string(characterCode)
		switch character {
		case ` `:
			if delimeter == "" {
				words = append(words, word)
				word = ""
				break
			}
			word += character
		case `"`:
			if delimeter == character {
				delimeter = ""
				break
			}
			if delimeter == "" {
				delimeter = character
				break
			}
		case "\n", "\r":
		default:
			word += character

		}
	}
	if word != "" {
		words = append(words, word)
	}
	return words
}
