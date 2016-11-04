package tcp

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"hipster-cache-proxy/common"
)

type ServersSharding struct {
	logger                   common.ILogger
	cacheServersMap          map[string]*CacheServer
	cacheServers             []*CacheServer
	cacheServersMutex        sync.RWMutex
	cacheServersMapMutex	 sync.RWMutex
	reShardingMutex		 sync.RWMutex
	virtualNodes             []*CacheServer
	lastIndexServerAddNodes  int
	lastIndexServerFreeNodes int
	hashFunction             *common.ComplexStringHash
}

func (s *ServersSharding) HealthCheck() {
	var (
		err error
	)
	for {
		s.cacheServersMutex.RLock()
		for _,cacheServer := range s.cacheServers {
			if !cacheServer.proxyClient.HasConnection() {
				err = cacheServer.proxyClient.InitConnection()
				if err != nil {
					continue
				}
			}
			// try 3 times, sleep 100 ms
			isAlive := cacheServer.healthCheck()
			if cacheServer.isAlive && !isAlive {
				s.reShardingOnUnregister([]*CacheServer{cacheServer})
				cacheServer.isAlive = false
			}
			if !cacheServer.isAlive && isAlive {
				s.reShardingOnRegister([]*CacheServer{cacheServer})
				cacheServer.isAlive = true
			}
		}
		s.cacheServersMutex.RUnlock()
		time.Sleep(100*time.Millisecond)
	}
}

func NewServersSharding(countVirtNodes, maxKeyLenght int, logger common.ILogger) *ServersSharding {
	coefP := uint64(int64(countVirtNodes)*int64(maxKeyLenght) + (rand.Int63n(math.MaxInt64 - int64(countVirtNodes*maxKeyLenght))))
	return &ServersSharding{
		logger:          logger,
		cacheServersMap: make(map[string]*CacheServer),
		virtualNodes:    make([]*CacheServer, countVirtNodes, countVirtNodes),
		hashFunction:    common.NewComplexStringHash(uint64(countVirtNodes), coefP, coefP)}
}

func (s *ServersSharding) CacheServerChangedRegistration(services []*consulapi.CatalogService) {
	var (
		cacheServer *CacheServer
		ok          bool
		service     *consulapi.CatalogService
		wanAddress  string
	)

	servicesMap := make(map[string]*consulapi.CatalogService)

	registeredCacheServers := make([]*CacheServer, 0, 1)

	for _, service = range services {
		// Exclude health check
		if service.ServicePort == 0 {
			continue
		}
		s.cacheServersMapMutex.RLock()
		cacheServer, ok = s.cacheServersMap[service.ServiceID]
		s.cacheServersMapMutex.RUnlock()

		if !ok {
			wanAddress = service.Address
			if service.TaggedAddresses != nil {
				wanAddress, ok = service.TaggedAddresses["wan"]
				if !ok {
					wanAddress = service.Address
				}
			}

			cacheServer = NewCacheServer(service.ServiceID, service.Address, wanAddress, service.ServicePort)
			err := cacheServer.proxyClient.InitConnection()
			// Check Health
			if err == nil && cacheServer.healthCheck() {
				cacheServer.isAlive = true
				registeredCacheServers = append(registeredCacheServers, cacheServer)
			} else {
				cacheServer.isAlive = false
			}

			s.cacheServersMapMutex.Lock()
			s.cacheServersMap[cacheServer.id] = cacheServer
			s.cacheServersMapMutex.Unlock()

			s.cacheServersMutex.Lock()
			s.cacheServers = append(s.cacheServers, cacheServer)
			s.cacheServersMutex.Unlock()

			fmt.Printf(`\n Services : "%#v"`, service)
		}
		servicesMap[service.ServiceID] = service
	}

	s.reShardingOnRegister(registeredCacheServers)

	s.cacheServersMutex.RLock()
	lenCacheServers := len(s.cacheServersMap)
	s.cacheServersMutex.RUnlock()

	var unregisteredCacheServers []*CacheServer
	if len(servicesMap) != lenCacheServers {
		s.cacheServersMutex.Lock()
		newCacheServers := []*CacheServer{}
		for _, cacheServer = range s.cacheServersMap {
			service, ok = servicesMap[cacheServer.id]
			if !ok {
				unregisteredCacheServers = append(unregisteredCacheServers, cacheServer)
				s.cacheServersMapMutex.Lock()
				delete(s.cacheServersMap, cacheServer.id)
				s.cacheServersMapMutex.Unlock()
			} else {
				// New array cache services
				newCacheServers = append(newCacheServers, cacheServer)
			}
		}
		s.cacheServersMutex.Lock()
		s.cacheServers = newCacheServers
		s.cacheServersMutex.Unlock()
		s.reShardingOnUnregister(unregisteredCacheServers)
	}

	fmt.Printf(`\n CacheServers: "%#v"`, s.cacheServers)
}

func (s *ServersSharding) reShardingOnUnregister(freeCacheServers []*CacheServer) {
	s.reShardingMutex.Lock()
	defer s.reShardingMutex.Unlock()
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
		fmt.Printf("\n Unregister node:'%#v` \n", freeCacheServer)
	}
}

func (s *ServersSharding) initVirtualNodesDistribution(newCacheServers []*CacheServer) {
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
		nodeIndex := freeVirtualNodes[randIndex]
		cacheServer := newCacheServers[i]
		s.virtualNodes[nodeIndex] = cacheServer
		cacheServer.addVirtualNode(nodeIndex)
		s.lastIndexServerAddNodes = i
		i++

		// Remove this index from slice
		freeVirtualNodes = append(freeVirtualNodes[:randIndex], freeVirtualNodes[randIndex+1:]...)
		if len(freeVirtualNodes) == 0 {
			break
		}
	}

	for _, cacheServer := range newCacheServers {
		fmt.Printf("\n Add Cache Server nodesLen:'%#v' '%d` \n", cacheServer, len(cacheServer.virtualNodes))
//		s.cacheServersMap[cacheServer.id] = cacheServer
	}
//	s.cacheServers = newCacheServers
}

func (s *ServersSharding) reShardingOnRegister(newCacheServers []*CacheServer) {
	s.reShardingMutex.Lock()
	defer s.reShardingMutex.Unlock()
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
	countNodesPerServer := len(s.virtualNodes) / (lenServers + lenNewServers)
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
		fmt.Printf("\n Add Cache Server:'%#v' \n", cacheServer)
//		s.cacheServersMap[cacheServer.id] = cacheServer
	}
//	s.cacheServers = append(s.cacheServers, newCacheServers...)
}

func (s *ServersSharding) GetCacheServer(key string) (*CacheServer, error) {
	if len(s.cacheServers) == 0 {
		return nil, fmt.Errorf("Virtual nodes are not initialized")
	}
	hashKey := s.hashFunction.CalculateHash(key)
	fmt.Printf("\n Cache key: %d \n", hashKey)
	fmt.Printf("\n CacheServers len: %d \n", len(s.virtualNodes))
	s.reShardingMutex.RLock()
	//fmt.Printf(`Cache Servers:"%#v"`, s.virtualNodes)
	cacheServer := s.virtualNodes[hashKey]
	s.reShardingMutex.RUnlock()
	if cacheServer == nil {
		return nil, fmt.Errorf(`Can't find cache server by index "%s"`, key)
	}
	return cacheServer, nil
}
