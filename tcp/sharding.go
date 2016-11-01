package tcp

import (
	"fmt"
	"math"
	"math/rand"
	"sync"

	consulapi "github.com/hashicorp/consul/api"
	"hipster-cache-proxy/common"
)

type ServersSharding struct {
	logger                   common.ILogger
	cacheServersMap          map[string]*CacheServer
	cacheServers             []*CacheServer
	mutexCacheServers        sync.RWMutex
	virtualNodes             []*CacheServer
	lastIndexServerAddNodes  int
	lastIndexServerFreeNodes int
	hashFunction             *common.ComplexStringHash
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
	)

	servicesMap := make(map[string]*consulapi.CatalogService)

	registeredCacheServers := make([]*CacheServer, 0, 1)

	for _, service = range services {
		// Exclude health check
		if service.ServicePort == 0 {
			continue
		}
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

func (s *ServersSharding) reCachingOnUnregister(freeCacheServers []*CacheServer) {
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
		s.cacheServersMap[cacheServer.id] = cacheServer
	}
	s.cacheServers = newCacheServers
}

func (s *ServersSharding) reCachingOnRegister(newCacheServers []*CacheServer) {
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
		s.cacheServersMap[cacheServer.id] = cacheServer
	}
	s.cacheServers = append(s.cacheServers, newCacheServers...)
}

func (s *ServersSharding) GetCacheServer(key string) (*CacheServer, error) {
	if len(s.cacheServers) == 0 {
		return nil, fmt.Errorf("Virtual nodes are not initialized")
	}
	hashKey := s.hashFunction.CalculateHash(key)
	fmt.Printf("\n Cache key: %d \n", hashKey)
	fmt.Printf("\n CacheServers len: %d \n", len(s.virtualNodes))
	s.mutexCacheServers.RLock()
	//fmt.Printf(`Cache Servers:"%#v"`, s.virtualNodes)
	cacheServer := s.virtualNodes[hashKey]
	s.mutexCacheServers.RUnlock()
	if cacheServer == nil {
		return nil, fmt.Errorf(`Can't find cache server by index "%s"`, key)
	}
	return cacheServer, nil
}
