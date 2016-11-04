package tcp

import (
	"math/rand"
)

type CacheServer struct {
	id           string
	address      string
	wanAddress   string
	port         int
	proxyClient  *ProxyClient
	virtualNodes []int
}

func (s *CacheServer) healthCheck() bool {
	response, err := cacheServer.ProxyClient.sendMessage("ping")
	return err!= nil && response == "pong"
}

func (s *CacheServer) addVirtualNode(nodeIndex int) {
	s.virtualNodes = append(s.virtualNodes, nodeIndex)
}

func (s *CacheServer) getVirtualNode() int {
	randIndex := rand.Intn(len(s.virtualNodes))
	nodeIndex := s.virtualNodes[randIndex]
	// Delete link to virtual node
	s.virtualNodes = append(s.virtualNodes[:randIndex][(randIndex + 1):])
	return nodeIndex
}

func NewCacheServer(id, address, wanAddress string, port int) *CacheServer {
	return &CacheServer{id: id, address: address, wanAddress: wanAddress, port: port}
}
