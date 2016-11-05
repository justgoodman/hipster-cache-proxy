package tcp

import (
	 "math/rand"
	"fmt"

	"hipster-cache-proxy/common"
)

type CacheServer struct {
	id           string
	address      string
	wanAddress   string
	port         int
	proxyClient  *ProxyClient
	virtualNodes []int
	isAlive	     bool
}

func (s *CacheServer) healthCheck() bool {
	response, err := s.proxyClient.SendMessage("ping")
	fmt.Printf("\n Response HealthCheck %s err: %#v  %t \n", response, err, response == (`"pong"` + "\n"))
	return err == nil && response == `"pong"` + endSymbol
}

func (s *CacheServer) addVirtualNode(nodeIndex int) {
	s.virtualNodes = append(s.virtualNodes, nodeIndex)
}

func (s *CacheServer) getVirtualNode() int {
	fmt.Printf("\n LenVirtNodes :%d \n", len(s.virtualNodes))
	randIndex := rand.Intn(len(s.virtualNodes))
	nodeIndex := s.virtualNodes[randIndex]
	// Delete link to virtual node
	s.virtualNodes = append(s.virtualNodes[:randIndex][(randIndex + 1):])
	return nodeIndex
}

func NewCacheServer(id, address, wanAddress string, port int, logger common.ILogger) *CacheServer {
	return &CacheServer{
		id: id,
		address: address,
		wanAddress: wanAddress,
		port: port,
		proxyClient: NewProxyClient(address,port,logger),
	}
}
