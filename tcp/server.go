package tcp

import (
	"fmt"
	"io"
	"net"

	"hipster-cache-proxy/common"
)

const (
	getShardingCommand = "GET_SHARDING"
	getShardCommand = "GET_SHARD"
	exitCommand     = "EXIT"
	pingCommand	= "PING"
	endSymbol       = "\n"
)

type ProxyServer struct {
	serverPort      int
	listener        *net.TCPListener
	logger          common.ILogger
	ServersSharding *ServersSharding
}

func NewProxyServer(serverPort, countVirtNodes, maxKeyLenght int, logger common.ILogger) *ProxyServer {
	return &ProxyServer{
		serverPort:      serverPort,
		logger:          logger,
		ServersSharding: NewServersSharding(countVirtNodes, maxKeyLenght, logger),
	}
}

func (s *ProxyServer) Init() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf(`:%d`, s.serverPort))
	if err != nil {
		return err
	}

	s.listener, err = net.ListenTCP("tcp", tcpAddr)
	return err
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
	var (
		buf           [512]byte
		clientMessage *ClientMessage
	)
	for {
		n, err := conn.Read(buf[0:])
		if err != nil {
			s.logger.Errorf(`Read message error: "%s"`, err.Error())
			// Connection closed by client
			if err == io.EOF {
				if err = conn.Close(); err != nil {
					s.logger.Errorf(`Close connection error: "%s"`, err.Error())
				}
				return
			}
		}
		command := string(buf[0:n])
		fmt.Printf(`Response "%s"`, command)

		clientMessage, err = s.getClientMessage(command)
		if err != nil {
			conn.Write([]byte(err.Error() + endSymbol))
			return
		}

		if clientMessage.command == exitCommand	{
			conn.Close()
			return
		}

		response, err := s.getResponse(command, clientMessage)
		if err != nil {
			response = err.Error() + endSymbol
		}
		if err != nil {
			s.logger.Errorf(`Error message: "%s"`, err.Error())
		}
		fmt.Printf(`Response: "%s"`, string(response))
		conn.Write([]byte(response))
	}
	return
}

func (s *ProxyServer) getClientMessage(command string) (*ClientMessage, error) {
	clientMessage := NewClientMessage()
	if err := clientMessage.Init(command); err != nil {
		return nil, err
	}
	return clientMessage, nil
}

func (s *ProxyServer) getResponse(command string, clientMessage *ClientMessage) (string, error) {
	switch clientMessage.command {
		case getShardingCommand:
			if len(clientMessage.params) != 0 {
				return "", fmt.Errorf(`Error: incorrect parametes count, it needs 0, was sended "%d"`, len(clientMessage.params))
			}
			return s.ServersSharding.GetShardingInfo()
		// Command get shard node
		case getShardCommand:
			if len(clientMessage.params) != 1 {
				return "", fmt.Errorf(`Error: incorrect parametes count, it needs 1, was sended "%d"`, len(clientMessage.params))
			}
			key := clientMessage.params[0]
			cacheServer, err := s.ServersSharding.GetCacheServer(key)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf(`"%s:%d"`, cacheServer.wanAddress, cacheServer.port) + endSymbol, nil
		case pingCommand:
			if len(clientMessage.params) != 0 {
				return "", fmt.Errorf(`Error: incorrect parametes count, it needs 0, was sended "%d"`, len(clientMessage.params))
			}
			return `"pong"` + endSymbol, nil
		default:
			if len(clientMessage.params) == 0 {
				return "", fmt.Errorf(`Error: incorrect parametes count, it needs minimum 1, was sended "%d"`, len(clientMessage.params))
			}
			key := clientMessage.params[0]
			cacheServer, err := s.ServersSharding.GetCacheServer(key)
			if err != nil {
				return "", err
			}
			if cacheServer.proxyClient == nil {
				cacheServer.proxyClient = NewProxyClient(cacheServer.address, cacheServer.port, s.logger)
				// TODO: Repeat if connection false
				err := cacheServer.proxyClient.InitConnection()
				if err != nil {
					s.logger.Errorf(`Error init connection with the CacheServer "%s"r`, err.Error())
				}
			}
			return cacheServer.proxyClient.SendMessage(command)

	}
}
