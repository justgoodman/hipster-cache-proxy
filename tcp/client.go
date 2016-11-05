package tcp

import (
	"fmt"
	"net"
	"sync"

	"hipster-cache-proxy/common"
)

type ProxyClient struct {
	serverAddress string
	serverPort    int
	logger        common.ILogger
	conn          net.Conn
	sendMessageMutex sync.Mutex
}

func NewProxyClient(serverAddress string, serverPort int, logger common.ILogger) *ProxyClient {
	return &ProxyClient{serverAddress: serverAddress, serverPort: serverPort, logger: logger}
}

func (c *ProxyClient) HasConnection() bool {
	return c.conn != nil
}


func (c *ProxyClient) InitConnection() error {
	/*
		serverTCPAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("%s:%d", c.serverAddress, c.serverPort))
		if err != nil {
			return err
		}
		localTCPAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf(":%d", c.clientPort))
	*/
	var err error
	fmt.Printf("Try to connect with: %s:%d", c.serverAddress, c.serverPort)
	c.conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.serverAddress, c.serverPort))
	if err != nil {
		return err
	}
	return nil
}

func (c *ProxyClient) SendMessage(message string) (string, error) {
	c.sendMessageMutex.Lock()
	defer c.sendMessageMutex.Unlock()
	var buf [512]byte
	_, err := c.conn.Write([]byte(message))
	if err != nil {
		c.logger.Errorf(`Error send message "%s", error "%s"`, message, err.Error())
		c.conn.Close()
		c.InitConnection()
		return "", err
	}

	n, err := c.conn.Read(buf[0:])
	if err != nil {
		c.logger.Errorf(`Read message error: "%s"`, err.Error())
		c.conn.Close()
		c.InitConnection()
	}

	fmt.Println(string(buf[0:n]))
	//	response, err := ioutil.ReadAll(c.conn)
	if err != nil {
		c.logger.Errorf(`Error response for message "%s", error "%s"`, message, err.Error())
		return "", err
	}
	//	fmt.Printf(string(response))
	fmt.Printf(string(buf[0:n]))
	return string(buf[0:n]), nil
}
