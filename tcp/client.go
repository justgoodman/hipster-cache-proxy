package tcp

import (
	"net"
	"fmt"
	//"io/ioutil"

	"hipster-cache-proxy/common"
)

type ProxyClient struct {
	serverAddress string
	serverPort    int
	clientPort int
	logger common.ILogger
	conn *net.TCPConn
}

func NewProxyClient(clientPort int, serverAddress string, serverPort int, logger common.ILogger) *ProxyClient {
	return &ProxyClient{clientPort: clientPort, serverAddress: serverAddress, serverPort: serverPort, logger: logger,}
}

func (this *ProxyClient) InitConnection() error {
	serverTCPAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("%s:%d", this.serverAddress, this.serverPort))
	if err != nil {
		return err
	}

	localTCPAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf(":%d", this.clientPort))


	this.conn, err = net.DialTCP("tcp", localTCPAddr, serverTCPAddr)
	if err != nil {
		return err
	}
	return nil
}

func (this *ProxyClient) SendMessage(message string) ([]byte,error) {
	var buf [512]byte
	_, err := this.conn.Write([]byte(message))
	if err != nil {
		this.logger.Errorf(`Error send message "%s", error "%s"`, message, err.Error())
		return nil,err
	}

	n, err := this.conn.Read(buf[0:])
        if err != nil {
                this.logger.Errorf(`Read message error: "%s"`, err.Error())
        }

        fmt.Println(string(buf[0:n]))
//	response, err := ioutil.ReadAll(this.conn)
	if err != nil {
		this.logger.Errorf(`Error response for message "%s", error "%s"`, message, err.Error())
		return nil, err
	}
//	fmt.Printf(string(response))
	fmt.Printf(string(buf[0:n]))
	return buf[0:n], nil
}

