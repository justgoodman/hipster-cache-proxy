package tcp

import (
	"net"
	"fmt"
	"io/ioutil"

	"hipster-cache-proxy/common"
)

type ClientProxy struct {
	serverAddress string
	serverPort    int
	localPort int
	logger common.ILogger
	conn *net.TCPConn
}

func NewClientProxy(localPort int, serverAddress string, serverPort int, logger common.ILogger) *ClientProxy {
	return &ClientProxy{localPort: localPort, serverAddress: serverAddress, serverPort: serverPort, logger: logger,}
}
/*
func (this *ClientProxy) getIPbyHost(host string) net.IP, error {
	fmt.Printf("Init Connection")
        ips,err := net.LookupIP(this.serverAddress)
        fmt.Printf("\n Finish \n")
        if err != nil {
		retun nil, err
        }
        if len(ips) != 1 {
		return nil, fmt.Errorf(`Count IPs in not equal to 1, currect count is "%d"`, len(ips))
	)
	return ips[0]
*/
func (this *ClientProxy) InitConnection() error {
/*
        ip,err := this.getIPbyHost(this.serverAddress)
        if err != nil {
                this.logger.Errorf(`Error get IP:"%s"`, err)
                return
        }
*/
	serverTCPAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("%s:%d", this.serverAddress, this.serverPort))
	if err != nil {
		return err
	}

	localTCPAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf(":%d", this.localPort))


	this.conn, err = net.DialTCP("tcp", localTCPAddr, serverTCPAddr)
	if err != nil {
		return err
	}
	return nil
}

func (this *ClientProxy) SendMessage(message string) (string,error) {
	_, err := this.conn.Write([]byte(message))
	if err != nil {
		this.logger.Errorf(`Error send message "%s", error "%s"`, message, err.Error())
		return "",err
	}
	response, err := ioutil.ReadAll(this.conn)
	if err != nil {
		this.logger.Errorf(`Error response for message "%s", error "%s"`, message, err.Error())
		return "", err
	}
	fmt.Printf(string(response))
	return string(response), nil
}

