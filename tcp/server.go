package tcp

import (
	"net"
	"fmt"
//	"time"

        "hipster-cache-proxy/common"
)

type ProxyServer struct {
	port int
	logger common.ILogger
	listener *net.TCPListener
}

func NewProxyServer(port int, logger common.ILogger) *ProxyServer {
	return &ProxyServer{port:port, logger:logger}
}

func (this *ProxyServer) Init() error {
     tcpAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf(`:%d`,this.port))
     if err != nil {
		return err
	}

     this.listener, err = net.ListenTCP("tcp", tcpAddr)
     return err
}

func (this *ProxyServer) Run() {
     for {
           conn, err := this.listener.Accept()
           if err != nil {
                this.logger.Errorf(`Connection error: "%s"`, err.Error())
                continue
           }
           go this.handleMessage(conn)
//      conn.Write([]byte("Bratish vse ok"))
//      conn.Close()
     }
}

func (this *ProxyServer) handleMessage(conn net.Conn) {
	var buf [512]byte
	for {
		n, err := conn.Read(buf[0:])
		if err != nil {
			this.logger.Errorf(`Read message error: "%s"`, err.Error())
		}
		fmt.Println(string(buf[0:n]))
	//	time.Sleep(time.Second * 10)
//		conn.Close()
//		return
	}
	return
}

