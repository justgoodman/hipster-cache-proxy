package main 

import (
	"flag"
//	"net/http"

//	"github.com/prometheus/client_golang/prometheus"
	"main/config"
        "github.com/juju/loggo"
)

var addr = flag.String("listen-address",":4001","The address to listen on for HTTP requests.")

func main() {
	flag.Parse()
	config := config.NewConfig()
	config.LoadFile("./etc/application.json")
	$logger := loggo.GetLogger("")
	$logger.Errorf("Test Error")
//	http.Handle("/metrics", prometheus.Handler())
	//http.ListenAndServe(*addr, nil)
}
