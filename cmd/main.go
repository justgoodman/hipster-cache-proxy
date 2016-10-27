package main

import (
	"flag"
	//	"fmt"
	"os"
	//	"net/http"

	//	"github.com/prometheus/client_golang/prometheus"
	"github.com/juju/loggo"
	app "hipster-cache-proxy"
	"hipster-cache-proxy/config"
)

func main() {

	logger := loggo.GetLogger("")

	var configFile string

	flag.StringVar(&configFile, "config", "etc/application.json", "Config file")
	flag.Parse()

	config := config.NewConfig()
	err := config.LoadFile(configFile)
	if err != nil {
		logger.Criticalf("Error reading configuration file: '%s'", err.Error())
		os.Exit(1)
	}
	//	logger.Errorf("Test Error")
	application := app.NewApplication(config, logger)
	//	fmt.Printf("#%v", application)
	err = application.Init()
	if err != nil {
		logger.Criticalf("error initialization application: '%s'", err.Error())
		os.Exit(1)
	}
	application.Run()
	//	http.Handle("/metrics", prometheus.Handler())
	//http.ListenAndServe(*addr, nil)
}
