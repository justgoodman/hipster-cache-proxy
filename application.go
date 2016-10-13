package main

import (
	"hipster_cache_proxy/config"
	consulapi "github.com/hashicorp/consul/api"
)

type Application struct {
	Config *config.Config
}


