package config

import (
	"io/ioutil"
	"fmt"
	"encoding/json"
)

type Config struct {
	Port int `json:"port"`
	ConsulAddress string `json:"consul_address"`
}

func NewConfig() *Config {
	return &Config{}
}

func (this *Config) LoadFile(configPath string) error {
	configString, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(configString, this)
	if err != nil {
		return err
	}
	fmt.Printf("%#v",this)
	return nil
}

