package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Config struct {
	MetricsPort    int    `json:"metrics_port"`
	ServerPort     int    `json:"server_port"`
	ClientPort     int    `json:"client_port"`
	Address        string `json:"address"`
	ConsulAddress  string `json:"consul_address"`
	CountVirtNodes int    `json:"count_virt_nodes"`
	MaxKeyLenght   int    `json:"max_key_lenght"`
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) LoadFile(configPath string) error {
	configString, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(configString, c)
	if err != nil {
		return err
	}

	if serverIP := os.Getenv("SERVER_IP"); serverIP != "" {
		c.Address = serverIP
	}

	if consulURL := os.Getenv("CONSUL_URL"); consulURL != "" {
		c.ConsulAddress = consulURL
	}

	fmt.Printf("%#v", c)
	return nil
}
