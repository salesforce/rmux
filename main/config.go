package main

import (
	"time"
	"io/ioutil"
	"encoding/json"
)

type PoolConfig struct {
	Host                   string          `json:"host"`
	Port                   int             `json:"port"`
	Socket                 string          `json:"socket"`
	MaxProcesses           int             `json:"maxProcesses"`
	PoolSize               int             `json:"poolSize"`

	TcpConnections         []string        `json:"tcpConnections"`
	UnixConnections        []string        `json:"unixConnections"`

	LocalTimeout           time.Duration   `json:"localTimeout"`
	LocalReadTimeout       time.Duration   `json:"localReadTimeout"`
	LocalWriteTimeout      time.Duration   `json:"localWriteTimeout"`

	RemoteTimeout          time.Duration   `json:"remoteTimeout"`
	RemoteReadTimeout      time.Duration   `json:"remoteReadTimeout"`
	RemoteWriteTimeout     time.Duration   `json:"remoteWriteTimeout"`
	RemoteConnectTimeout   time.Duration   `json:"remoteConnectTimeout"`
}

func ReadConfigFromFile(configFile string) ([]PoolConfig, error) {
	fileContents, err :=  ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	configs, err := ParseConfigJson(fileContents)
	if err != nil {
		return nil, err
	}

	return configs, nil
}

func ParseConfigJson(configJson []byte) ([]PoolConfig, error) {
	var configs []PoolConfig

	if err := json.Unmarshal(configJson, &configs); err != nil {
		return nil, err
	}

	return configs, nil
}

