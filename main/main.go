//Copyright (c) 2013, Salesforce.com, Inc.
//All rights reserved.
//
//Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
//
//	Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
//	Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
//	Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package main

import (
	"flag"
	"fmt"
	"github.com/forcedotcom/rmux"
	"net"
	"runtime"
	"strings"
	"syscall"
	"runtime/pprof"
	"os"
	"time"
	"io/ioutil"
	"encoding/json"
	"errors"
	"sync"
)

type poolConfig struct {
	Host string `json:"host"`
	Port string `json:"port"`
	Socket string `json:"socket"`
	MaxProcesses int `json:"maxProcesses"`
	PoolSize int `json:"poolSize"`

	TcpConnections []string `json:"tcpConnections"`
	UnixConnections []string `json:"unixConnections"`

	LocalTimeout time.Duration `json:"localTimeout"`
	LocalReadTimeout time.Duration `json:"localReadTimeout"`
	LocalWriteTimeout time.Duration `json:"localWriteTimeout"`

	RemoteTimeout time.Duration `json:"remoteTimeout"`
	RemoteReadTimeout time.Duration `json:"remoteReadTimeout"`
	RemoteWriteTimeout time.Duration `json:"remoteWriteTimeout"`
	RemoteConnectTimeout time.Duration `json:"remoteConnectTimeout"`
}

var host = flag.String("host", "localhost", "The host to listen for incoming connections on")
var port = flag.String("port", "6379", "The port to listen for incoming connections on")
var socket = flag.String("socket", "", "The socket to listen for incoming connections on.  If this is provided, host and port are ignored")
var maxProcesses = flag.Int("maxProcesses", 0, "The number of processes to use.  If this is not defined, go's default is used.")
var poolSize = flag.Int("poolSize", 50, "The size of the connection pools to use")
var tcpConnections = flag.String("tcpConnections", "localhost:6380 localhost:6381", "TCP connections (destination redis servers) to multiplex over")
var unixConnections = flag.String("unixConnections", "", "Unix connections (destination redis servers) to multiplex over")
var localTimeout = flag.Duration("localTimeout", 0, "Timeout to set locally (read+write)")
var localReadTimeout = flag.Duration("localReadTimeout", 0, "Timeout to set locally (read)")
var localWriteTimeout = flag.Duration("localWriteTimeout", 0, "Timeout to set locally (write)")
var remoteTimeout = flag.Duration("remoteTimeout", 0, "Timeout to set for remote redises (connect+read+write)")
var remoteReadTimeout = flag.Duration("remoteReadTimeout", 0, "Timeout to set for remote redises (read)")
var remoteWriteTimeout = flag.Duration("remoteWriteTimeout", 0, "Timeout to set for remote redises (write)")
var remoteConnectTimeout = flag.Duration("remoteConnectTimeout", 0, "Timeout to set for remote redises (connect)")
var cpuProfile = flag.String("cpuProfile", "", "Direct CPU Profile to target file")
var configFile = flag.String("config", "", "Configuration file (JSON)")

func main() {
	flag.Parse()

	var configs []poolConfig
	var err error

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when creating cpu profile file: %s\r\n", err)
			os.Exit(1)
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}


	if *configFile != "" {
		configs, err = configureFromFile(*configFile)
	} else {
		configs, err = configureFromArgs()
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing configuration options: %s\n", err)
		os.Exit(1)
	}

	rmuxInstances, err := createInstances(configs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating rmux instances: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Starting %d rmux instances\r\n", len(rmuxInstances))

	start(rmuxInstances)
}

func configureFromArgs() ([]poolConfig, error) {
	var arrTcpConnections []string
	if *tcpConnections != "" {
		arrTcpConnections = strings.Split(*tcpConnections, "")
	} else {
		arrTcpConnections = []string{}
	}

	var arrUnixConnections []string
	if *unixConnections != "" {
		arrUnixConnections = strings.Split(*unixConnections, "")
	} else {
		arrUnixConnections = []string{}
	}

	return []poolConfig { {
		Host: *host,
		Port: *port,
		Socket: *socket,
		MaxProcesses: *maxProcesses,
		PoolSize: *poolSize,

		TcpConnections: arrTcpConnections,
		UnixConnections: arrUnixConnections,

		LocalTimeout: *localTimeout,
		LocalReadTimeout: *localReadTimeout,
		LocalWriteTimeout: *localWriteTimeout,

		RemoteTimeout: *remoteTimeout,
		RemoteReadTimeout: *remoteReadTimeout,
		RemoteWriteTimeout: *remoteWriteTimeout,
		RemoteConnectTimeout: *remoteConnectTimeout,
	} }, nil
}

func configureFromFile(configFile string) ([]poolConfig, error) {
	fileContents, err :=  ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	var configs []poolConfig

	err = json.Unmarshal(fileContents, &configs)
	if err != nil {
		return nil, err
	}

	return configs, nil
}

func createInstances(configs []poolConfig) ([]*rmux.RedisMultiplexer, error) {
	rmuxInstances := make([]*rmux.RedisMultiplexer, len(configs))

	fmt.Printf("+%v\r\n", configs)

	for i, config := range configs {
		var rmuxInstance *rmux.RedisMultiplexer
		var err error

		if config.MaxProcesses > 0 {
			fmt.Printf("Max processes increased to: %d from: %d\r\n", config.MaxProcesses, runtime.GOMAXPROCS(config.MaxProcesses))
		}

		if config.PoolSize < 1 {
			fmt.Println("Pool size must be positive - defaulting to 50\r\n")
			config.PoolSize = 50
		}

		if config.Socket != "" {
			syscall.Umask(0111)
			fmt.Printf("Initializing rmux server on socket %s\r\n", config.Socket)
			rmuxInstance, err = rmux.NewRedisMultiplexer("unix", config.Socket, config.PoolSize)
		} else {
			fmt.Printf("Initializing rmux server on host: %s and port: %s\r\n", config.Host, config.Port)
			rmuxInstance, err = rmux.NewRedisMultiplexer("tcp", net.JoinHostPort(config.Host, config.Port), config.PoolSize)
		}

		if err != nil {
			return nil, err
		}

		if len(config.TcpConnections) > 0 {
			for _, tcpConnection := range config.TcpConnections {
				fmt.Printf("Adding tcp (destination) connection: %s\r\n", tcpConnection)
				rmuxInstance.AddConnection("tcp", tcpConnection)
			}
		}

		if len(config.UnixConnections) > 0 {
			for _, unixConnection := range config.UnixConnections {
				fmt.Printf("Adding unix (destination) connection: %s\r\n", unixConnection)
				rmuxInstance.AddConnection("unix", unixConnection)
			}
		}

		if rmuxInstance.PrimaryConnectionPool == nil {
			return nil, errors.New("You must have at least one connection defined")
		}

		if config.LocalTimeout != 0 {
			rmuxInstance.ClientReadTimeout = config.LocalTimeout
			rmuxInstance.ClientWriteTimeout = config.LocalTimeout
			fmt.Printf("Setting local client read and write timeouts to: %s\r\n", config.LocalTimeout)
		}

		if config.LocalReadTimeout != 0 {
			rmuxInstance.ClientReadTimeout = config.LocalReadTimeout
			fmt.Printf("Setting local client read timeout to: %s\r\n", config.LocalReadTimeout)
		}

		if config.LocalWriteTimeout != 0 {
			rmuxInstance.ClientWriteTimeout = config.LocalWriteTimeout
			fmt.Printf("Setting local client write timeout to: %s\r\n", config.LocalWriteTimeout)
		}

		if config.RemoteTimeout != 0 {
			rmuxInstance.EndpointConnectTimeout = config.RemoteTimeout
			rmuxInstance.EndpointReadTimeout = config.RemoteTimeout
			rmuxInstance.EndpointWriteTimeout = config.RemoteTimeout
			fmt.Printf("Setting remote redis connect, read, and write timeouts to: %s\r\n", config.RemoteTimeout)
		}

		if config.RemoteConnectTimeout != 0 {
			rmuxInstance.EndpointConnectTimeout = config.RemoteConnectTimeout
			fmt.Printf("Setting remote redis connect timeout to: %s\r\n", config.RemoteConnectTimeout)
		}

		if config.RemoteReadTimeout != 0 {
			rmuxInstance.EndpointReadTimeout = config.RemoteReadTimeout
			fmt.Printf("Setting remote redis read timeouts to: %s\r\n", config.RemoteReadTimeout)
		}

		if config.RemoteWriteTimeout != 0 {
			rmuxInstance.EndpointWriteTimeout = config.RemoteWriteTimeout
			fmt.Printf("Setting remote redis write timeout to: %s\r\n", config.RemoteWriteTimeout)
		}

		rmuxInstances[i] = rmuxInstance
	}

	return rmuxInstances, nil
}

func start(rmuxInstances []*rmux.RedisMultiplexer) {
	var waitGroup sync.WaitGroup

	defer func() {
		for _, rmuxInstance := range rmuxInstances {
			rmuxInstance.Listener.Close()
		}
	}()

	for _, rmuxInstance := range rmuxInstances {
		waitGroup.Add(1)

		go func(instance *rmux.RedisMultiplexer) {
			instance.Start()
			waitGroup.Done()
		}(rmuxInstance)
	}

	waitGroup.Wait()
}

