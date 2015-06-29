/*
 * Copyright (c) 2015, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/forcedotcom/rmux"
	. "github.com/forcedotcom/rmux/log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"github.com/forcedotcom/rmux/graphite"
	"time"
)

const DEFAULT_POOL_SIZE = 20

var host = flag.String("host", "localhost", "The host to listen for incoming connections on")
var port = flag.Int("port", 6379, "The port to listen for incoming connections on")
var socket = flag.String("socket", "", "The socket to listen for incoming connections on.  If this is provided, host and port are ignored")
var maxProcesses = flag.Int("maxProcesses", 0, "The number of processes to use.  If this is not defined, go's default is used.")
var poolSize = flag.Int("poolSize", DEFAULT_POOL_SIZE, "The size of the connection pools to use")
var tcpConnections = flag.String("tcpConnections", "localhost:6380 localhost:6381", "TCP connections (destination redis servers) to multiplex over")
var unixConnections = flag.String("unixConnections", "", "Unix connections (destination redis servers) to multiplex over")
var localTimeout = flag.Int64("localTimeout", 0, "Timeout to set locally in milliseconds (read+write)")
var localReadTimeout = flag.Int64("localReadTimeout", 0, "Timeout to set locally in milliseconds (read)")
var localWriteTimeout = flag.Int64("localWriteTimeout", 0, "Timeout to set locally (write)")
var remoteTimeout = flag.Int64("remoteTimeout", 0, "Timeout to set for remote redises (connect+read+write)")
var remoteReadTimeout = flag.Int64("remoteReadTimeout", 0, "Timeout to set for remote redises (read)")
var remoteWriteTimeout = flag.Int64("remoteWriteTimeout", 0, "Timeout to set for remote redises (write)")
var remoteConnectTimeout = flag.Int64("remoteConnectTimeout", 0, "Timeout to set for remote redises (connect)")
var cpuProfile = flag.String("cpuProfile", "", "Direct CPU Profile to target file")
var configFile = flag.String("config", "", "Configuration file (JSON)")
var doDebug = flag.Bool("debug", false, "Debug mode")
var graphiteServer = flag.String("graphite", "", "Graphite statsd endpoint")
var doTiming = flag.Bool("timing", false, "Send command timings to graphite")
var failover = flag.Bool("failover", false, "Failover to another connection pool if target pool is down in mux mode")

func main() {
	flag.Parse()

	var configs []PoolConfig
	var err error

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		terminateIfError(err, "Error when creating cpu profile file: %s\r\n")

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *doDebug {
		SetLogLevel(LOG_DEBUG)
	} else {
		SetLogLevel(LOG_INFO)
	}

	if *graphiteServer != "" {
		Info("Enabling graphite stats")
		err := graphite.SetEndpoint(*graphiteServer)
		if err != nil {
			Error("Error when setting graphite endpoint: %s", err)
		}
	}

	if *doTiming {
		Info("Enabling graphite timings")
		graphite.EnableTimings()
	}

	if *configFile != "" {
		configs, err = ReadConfigFromFile(*configFile)
	} else {
		configs, err = configureFromArgs()
	}
	terminateIfError(err, "Error parsing configuration options: %s\r\n")

	rmuxInstances, err := createInstances(configs)
	terminateIfError(err, "Error creating rmux instances: %s\r\n")

	Info("Starting %d rmux instances", len(rmuxInstances))

	start(rmuxInstances)
}

func configureFromArgs() ([]PoolConfig, error) {
	var arrTcpConnections []string
	if *tcpConnections != "" {
		arrTcpConnections = strings.Split(*tcpConnections, " ")
	} else {
		arrTcpConnections = []string{}
	}

	var arrUnixConnections []string
	if *unixConnections != "" {
		arrUnixConnections = strings.Split(*unixConnections, " ")
	} else {
		arrUnixConnections = []string{}
	}

	config := []PoolConfig{{
		Host:         *host,
		Port:         *port,
		Socket:       *socket,
		MaxProcesses: *maxProcesses,
		PoolSize:     *poolSize,
		Failover:     *failover,

		TcpConnections:  arrTcpConnections,
		UnixConnections: arrUnixConnections,

		LocalTimeout:      *localTimeout,
		LocalReadTimeout:  *localReadTimeout,
		LocalWriteTimeout: *localWriteTimeout,

		RemoteTimeout:        *remoteTimeout,
		RemoteReadTimeout:    *remoteReadTimeout,
		RemoteWriteTimeout:   *remoteWriteTimeout,
		RemoteConnectTimeout: *remoteConnectTimeout,
	}}

	return config, nil
}

func createInstances(configs []PoolConfig) (rmuxInstances []*rmux.RedisMultiplexer, err error) {
	rmuxInstances = make([]*rmux.RedisMultiplexer, len(configs))

	defer func() {
		if err != nil {
			for _, instance := range rmuxInstances {
				if instance == nil {
					continue
				}

				instance.Listener.Close()
			}

			rmuxInstances = nil
		}
	}()

	for i, config := range configs {
		var rmuxInstance *rmux.RedisMultiplexer

		if config.MaxProcesses > 0 {
			Info("Max processes increased to: %d from: %d", config.MaxProcesses, runtime.GOMAXPROCS(config.MaxProcesses))
		}

		if config.PoolSize < 1 {
			Info("Pool size must be positive - defaulting to %d", DEFAULT_POOL_SIZE)
			config.PoolSize = DEFAULT_POOL_SIZE
		}

		if config.Socket != "" {
			syscall.Umask(0111)
			Info("Initializing rmux server on socket %s", config.Socket)
			rmuxInstance, err = rmux.NewRedisMultiplexer("unix", config.Socket, config.PoolSize)
		} else {
			Info("Initializing rmux server on host: %s and port: %d", config.Host, config.Port)
			rmuxInstance, err = rmux.NewRedisMultiplexer("tcp", net.JoinHostPort(config.Host, strconv.Itoa(config.Port)), config.PoolSize)
		}

		rmuxInstances[i] = rmuxInstance

		if err != nil {
			return
		}

		rmuxInstance.Failover = config.Failover

		if config.LocalTimeout != 0 {
			timeout := time.Duration(config.LocalTimeout) * time.Millisecond
			rmuxInstance.ClientReadTimeout = timeout
			rmuxInstance.ClientWriteTimeout = timeout
			Info("Setting local client read and write timeouts to: %s", timeout)
		}

		if config.LocalReadTimeout != 0 {
			timeout := time.Duration(config.LocalReadTimeout) * time.Millisecond
			rmuxInstance.ClientReadTimeout = timeout
			Info("Setting local client read timeout to: %s", timeout)
		}

		if config.LocalWriteTimeout != 0 {
			timeout := time.Duration(config.LocalWriteTimeout) * time.Millisecond
			rmuxInstance.ClientWriteTimeout = timeout
			Info("Setting local client write timeout to: %s", timeout)
		}

		if config.RemoteTimeout != 0 {
			duration := time.Duration(config.RemoteTimeout) * time.Millisecond
			rmuxInstance.EndpointConnectTimeout = duration
			rmuxInstance.EndpointReadTimeout = duration
			rmuxInstance.EndpointWriteTimeout = duration
			Info("Setting remote redis connect, read, and write timeouts to: %s", duration)
		}

		if config.RemoteConnectTimeout != 0 {
			duration := time.Duration(config.RemoteConnectTimeout) * time.Millisecond
			rmuxInstance.EndpointConnectTimeout = duration
			Info("Setting remote redis connect timeout to: %s", duration)
		}

		if config.RemoteReadTimeout != 0 {
			duration := time.Duration(config.RemoteReadTimeout) * time.Millisecond
			rmuxInstance.EndpointReadTimeout = duration
			Info("Setting remote redis read timeouts to: %s", duration)
		}

		if config.RemoteWriteTimeout != 0 {
			duration := time.Duration(config.RemoteWriteTimeout) * time.Millisecond
			rmuxInstance.EndpointWriteTimeout = duration
			Info("Setting remote redis write timeout to: %s", duration)
		}

		if len(config.TcpConnections) > 0 {
			for _, tcpConnection := range config.TcpConnections {
				Info("Adding tcp (destination) connection: %s", tcpConnection)
				rmuxInstance.AddConnection("tcp", tcpConnection)
			}
		}

		if len(config.UnixConnections) > 0 {
			for _, unixConnection := range config.UnixConnections {
				Info("Adding unix (destination) connection: %s", unixConnection)
				rmuxInstance.AddConnection("unix", unixConnection)
			}
		}

		if rmuxInstance.PrimaryConnectionPool == nil {
			err = errors.New("You must have at least one connection defined")
			return
		}
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

	for i, rmuxInstance := range rmuxInstances {
		waitGroup.Add(1)

		go func(instance *rmux.RedisMultiplexer) {
			defer waitGroup.Done()

			err := instance.Start()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error starting rmux instance %d: %s", i, err)
				return
			}
		}(rmuxInstance)
	}

	waitGroup.Wait()
}

// Terminates the program if the passed in error does not evaluate to nil.
// err will be the first value of formatted string
func terminateIfError(err error, format string, a ...interface{}) {
	if err != nil {
		allArgs := append([]interface{}{err}, a...)

		Error(format, allArgs...)
		os.Exit(1)
	}
}
