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
)

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

func main() {
	flag.Parse()
	if *maxProcesses > 0 {
		fmt.Printf("Max processes increased to: %d from: %d\r\n", *maxProcesses, runtime.GOMAXPROCS(*maxProcesses))
	}
	if *poolSize < 1 {
		fmt.Println("Pool size must be positive\r\n")
	}
	var rmuxInstance *rmux.RedisMultiplexer
	var err error
	if *socket != "" {
		syscall.Umask(0111)
		fmt.Printf("Initializing rmux server on socket %s\r\n", *socket)
		rmuxInstance, err = rmux.NewRedisMultiplexer("unix", *socket, *poolSize)
	} else {
		fmt.Printf("Initializing rmux server on host: %s and port: %s\r\n", *host, *port)
		rmuxInstance, err = rmux.NewRedisMultiplexer("tcp", net.JoinHostPort(*host, *port), *poolSize)
	}

	if err != nil {
		println("Rmux Initialization Error", err.Error())
		return
	}

	defer func() {
		rmuxInstance.Listener.Close()
	}()

	if *tcpConnections != "" {
		for _, tcpConnection := range strings.Split(*tcpConnections, " ") {
			fmt.Printf("Adding tcp (destination) connection: %s\r\n", tcpConnection)
			rmuxInstance.AddConnection("tcp", tcpConnection)
		}
	}

	if *unixConnections != "" {
		for _, unixConnection := range strings.Split(*unixConnections, " ") {
			fmt.Printf("Adding unix (destination) connection: %s\r\n", unixConnection)
			rmuxInstance.AddConnection("unix", unixConnection)
		}
	}

	if rmuxInstance.PrimaryConnectionKey == "" {
		fmt.Printf("You must have at least one connection defined\r\n")
		return
	}

	if *localTimeout != 0 {
		rmuxInstance.ClientReadTimeout = *localTimeout
		rmuxInstance.ClientWriteTimeout = *localTimeout
		fmt.Printf("Setting local client read and write timeouts to: %s\r\n", *localTimeout)
	}

	if *localReadTimeout != 0 {
		rmuxInstance.ClientReadTimeout = *localReadTimeout
		fmt.Printf("Setting local client read timeout to: %s\r\n", *localReadTimeout)
	}

	if *localWriteTimeout != 0 {
		rmuxInstance.ClientWriteTimeout = *localWriteTimeout
		fmt.Printf("Setting local client write timeout to: %s\r\n", *localWriteTimeout)
	}

	if *remoteTimeout != 0 {
		rmuxInstance.EndpointConnectTimeout = *remoteTimeout
		rmuxInstance.EndpointReadTimeout = *remoteTimeout
		rmuxInstance.EndpointWriteTimeout = *remoteTimeout
		fmt.Printf("Setting remote redis connect, read, and write timeouts to: %s\r\n", *remoteTimeout)
	}

	if *remoteConnectTimeout != 0 {
		rmuxInstance.EndpointConnectTimeout = *remoteConnectTimeout
		fmt.Printf("Setting remote redis connect timeout to: %s\r\n", *remoteConnectTimeout)
	}

	if *remoteReadTimeout != 0 {
		rmuxInstance.EndpointReadTimeout = *remoteReadTimeout
		fmt.Printf("Setting remote redis read timeouts to: %s\r\n", *remoteReadTimeout)
	}

	if *remoteWriteTimeout != 0 {
		rmuxInstance.EndpointWriteTimeout = *remoteWriteTimeout
		fmt.Printf("Setting remote redis write timeout to: %s\r\n", *remoteWriteTimeout)
	}

	rmuxInstance.Start()
}
