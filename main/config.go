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
	"encoding/json"
	"io/ioutil"
)

type PoolConfig struct {
	Host                 string   `json:"host"`
	Port                 int      `json:"port"`
	Socket               string   `json:"socket"`
	MaxProcesses         int      `json:"maxProcesses"`
	PoolSize             int      `json:"poolSize"`
	TcpConnections       []string `json:"tcpConnections"`
	UnixConnections      []string `json:"unixConnections"`
	LocalTimeout         int64    `json:"localTimeout"`
	LocalReadTimeout     int64    `json:"localReadTimeout"`
	LocalWriteTimeout    int64    `json:"localWriteTimeout"`
	RemoteTimeout        int64    `json:"remoteTimeout"`
	RemoteReadTimeout    int64    `json:"remoteReadTimeout"`
	RemoteWriteTimeout   int64    `json:"remoteWriteTimeout"`
	RemoteConnectTimeout int64    `json:"remoteConnectTimeout"`
	Failover             bool     `json:"failover"`
}

func ReadConfigFromFile(configFile string) ([]PoolConfig, error) {
	fileContents, err := ioutil.ReadFile(configFile)
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
