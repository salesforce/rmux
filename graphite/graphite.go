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

package graphite

import (
	"os"
	"net"
	"fmt"
	"strings"
	"strconv"
	"time"
)

var udpConn *net.UDPConn = nil
var prefix string
var timingsEnabled bool = false

func SetEndpoint(endpoint string) error {
	addr, err := net.ResolveUDPAddr("udp", endpoint)
	if err != nil {
		return err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	// replace any dots in the hostname with dashes
	hostname = strings.Replace(hostname, ".", "-", -1)

	conn, err := net.DialUDP("udp", nil, addr);
	if err != nil {
		return err
	}

	udpConn = conn
	prefix = fmt.Sprintf("rmux.%s.", hostname)
	return nil
}

func EnableTimings() {
	timingsEnabled = true
}

func Increment(metric string) {
	if Enabled() {
		sd := prefix + metric + ":1|c"
		udpConn.Write([]byte(sd))
	}
}

func Gauge(metric string, value int) {
	if Enabled() {
		sd := prefix + metric + ":" + strconv.Itoa(value) + "|g"
		udpConn.Write([]byte(sd))
	}
}

func Timing(metric string, value time.Duration) {
	if Enabled() && timingsEnabled {
		sd := fmt.Sprintf("%s%s:%.4f|ms", prefix, metric, float64(value)/float64(time.Millisecond))
		udpConn.Write([]byte(sd))
	}
}

func Enabled() bool {
	return udpConn != nil
}

// Todo: Maybe this should aggregate increments
