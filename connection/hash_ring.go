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

package connection

import (
	"errors"
//	. "github.com/forcedotcom/rmux/log"
	"github.com/forcedotcom/rmux/protocol"
)

//An outbound connection to a redis server
//Maintains its own underlying TimedNetReadWriter, and keeps track of its DatabaseId for select() changes
type HashRing struct {
	//The connection pools that we will be hashing our connections to
	ConnectionPools []*ConnectionPool
	//The bitmask to use for all hashed queries
	BitMask uint32
	//The default connection pool
	DefaultConnectionPool *ConnectionPool
}

func NewHashRing(connectionPools []*ConnectionPool) (newHashRing *HashRing, err error) {
	newHashRing = &HashRing{}
	//The goal here is to have an even distribution of connection pools for a hash,
	//AND ensuring that the distribution stays balanced when a pool goes down
	//start out by rounding up to the nearest prime p
	poolLength := len(connectionPools)
	prime, err := newHashRing.getNextPrime(poolLength)
	if err != nil {
		return
	}
//	Debug("Making a hash ring for prime %v", prime)
	newHashRing.setBitMask(prime)
	newHashRing.ConnectionPools = make([]*ConnectionPool, newHashRing.BitMask+1)
//	Debug("Made a set of connection pools of size %v", len(newHashRing.ConnectionPools))

	newHashRing.distributeConnectionPools(prime, connectionPools)
	return
}

func (myHashRing *HashRing) distributeConnectionPools(prime int, connectionPools []*ConnectionPool) {
	lastTarget := 0
	for multiplier := 1; multiplier < prime; multiplier++ {
		for value := 0; value < prime; value++ {
			if multiplier*value%prime < len(connectionPools) {
				lastTarget = multiplier * value % prime
			}
			myHashRing.ConnectionPools[(multiplier-1)*prime+value] = connectionPools[lastTarget]
		}
	}

	copy(myHashRing.ConnectionPools[prime*(prime-1):], myHashRing.ConnectionPools)
	if len(myHashRing.ConnectionPools) > 0 {
		myHashRing.DefaultConnectionPool = myHashRing.ConnectionPools[0]
	}
}

func (myHashRing *HashRing) getNextPrime(poolLength int) (int, error) {
	if poolLength == 0 {
		return -1, errors.New("At least one connection pool is required")
	}
	primes := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101}
	for _, curPrime := range primes {
		if poolLength <= curPrime {
			return curPrime, nil
		}
	}
	return -1, errors.New("Prime list isn't big enough")
}

func (myHashRing *HashRing) setBitMask(prime int) {
	myHashRing.BitMask = 1
	var minimumSize uint32 = uint32(prime * (prime - 1))
	for minimumSize > myHashRing.BitMask {
		myHashRing.BitMask = myHashRing.BitMask << 1
	}
	myHashRing.BitMask = myHashRing.BitMask - 1
}

//Gets the connectionKey, for a to-be-multiplexed command
//Uses the bernstein hash, which is one of the fastest key-distribution algorithms out there
//Also calculates a failover connectionKey, incase the primary is down
func (myHashRing *HashRing) GetConnectionPool(command protocol.Command) (connectionPool *ConnectionPool) {
	var hash uint32 = 0
	if command.GetArgCount() > 0 {
		//The bernstein hash is one of the faster key-distribution algorithms out there, for small character keys
		//An alternate (but slower) algorithm would be to use go's built-in hash/fnv, if this proves insufficient
		for _, char := range command.GetFirstArg() {
			hash = hash<<5 + hash + uint32(char)
		}
	}

	hash = myHashRing.BitMask & hash
	connectionPool = myHashRing.ConnectionPools[hash]
	for !connectionPool.IsConnected {
		if hash == myHashRing.BitMask {
			hash = 0
		} else {
			hash = hash + 1
		}
		connectionPool = myHashRing.ConnectionPools[hash]
	}
	return
}
