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

package log

import (
	"fmt"
	"log/syslog"
	"runtime/debug"
)

const (
	// From /usr/include/sys/syslog.h.
	// These are the same on Linux, BSD, and OS X.
	LOG_EMERG = iota
	LOG_ALERT
	LOG_CRIT
	LOG_ERR
	LOG_WARNING
	LOG_NOTICE
	LOG_INFO
	LOG_DEBUG
)

var slw *syslog.Writer
var _enableSyslog = true
var _level = LOG_INFO

func SetLogLevel(level int) {
	_level = level
}

func UseSyslog(useSyslog bool)  {
	_enableSyslog = useSyslog
	if useSyslog {
		var e error
		slw, e = syslog.New(syslog.LOG_INFO, "rmux")
		if e != nil {
			fmt.Printf("Error initializing syslog: %s\r\n", e)
		}
	}
}

func Info(format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)

	if _enableSyslog {
		slw.Info(out)
	}
	if LOG_INFO <= _level {
		fmt.Println(out)
	}
}

func Debug(format string, a ...interface{}) {
	if LOG_DEBUG <= _level {
		out := fmt.Sprintf(format, a...)

		if _enableSyslog {
			slw.Info(out)
		}
		fmt.Println(out)
	}
}

func Error(format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)

	if _enableSyslog {
		slw.Err(out)
	}
	if LOG_ERR <= _level {
		fmt.Println(out)
	}
}

func LogPanic(r interface{}) {
	Error("Panic: %s\r\nStack: %s\r\n", r, debug.Stack())
}

func Warn(format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)

	if _enableSyslog {
		slw.Warning(out)
	}
	if LOG_WARNING <= _level {
		fmt.Println(out)
	}
}
