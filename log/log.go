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
var _level = LOG_INFO

func init() {
	var e error
	slw, e = syslog.New(syslog.LOG_INFO, "rmux")
	if e != nil {
		fmt.Printf("Error initializing syslog: %s\r\n", e)
	}
}

func SetLogLevel(level int) {
	_level = level
}

func Info(format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)

	slw.Info(out)
	if LOG_INFO <= _level {
		fmt.Println(out)
	}
}

func Debug(format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)

	slw.Debug(out)
	if LOG_DEBUG <= _level {
		fmt.Println(out)
	}
}

func Error(format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)

	slw.Err(out)
	if LOG_ERR <= _level {
		fmt.Println(out)
	}
}

func DebugPanic(r interface{}) {
	Error("Panic: %s\r\nStack: %s\r\n", r, debug.Stack())
}

func Warn(format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)

	slw.Warning(out)
	if LOG_WARNING <= _level {
		fmt.Println(out)
	}
}
