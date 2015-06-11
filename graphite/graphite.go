package graphite
import (
	"os"
	"net"
	"fmt"
	"strings"
	"strconv"
)

var udpConn *net.UDPConn = nil
var prefix string

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

func Enabled() bool {
	return udpConn != nil
}

// Todo: Maybe this should aggregate increments
