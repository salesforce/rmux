package protocol

import (
	"bufio"
	"bytes"
	"testing"
)

func TestScanResp(t *testing.T) {
	testData := []struct {
		inBytes string
		outResp []string
	}{
		{"-Error statement\r\n-Another\r\n", []string{"-Error statement\r\n", "-Another\r\n"}},
		{"Inline Command\r\nSecond Inline\r\n", []string{"Inline Command\r\n", "Second Inline\r\n"}},
		{"+OK\r\n+PONG\r\n", []string{"+OK\r\n", "+PONG\r\n"}},
		{":5\r\n:1\r\n", []string{":5\r\n", ":1\r\n"}},
		{"$5\r\nbulks\r\n$4\r\nbulk\r\n", []string{"$5\r\nbulks\r\n", "$4\r\nbulk\r\n"}},
		{
			"*2\r\n-Error Thing\r\n+OK\r\n*5\r\n$4\r\nping\r\n$3\r\nget\r\n$2\r\nok\r\n:5\r\n+ok\r\n",
			[]string{
				"*2\r\n-Error Thing\r\n+OK\r\n",
				"*5\r\n$4\r\nping\r\n$3\r\nget\r\n$2\r\nok\r\n:5\r\n+ok\r\n",
			},
		},
		{
			"*2\r\n*2\r\n+OK\r\n+PING\r\n*2\r\n$6\r\nSELECT\r\n:5\r\n+Test\r\n",
			[]string{
				"*2\r\n*2\r\n+OK\r\n+PING\r\n*2\r\n$6\r\nSELECT\r\n:5\r\n",
				"+Test\r\n",
			},
		},
		{"$-1\r\n$-1\r\n", []string{"$-1\r\n", "$-1\r\n"}},
		{"*2\r\n$-1\r\n$-1\r\n", []string{"*2\r\n$-1\r\n$-1\r\n"}},

		// Check for panic case in testing
		{"$", []string{}},
/*
 2a 32 0d 0a                                      *2..
 24 33 0d 0a                                      $3..
 47 45 54 0d 0a                                   GET..
 24 31 34 0d 0a                                   $14..
 6d 6f 6e 69 74 6f 72 5f 6d 61 73 74 65 72 0d 0a  monitor_master..
  */

		{
			"*2\r\n$3\r\nGET\r\n$14\r\nmonitor_master\r\n*3\r\n$7\r\nEVALSHA\r\n$40\r\n29e86375a0dc24139361139c9e8853cb34aa16a6\r\n$1\r\n0\r\nasdf",
			[]string{
				"*2\r\n$3\r\nGET\r\n$14\r\nmonitor_master\r\n",
				"*3\r\n$7\r\nEVALSHA\r\n$40\r\n29e86375a0dc24139361139c9e8853cb34aa16a6\r\n$1\r\n0\r\n",
			},
		},
	}

	for _, d := range testData {
		s := NewRespScanner(getReader(d.inBytes))

		scanned := [][]byte{}
		for i := 0; s.Scan(); i++ {
			b := s.Bytes()
			scanned = append(scanned, b)

			if len(d.outResp) < i+1 {
				t.Errorf("Did not expect a %d-th response from %q", i, d.inBytes)
				continue
			}

			if bytes.Compare([]byte(d.outResp[i]), b) != 0 {
				t.Errorf("Did not scan expected resp data from %q. Expected %q, Got %q", d.inBytes, d.outResp[i], b)
			}
		}

		if len(scanned) != len(d.outResp) {
			t.Errorf("Did not receive expected number of scan results from %q. Expected %d, Got %d", d.inBytes, len(d.outResp), len(scanned))
			t.Errorf("Received results %q", scanned)
		}
	}
}

func TestScanNewline(t *testing.T) {
	testData := []struct {
		inBytes string
		outResp []string
	}{
		{"-Error statement\r\n-Another\r\n", []string{"-Error statement\r\n", "-Another\r\n"}},
		{"Test newline\nin middle\r\nOf a grouping\r\n", []string{"Test newline\nin middle\r\n", "Of a grouping\r\n"}},
	}

	for _, d := range testData {
		s := bufio.NewScanner(getReader(d.inBytes))
		s.Split(scanNewline)

		for i := 0; s.Scan(); i++ {
			b := s.Bytes()

			if len(d.outResp) < i+1 {
				t.Errorf("Did not expect a %d-th response", i)
				continue
			}

			if bytes.Compare([]byte(d.outResp[i]), b) != 0 {
				t.Errorf("Did not scan expected resp data. Expected %q, Got %q", d.outResp[i], b)
			}
		}
	}
}
