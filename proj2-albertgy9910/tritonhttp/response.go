package tritonhttp

import (
	"fmt"
	"io/ioutil"
	"net"
	"sort"
)

type Response struct {
	Proto      string // e.g. "HTTP/1.1"
	StatusCode int    // e.g. 200
	StatusText string // e.g. "OK"

	// Headers stores all headers to write to the response.
	Headers map[string]string

	// Request is the valid request that leads to this response.
	// It could be nil for responses not resulting from a valid request.
	// Hint: you might need this to handle the "Connection: Close" requirement
	Request *Request

	// FilePath is the local path to the file to serve.
	// It could be "", which means there is no file to serve.
	FilePath string
}

const (
	responseProto = "HTTP/1.1"

	statusOK               = 200
	statusMethodNotAllowed = 400
	statusNotFound         = 404
)

var statusText = map[int]string{
	statusOK:               "OK",
	statusMethodNotAllowed: "Bad Request",
	statusNotFound:         "Not Found",
}

func (res *Response) WriteResponse(conn net.Conn) {
	statusLine := fmt.Sprintf("%v %v %v\r\n", res.Proto, res.StatusCode, statusText[res.StatusCode])
	_, err := conn.Write([]byte(statusLine))
	checkErr(err)
	keys := make([]string, len(res.Headers))
	n := 0
	for k := range res.Headers {
		keys[n] = k
		n++
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, err := conn.Write([]byte(k + ": " + res.Headers[k] + "\r\n"))
		checkErr(err)
	}
	if _, err := conn.Write([]byte("\r\n")); err != nil {
		panic(err)
	}

	if res.StatusCode == statusOK {
		fi, err := ioutil.ReadFile(res.FilePath)
		checkErr(err)
		_, err = conn.Write(fi)
		checkErr(err)
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
