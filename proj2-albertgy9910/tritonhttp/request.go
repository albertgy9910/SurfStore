package tritonhttp

import (
	"bufio"
	"strings"
)

type Request struct {
	Method string // e.g. "GET"
	URL    string // e.g. "/path/to/a/file"
	Proto  string // e.g. "HTTP/1.1"

	// Headers stores the key-value HTTP headers
	Headers map[string]string

	Host  string // determine from the "Host" header
	Close bool   // determine from the "Connection" header
}

func ReadRequest(br *bufio.Reader, left *string) (req string, err error) {
	buf := make([]byte, 1024)
	msg := 4
	for {
		for strings.Contains(*left, "\r\n\r\n") {
			req := (*left)[:(strings.Index(*left, "\r\n\r\n") + msg)]
			*left = (*left)[(strings.Index(*left, "\r\n\r\n") + msg):]
			return req, nil
		}
		size, err := br.Read(buf)
		if err != nil {
			return "", err
		}
		*left = *left + string(buf[:size])
	}
}
