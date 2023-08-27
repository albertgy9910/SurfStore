package tritonhttp

import (
	"bufio"
	"io"
	"log"
	"mime"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"

	// VirtualHosts contains a mapping from host name to the docRoot path
	// (i.e. the path to the directory to serve static files from) for
	// all virtual hosts that this server supports
	VirtualHosts map[string]string
}

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.
func (s *Server) ListenAndServe() error {
	for _, docRoot := range s.VirtualHosts {
		fi, err := os.Stat(docRoot)
		if os.IsNotExist(err) {
			return err
		} else if !fi.IsDir() {
			return err
		}
	}

	ln, err := net.Listen("tcp", "127.0.0.1"+s.Addr)
	if err != nil {
		return err
	}

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.HandleConnection(conn)
	}
}

// HandleConnection reads requests from the accepted conn and handles them.
func (s *Server) HandleConnection(conn net.Conn) {
	defer conn.Close()
	var left *string
	left = new(string)
	*left = ""
	br := bufio.NewReader(conn)
	for {
		// Set a read timeout for 5 sec
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		// Read request from the client
		log.Print("Read request from the client")
		reqStr, err := ReadRequest(br, left)
		request := new(Request)

		// Handle errors
		log.Println(err)

		if err != nil {
			// net.Error
			if err, ok := err.(net.Error); ok && err.Timeout() {
				log.Printf("Connection %v: timed out", conn)
				if *left != "" {
					sign := false
					res := s.HandleRequest(request, sign)
					res.WriteResponse(conn)
				}
				break
			}
		} else if err == io.EOF {
			// io.EOF error
			sign := false
			res := s.HandleRequest(request, sign)
			res.WriteResponse(conn)
			break
		}
		sign := true
		request.URL, sign = RequestPreProcess(reqStr, sign)
		if request.URL[0] != '/' {
			sign = false
		} else if len(request.URL) == 0 {
			sign = false
		} else {
			// Determine file path
			if strings.HasSuffix(request.URL, "/") {
				request.URL += "index.html"
			}
			reqStr = reqStr[(strings.Index(reqStr, "\r\n") + 2):]
			for {
				// parses "Key: value" into "Key" and "value".
				key2val := reqStr[:strings.Index(reqStr, "\r\n")]
				reqStr = reqStr[(strings.Index(reqStr, "\r\n") + 2):]
				if key2val == "" {
					break
				} else if !strings.Contains(key2val, ":") {
					sign = false
					break
				} else {
					key := TransformCanonical(key2val)
					if sign = check(key); !sign {
						break
					}
					val := key2val[(strings.Index(key2val, ":") + 1):]
					val = strings.TrimLeft(val, " ")

					if string(key) == "Connection" && val == "close" {
						request.Close = true
					}
					if string(key) == "Host" {
						request.Host = val
					}
				}
			}
			if len(request.Host) == 0 {
				sign = false
			}
		}
		res := s.HandleRequest(request, sign)
		res.WriteResponse(conn)
		if res.Headers["Connection"] == "close" {
			conn.Close()
			break
		}
	}
}

func (s *Server) HandleRequest(req *Request, sign bool) (res *Response) {
	res = new(Response)
	res.Proto = responseProto
	res.Headers = map[string]string{
		"Date": FormatTime(time.Now()),
	}
	if !sign {
		// 400 Bad Request response
		res.StatusCode = statusMethodNotAllowed
		res.Headers["Connection"] = "close"
	} else {
		if req.Close {
			res.Headers["Connection"] = "close"
		}
		url := req.URL
		docRoot := s.VirtualHosts[req.Host]
		path := filepath.Join(docRoot, url)
		pathCle := filepath.Clean(path)
		fi, err := os.Stat(pathCle)
		if err != nil || fi.IsDir() {
			// 404 Not Found response
			res.StatusCode = statusNotFound
		} else {
			// 200 OK response
			res.StatusCode = statusOK
			res.FileServer(pathCle)
		}
	}
	return res
}

func RequestPreProcess(reqStr string, sign bool) (string, bool) {
	reqtemp := reqStr[:strings.Index(reqStr, "\r\n")]
	if !strings.HasPrefix(reqtemp, "GET") {
		sign = false
	} else if !strings.HasSuffix(reqtemp, "HTTP/1.1") {
		sign = false
	} else {
		reqtemp = strings.TrimPrefix(reqtemp, "GET")
		reqtemp = strings.TrimSuffix(reqtemp, "HTTP/1.1")
		reqtemp = strings.TrimSpace(reqtemp)
	}
	return reqtemp, sign
}

func TransformCanonical(key2val string) string {
	tempK := key2val[:strings.Index(key2val, ":")]
	// Transform header key to the canonical form
	key := CanonicalHeaderKey(tempK)
	return key
}

func check(key string) bool {
	for _, r := range key {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-') {
			return false
		}
	}
	return true
}

func (res *Response) FileServer(fileP string) error {
	fi, _ := os.Stat(fileP)

	res.Headers["Last-Modified"] = FormatTime(fi.ModTime())
	res.Headers["Content-Type"] = mime.TypeByExtension(filepath.Ext(fileP))
	res.Headers["Content-Length"] = strconv.FormatInt(fi.Size(), 10)
	res.FilePath = fileP

	return nil
}
