package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func check(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s", err.Error())
	}
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	_ = yaml.Unmarshal(f, &scs)

	return scs
}

func handleClientConnection(conn net.Conn, ch chan<- [][]byte) {
	for {
		record := make([]byte, 101)
		counter := 0
		for {
			read, err := conn.Read(record[counter:])
			if err == io.EOF {
			} else if err != io.EOF && err != nil {
				fmt.Fprintf(os.Stderr, "Error:  %v\n", err)
			}
			counter += read
			if counter == 101 {
				break
			}
		}
		var msg [][]byte
		msg = append(msg, record)
		ch <- msg
		if record[0] == 1 {
			conn.Close()
			return
		}
	}
}

func DataListener(host string, port string, ch chan<- [][]byte) {
	listener, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		fmt.Println("Listener Error: ", err.Error())
		os.Exit(1)
	}
	defer listener.Close()

	// call func before to handle the connection
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection Error: ", err.Error())
			return
		}
		go handleClientConnection(conn, ch)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	// via channel instead of explicit locks and find correct server
	ch := make(chan [][]byte)
	defer close(ch)

	for _, server := range scs.Servers {
		if serverId == server.ServerId {
			go DataListener(server.Host, server.Port, ch)
		}
	}

	// To build mesh
	time.Sleep(100 * time.Millisecond)

	// Read func refer project 0
	inputFile := os.Args[2]
	outputFile := os.Args[3]
	readFile, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Input Error: %v", err)
	}
	n := int(math.Log2(float64(len(scs.Servers))))
	conns := make([]net.Conn, len(scs.Servers))
	for key, server := range scs.Servers {
		for i := 0; i < 8; i++ {
			conns[key], err = net.Dial("tcp", server.Host+":"+server.Port)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				if i == 8 {
					panic(err)
				}
			} else {
				break
			}
		}
	}

	for {
		buffer := make([]byte, 100)
		_, err := readFile.Read(buffer)
		if err != nil {
			if err == io.EOF {
				readFile.Close()
				break
			}
			panic(err)
		}
		rServerId := int(buffer[0] >> (8 - n))
		for key, server := range scs.Servers {
			if rServerId == server.ServerId {
				buffer = append([]byte{0}, buffer...)
				_, err := conns[key].Write(buffer)
				check(err)
				break
			}
		}
	}
	for key := range scs.Servers {
		record := []byte{1}
		for i := 0; i < 100; i++ {
			record = append(record, 0)
		}
		_, err := conns[key].Write(record)
		check(err)
	}

	// read records
	var recArray [][]byte
	count := 0
	for {
		if count == len(scs.Servers) {
			break
		}
		recs := <-ch
		for _, rec := range recs {
			if rec[0] == 1 {
				count++
			} else {
				recArray = append(recArray, rec[1:])
			}
		}
	}

	sort.Slice(recArray, func(i, j int) bool { return string(recArray[i][:10]) < string(recArray[j][:10]) })

	writeFile, err := os.Create(outputFile)
	check(err)
	defer writeFile.Close()

	for _, record := range recArray {
		writeFile.Write(record)
	}

	err = writeFile.Close()
	check(err)
}
