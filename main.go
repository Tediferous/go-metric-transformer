package main

import (
	"fmt"
	"strings"
	"strconv"
	"net"
	"os"
)

type metric struct (
	measurement string
	tags map[string]string
	values map[string]string
	time string 
)

func CheckError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
}

func FluxParse (s string) (m metric){
//Takes metrics in line protocol and parses them out to a struct
	var doOnce bool = true
	split := strings.Split(s, " ")
	
	for _, element := range strings.Split(split[0], ","){
		if doOnce {
			m.measurement = element
			doOnce = false
		} else{
			m.tags[strings.Split(element,"=")[0]] =  strings.Split(element,"=")[1] 
		}
	}

	for _, element := range strings.Split(splt[1],","){
		m.values[strings.Split(element,"=")[0]] =  strings.Split(element,"=")[1] 
	}
	
	m.time = split[2]

	return m
}

func ToWave(m metric) (w []string){
	for k, v := range m.values {
		var line string = m.measurement+"."+k
		line += " " + v
		line += " " + m.time
		line += " source=" + WHATEVER?

		for ke, va := range m.tags {
			line +=  ke +"=\"" + va +"\""
		}

		w = append(w, line)
	}
	return w
}

func main() {
	/* Lets prepare a address at any address at port 10001*/
	ServerAddr, err := net.ResolveUDPAddr("udp", ":1996")
	CheckError(err)
	fmt.Println("listening on :1996")

	/* Now listen at selected port */
	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	CheckError(err)
	defer ServerConn.Close()

	buf := make([]byte, 1024)

	for {
		n, addr, err := ServerConn.ReadFromUDP(buf)
		fmt.Printf("received: %s from: %s\n", string(buf[0:n]), addr)

		if err != nil {
			fmt.Println("error: ", err)
		}

		ServerConn.WriteTo(buf[0:n], addr)
	}
}
