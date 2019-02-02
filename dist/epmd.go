package dist

import (
	"encoding/binary"
	"fmt"
	"github.com/halturin/ergonode/lib"
	"net"
	"strings"
)

const (
	EPMD_ALIVE2_REQ  = 120
	EPMD_ALIVE2_RESP = 121

	EPMD_PORT_PLEASE2_REQ = 122
	EPMD_PORT2_RESP       = 119

	EPMD_NAMES_REQ = 110 // $n

	EPMD_DUMP_REQ = 100 // $d
	EPMD_KILL_REQ = 107 // $k
	EPMD_STOP_REQ = 115 // $s
)

type EPMD struct {
	FullName string
	Name     string
	Domain   string

	// Listening port for incoming connections
	Port uint16

	// http://erlang.org/doc/reference_manual/distributed.html (section 13.5)
	// // 77 — regular public node, 72 — hidden
	Type uint8

	Protocol uint8
	HighVsn  uint16
	LowVsn   uint16
	Extra    []byte
	Creation uint16

	response chan interface{}
	out      chan []byte
}

func (e *EPMD) Init(name string, port uint16) {
	if !e.server() {
		e.client(name, port)
	}
}

func (e *EPMD) client(name string, port uint16) {

	ns := strings.Split(name, "@")
	// TODO: add fqdn support

	e.FullName = name
	e.Name = ns[0]
	e.Domain = ns[1]
	e.Port = port
	e.Type = 77 // or 72 if hidden
	e.Protocol = 0
	e.HighVsn = 5
	e.LowVsn = 5
	e.Creation = 0

	conn, err := net.Dial("tcp", "127.0.0.1:4369")
	if err != nil {
		panic(err.Error())
	}

	in := make(chan []byte)
	go epmdREADER(conn, in)
	e.out = make(chan []byte)
	go epmdWRITER(conn, in, e.out)

	e.response = make(chan interface{})

	//epmd handler loop
	go func() {
		defer conn.Close()
		for {
			select {
			case reply := <-in:
				lib.Log("From EPMD: %v", reply)

				if len(reply) < 1 {
					continue
				}

				switch reply[0] {
				case EPMD_ALIVE2_RESP:
					e.response <- read_ALIVE2_RESP(reply)
				}
			}
		}
	}()

	e.register()

}

func (e *EPMD) server() bool {

	return false
}

func (e *EPMD) register() {

	e.out <- compose_ALIVE2_REQ(e)
	creation := <-e.response

	switch creation {
	case false:
		panic(fmt.Sprintf("Duplicate name '%s'", e.Name))
	default:
		e.Creation = creation.(uint16)
	}
}

func (e *EPMD) ResolvePort(name string) int {
	var err error
	ns := strings.Split(name, "@")

	conn, err := net.Dial("tcp", net.JoinHostPort(ns[1], "4369"))
	if err != nil {
		return -1
	}

	defer conn.Close()

	data := compose_PORT_PLEASE2_REQ(ns[0])
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(data)))
	buf = append(buf, data...)
	_, err = conn.Write(buf)
	if err != nil {
		return -1
	}

	buf = make([]byte, 1024)
	_, err = conn.Read(buf)
	if err != nil {
		return -1
	}

	value := read_PORT2_RESP(buf)

	return int(value)
}

func epmdREADER(conn net.Conn, in chan []byte) {
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			in <- buf[0:n]
			in <- []byte{}
			return
		}
		lib.Log("Read from EPMD %d: %v", n, buf[:n])
		in <- buf[:n]
	}
}

func epmdWRITER(conn net.Conn, in chan []byte, out chan []byte) {
	for {
		select {
		case data := <-out:
			buf := make([]byte, 2)
			binary.BigEndian.PutUint16(buf[0:2], uint16(len(data)))
			buf = append(buf, data...)
			_, err := conn.Write(buf)
			if err != nil {
				in <- []byte{}
			}
		}
	}
}

func compose_ALIVE2_REQ(e *EPMD) (reply []byte) {
	reply = make([]byte, 14+len(e.Name)+len(e.Extra))
	reply[0] = byte(EPMD_ALIVE2_REQ)
	binary.BigEndian.PutUint16(reply[1:3], e.Port)
	reply[3] = e.Type
	reply[4] = e.Protocol
	binary.BigEndian.PutUint16(reply[5:7], e.HighVsn)
	binary.BigEndian.PutUint16(reply[7:9], e.LowVsn)
	nLen := len(e.Name)
	binary.BigEndian.PutUint16(reply[9:11], uint16(nLen))
	offset := (11 + nLen)
	copy(reply[11:offset], e.Name)
	nELen := len(e.Extra)
	binary.BigEndian.PutUint16(reply[offset:offset+2], uint16(nELen))
	copy(reply[offset+2:offset+2+nELen], e.Extra)
	return
}

func read_ALIVE2_RESP(reply []byte) interface{} {
	if reply[1] == 0 {
		return binary.BigEndian.Uint16(reply[2:4])
	}
	return false
}

func compose_PORT_PLEASE2_REQ(name string) (buf []byte) {
	buflen := uint16(len(name) + 1)
	buf = make([]byte, buflen)
	buf[0] = byte(EPMD_PORT_PLEASE2_REQ)
	copy(buf[1:buflen], name)
	return
}

func read_PORT2_RESP(reply []byte) (portno int) {
	if reply[0] == 119 && reply[1] == 0 {
		p := binary.BigEndian.Uint16(reply[2:4])
		portno = int(p)
	} else {
		portno = -1
	}
	return
}

// func epmd(host string, port int) bool {
// 	epmd, err := net.Listen("tcp", net.JoinHostPort(host, port))
// 	if err != nil {
// 		lib.Log("EPMD starting failed %s", err)
// 		return false
// 	}

// 	go func() {
// 		for {
// 			c, err := epmd.AcceptTCP()
// 			if err != nil {
// 				lib.Log(err.Error())
// 				continue
// 			}

// 			c.SetKeepAlive(true)
// 			c.SetKeepAlivePeriod(15 * time.Second)
// 			c.SetNoDelay(true)

// 			lib.Log("EPMD accepted new connection from %s", c.RemoteAddr().String())

// 			epmdconn := EPMDConn{
// 				conn: c,
// 			}
// 			epmdconn.run(c)

// 		}
// 	}()
// }

// func (e *EPMDConn) run() {
// 	go func() {
// 		for {
// 			terms := <-wchan
// 			err := currNd.WriteMessage(c, terms)
// 			if err != nil {
// 				lib.Log("Enode error (writing): %s", err.Error())
// 				break
// 			}
// 		}
// 		c.Close()
// 		n.lock.Lock()
// 		n.handle_monitors_node(currNd.GetRemoteName())
// 		delete(n.connections, currNd.GetRemoteName())
// 		n.lock.Unlock()
// 	}()

// 	go func() {
// 		for {
// 			terms, err := currNd.ReadMessage(c)
// 			if err != nil {
// 				lib.Log("Enode error (reading): %s", err.Error())
// 				break
// 			}
// 			n.handleTerms(c, wchan, terms)
// 		}
// 		c.Close()
// 		n.lock.Lock()
// 		n.handle_monitors_node(currNd.GetRemoteName())
// 		delete(n.connections, currNd.GetRemoteName())
// 		n.lock.Unlock()
// 	}()
// }
