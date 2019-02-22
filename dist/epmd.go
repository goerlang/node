package dist

import (
	"encoding/binary"
	"fmt"
	"github.com/halturin/ergonode/lib"
	"net"
	"strconv"
	"strings"
	"time"
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
}

func (e *EPMD) Init(name string, listenport uint16, epmdport uint16, hidden bool) {
	// trying to start embedded EPMD
	server(epmdport)

	ns := strings.Split(name, "@")
	if len(ns) != 2 {
		panic("FQDN for node name is required (example: node@hostname)")
	}

	e.FullName = name
	e.Name = ns[0]
	e.Domain = ns[1]
	e.Port = listenport

	if hidden {
		e.Type = 72
	} else {
		e.Type = 77
	}

	e.Protocol = 0
	e.HighVsn = 5
	e.LowVsn = 5
	e.Creation = 0
	dsn := net.JoinHostPort("", strconv.Itoa(int(epmdport)))
	conn, err := net.Dial("tcp", dsn)
	if err != nil {
		panic(err.Error())
	}

	in := make(chan []byte)
	go epmdREADER(conn, in)
	out := make(chan []byte)
	go epmdWRITER(conn, in, out)

	e.response = make(chan interface{})

	//epmd client handler loop
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

	e.register(out)

}

func (e *EPMD) register(out chan []byte) {

	out <- compose_ALIVE2_REQ(e)
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
			lib.Log("111112")

			return
		}
		lib.Log("EPMD reader. Read %d: %v", n, buf[:n])
		in <- buf[:n]
	}
}

func epmdWRITER(conn net.Conn, in chan []byte, out chan []byte) {
	for {
		select {
		case data := <-out:
			_, err := conn.Write(data)
			if err != nil {
				lib.Log("11111")
				in <- []byte{}
			}
			lib.Log("EPMD writer. Write: %v", data)

		}
	}
}

func compose_ALIVE2_REQ(e *EPMD) (reply []byte) {
	reply = make([]byte, 2+14+len(e.Name)+len(e.Extra))
	binary.BigEndian.PutUint16(reply[0:2], uint16(len(reply)-2))
	reply[2] = byte(EPMD_ALIVE2_REQ)
	binary.BigEndian.PutUint16(reply[3:5], e.Port)
	reply[5] = e.Type
	reply[6] = e.Protocol
	binary.BigEndian.PutUint16(reply[7:9], e.HighVsn)
	binary.BigEndian.PutUint16(reply[9:11], e.LowVsn)
	nLen := len(e.Name)
	binary.BigEndian.PutUint16(reply[11:13], uint16(nLen))
	offset := (13 + nLen)
	copy(reply[13:offset], e.Name)
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

func compose_PORT_PLEASE2_REQ(name string) (reply []byte) {
	replylen := uint16(2 + len(name) + 1)
	reply = make([]byte, replylen)
	binary.BigEndian.PutUint16(reply[0:2], uint16(len(reply)-2))
	reply[2] = byte(EPMD_PORT_PLEASE2_REQ)
	copy(reply[3:replylen], name)
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

var epmdmap map[string]uint16

func server(port uint16) {

	if epmdmap != nil {
		// already started
		return
	}

	epmd, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(int(port))))
	if err != nil {
		lib.Log("Can't start embedded EPMD service: %s", err)
		return
	}

	epmdmap = make(map[string]uint16)

	lib.Log("Started embedded EMPD service and listen port: %d", port)

	go func() {
		for {
			c, err := epmd.Accept()
			if err != nil {
				lib.Log(err.Error())
				continue
			}

			if tcp, ok := c.(*net.TCPConn); !ok {
				tcp.SetKeepAlive(true)
				tcp.SetKeepAlivePeriod(15 * time.Second)
				tcp.SetNoDelay(true)
			}

			lib.Log("EPMD accepted new connection from %s", c.RemoteAddr().String())

			//epmd connection handler loop
			go func() {
				in := make(chan []byte)
				go epmdREADER(c, in)
				out := make(chan []byte)
				go epmdWRITER(c, in, out)

				defer c.Close()
				for {
					select {
					case req := <-in:
						lib.Log("Request from EPMD client: %v", req)
						// req[0:1] - length
						switch req[2] {
						case EPMD_ALIVE2_REQ:
							out <- compose_ALIVE2_RESP(req[2:])
						default:
							lib.Log("unknown EPMD request")
						}
					}
				}
			}()

		}
	}()
}

func compose_ALIVE2_RESP(req []byte) []byte {
	reply := make([]byte, 4)
	reply[0] = EPMD_ALIVE2_RESP
	reply[1] = 1
	binary.BigEndian.PutUint16(reply[2:], uint16(99))
	lib.Log("Made reply for ALIVE2_REQ: %#v", reply)
	return reply
}
