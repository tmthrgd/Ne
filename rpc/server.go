package rpc

import (
	"errors"
	"io"
	"log"
	"net"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
	rpcp "github.com/tmthrgd/Ne/rpc/proto"
)

const protoBufferCapacity = 64 * 1024

var protoBufferPool = &sync.Pool{
	New: func() interface{} {
		return proto.NewBuffer(make([]byte, 0, protoBufferCapacity))
	},
}

type MethodDesc struct {
	MethodName string
	Handler    func(srv interface{}, dec func(interface{}) error) (interface{}, error)
}

type StreamDesc struct {
	StreamName string
	Handler    func(srv interface{}, dec func(interface{}) error, send func(interface{}), closed func()) error
}

type ServiceDesc struct {
	ServiceName string
	HandlerType interface{}
	Methods     []MethodDesc
	Streams     []StreamDesc
}

type service struct {
	server interface{}
	md     map[string]*MethodDesc
	sd     map[string]*StreamDesc
}

type Server struct {
	ln   net.Listener
	conn net.PacketConn

	emu    sync.RWMutex
	errors chan error

	mu sync.RWMutex
	m  map[string]*service
}

func NewServer() *Server {
	return &Server{
		m: make(map[string]*service),
	}
}

func (s *Server) RegisterService(sd *ServiceDesc, ss interface{}) {
	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(ss)

	if !st.Implements(ht) {
		log.Fatalf("grpc: Server.RegisterService found the handler of type %v that does not satisfy %v", st, ht)
	}

	s.register(sd, ss)
}

func (s *Server) register(sd *ServiceDesc, ss interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.m[sd.ServiceName]; ok {
		log.Fatalf("grpc: Server.RegisterService found duplicate service registration for %q", sd.ServiceName)
	}

	srv := &service{
		server: ss,
		md:     make(map[string]*MethodDesc),
		sd:     make(map[string]*StreamDesc),
	}

	for i := range sd.Methods {
		d := &sd.Methods[i]
		srv.md[d.MethodName] = d
	}

	for i := range sd.Streams {
		d := &sd.Streams[i]
		srv.sd[d.StreamName] = d
	}

	s.m[sd.ServiceName] = srv
}

func (s *Server) Errors() <-chan error {
	s.emu.RLock()

	if s.errors == nil {
		s.emu.RUnlock()

		s.emu.Lock()
		defer s.emu.Unlock()

		if s.errors == nil {
			s.errors = make(chan error)
		}

		return s.errors
	}

	defer s.emu.RUnlock()
	return s.errors
}

func (s *Server) error(err error) {
	if err == nil {
		return
	}

	s.emu.RLock()
	defer s.emu.RUnlock()

	select {
	case s.errors <- err:
	default:
		go func() {
			c.emu.RLock()
			defer c.emu.RUnlock()

			s.errors <- err
		}()
	}
}

func (s *Server) ListenAndServe(network, address string) error {
	switch network {
	case "udp", "udp4", "udp6", "ip", "ip4", "ip6", "unixgram":
		conn, err := net.ListenPacket(network, address)

		if err != nil {
			return err
		}

		return s.ServePacket(conn)
	default:
		ln, err := net.Listen(network, address)

		if err != nil {
			return err
		}

		return s.Serve(ln)
	}
}

func (s *Server) Serve(ln net.Listener) error {
	if s.ln != nil || s.conn != nil {
		return errors.New("Serve or ServePacket already called")
	}

	s.ln = ln

	for {
		conn, err := ln.Accept()

		if err != nil {
			if op, ok := err.(*net.OpError); ok {
				if op.Err.Error() == "use of closed network connection" {
					return nil
				}
			}

			ln.Close()
			return err
		}

		go func(conn net.Conn) {
			defer conn.Close()

			for {
				rbuf := protoBufferPool.Get().(*proto.Buffer)

				rb := rbuf.Bytes()
				n, err := conn.Read(rb[:cap(rb)])
				rbuf.SetBuf(rb[:n])

				if err != nil {
					protoBufferPool.Put(rbuf)

					if err != io.EOF {
						s.error(err)
					}

					return
				}

				go func(rbuf *proto.Buffer) {
					defer protoBufferPool.Put(rbuf)

					s.error(s.execute(rbuf, conn.Write))
				}(rbuf)
			}
		}(conn)
	}
}

func (s *Server) ServePacket(conn net.PacketConn) error {
	if s.ln != nil || s.conn != nil {
		return errors.New("Serve or ServePacket already called")
	}

	s.conn = conn

	for {
		rbuf := protoBufferPool.Get().(*proto.Buffer)

		rb := rbuf.Bytes()
		n, addr, err := conn.ReadFrom(rb[:cap(rb)])
		rbuf.SetBuf(rb[:n])

		if err != nil {
			protoBufferPool.Put(rbuf)

			if op, ok := err.(*net.OpError); ok {
				if op.Err.Error() == "use of closed network connection" {
					return nil
				}
			}

			conn.Close()
			return err
		}

		go func(rbuf *proto.Buffer, addr net.Addr) {
			defer protoBufferPool.Put(rbuf)

			s.error(s.execute(rbuf, func(p []byte) (int, error) {
				return conn.WriteTo(p, addr)
			}))
		}(rbuf, addr)
	}
}

func (s *Server) execute(rbuf *proto.Buffer, write func(p []byte) (int, error)) error {
	var req rpcp.RequestHeader
	if err := rbuf.DecodeMessage(&req); err != nil {
		return err
	}

	s.mu.RLock()
	srv, ok := s.m[req.Service]
	s.mu.RUnlock()

	if !ok {
		return s.sendResposne(&rpcp.ResponseHeader{
			Id:    req.Id,
			Error: "service not implemented",
		}, nil, write)
	}

	dec := func(in interface{}) error {
		return rbuf.Unmarshal(in.(proto.Message))
	}

	if meth, ok := srv.md[req.Method]; ok {
		out, err := meth.Handler(srv.server, dec)

		res := &rpcp.ResponseHeader{
			Id: req.Id,
		}

		if err != nil {
			res.Error = err.Error()
		}

		return s.sendResposne(res, out, write)
	} else {
		stream, ok := srv.sd[req.Method]

		if !ok {
			return s.sendResposne(&rpcp.ResponseHeader{
				Id:    req.Id,
				Error: "method not implemented",
			}, nil, write)
		}

		err := stream.Handler(srv.server, dec, func(out interface{}) {
			res := &rpcp.ResponseHeader{
				Id: req.Id,
			}

			err := s.sendResposne(res, out, write)

			if err == nil {
				return
			}

			res.Error = err.Error()

			s.error(s.sendResposne(res, nil, write))
		}, func() {
			s.error(s.sendResposne(&rpcp.ResponseHeader{
				Id:    req.Id,
				Close: true,
			}, nil, write))
		})

		if err != nil {
			return s.sendResposne(&rpcp.ResponseHeader{
				Id:    req.Id,
				Error: err.Error(),
			}, nil, write)
		}

		return nil
	}
}

func (s *Server) sendResposne(res *rpcp.ResponseHeader, out interface{}, write func(p []byte) (int, error)) error {
	wbuf := protoBufferPool.Get().(*proto.Buffer)
	defer func() {
		if cap(wbuf.Bytes()) == protoBufferCapacity {
			protoBufferPool.Put(wbuf)
		}
	}()
	wbuf.Reset()

	if err := wbuf.EncodeMessage(res); err != nil {
		return err
	}

	if out != nil {
		if err := wbuf.Marshal(out.(proto.Message)); err != nil {
			return err
		}
	}

	_, err := write(wbuf.Bytes())
	return err
}

func (s *Server) Stop() {
	if s.ln != nil {
		s.ln.Close()
	}

	if s.conn != nil {
		s.conn.Close()
	}
}
