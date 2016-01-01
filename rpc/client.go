package rpc

import (
	"crypto/rand"
	"encoding/binary"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	rpcp "github.com/tmthrgd/Ne/rpc/proto"
	"golang.org/x/net/context"
)

type result struct {
	buf *proto.Buffer

	res *rpcp.ResponseHeader
}

type Client struct {
	conn net.Conn

	id uint64

	mu      sync.RWMutex
	results map[uint64]chan<- result
}

func Dial(network, address string) (*Client, error) {
	conn, err := net.Dial(network, address)

	if err != nil {
		return nil, err
	}

	return NewClient(conn)
}

func NewClient(conn net.Conn) (*Client, error) {
	var id [8]byte

	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}

	c := &Client{
		conn: conn,

		id: binary.LittleEndian.Uint64(id[:]),

		results: make(map[uint64]chan<- result),
	}

	go c.reader()
	return c, nil
}

func (c *Client) reader() {
	for {
		buf := protoBufferPool.Get().(*proto.Buffer)

		rb := buf.Bytes()
		n, err := c.conn.Read(rb[:cap(rb)])
		buf.SetBuf(rb[:n])

		if err != nil {
			protoBufferPool.Put(buf)

			log.Println(err)
			continue
		}

		var res rpcp.ResponseHeader
		if err = buf.DecodeMessage(&res); err != nil {
			protoBufferPool.Put(buf)

			log.Println(err)
			continue
		}

		c.mu.RLock()
		waiter, ok := c.results[res.Id]

		if !ok {
			c.mu.RUnlock()

			protoBufferPool.Put(buf)

			log.Println("invalid id")
			continue
		}

		waiter <- result{
			buf,
			&res,
		}

		c.mu.RUnlock()
	}
}

func (c *Client) sendRequest(service, method string, in interface{}) (uint64, chan result, error) {
	wbuf := protoBufferPool.Get().(*proto.Buffer)
	defer protoBufferPool.Put(wbuf)
	wbuf.Reset()

	id := atomic.AddUint64(&c.id, 1)

	if err := wbuf.EncodeMessage(&rpcp.RequestHeader{
		Id:      id,
		Service: service,
		Method:  method,
	}); err != nil {
		return 0, nil, err
	}

	if err := wbuf.Marshal(in.(proto.Message)); err != nil {
		return 0, nil, err
	}

	waiter := make(chan result, 1)

	c.mu.Lock()
	c.results[id] = waiter
	c.mu.Unlock()

	_, err := c.conn.Write(wbuf.Bytes())
	return id, waiter, err
}

func (c *Client) Invoke(ctx context.Context, service, method string, in, out interface{}) error {
	id, waiter, err := c.sendRequest(service, method, in)

	if waiter != nil {
		defer func() {
			c.mu.Lock()
			delete(c.results, id)
			c.mu.Unlock()

			close(waiter)
		}()
	}

	if err != nil {
		return err
	}

	select {
	case r := <-waiter:
		defer protoBufferPool.Put(r.buf)

		if len(r.res.Error) != 0 {
			return RemoteError(r.res.Error)
		}

		return r.buf.Unmarshal(out.(proto.Message))
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) InvokeStream(ctx context.Context, service, method string, in interface{}, newOut func() interface{}, recvdOut func(interface{}), closed func()) error {
	id, waiter, err := c.sendRequest(service, method, in)

	if err != nil {
		return err
	}

	go func() {
		defer func() {
			c.mu.Lock()
			delete(c.results, id)
			c.mu.Unlock()

			close(waiter)

			closed()
		}()

		for {
			select {
			case r := <-waiter:
				defer protoBufferPool.Put(r.buf)

				if len(r.res.Error) != 0 {
					log.Println(r.res.Error)
					continue
				}

				if r.res.Close {
					return
				}

				out := newOut()

				if err := r.buf.Unmarshal(out.(proto.Message)); err != nil {
					log.Println(err)
					continue
				}

				recvdOut(out)
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					log.Println(err)
				}

				return
			}
		}
	}()

	return nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type RemoteError string

func (e RemoteError) Error() string {
	return string(e)
}
