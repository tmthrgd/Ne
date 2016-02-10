package rpc

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"sync"

	"github.com/golang/protobuf/proto"
	rpcp "github.com/tmthrgd/Ne/rpc/proto"
	"golang.org/x/net/context"
)

type result struct {
	buf *proto.Buffer

	res *rpcp.ResponseHeader
}

type Client struct {
	conn net.Conn

	used map[uint64]struct{}

	emu    sync.RWMutex
	errors chan error

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
	c := &Client{
		conn: conn,

		used: map[uint64]struct{}{0: struct{}{}},

		results: make(map[uint64]chan<- result),
	}

	go c.reader()
	return c, nil
}

func (c *Client) Errors() <-chan error {
	c.emu.RLock()

	if c.errors == nil {
		c.emu.RUnlock()

		c.emu.Lock()
		defer c.emu.Unlock()

		if c.errors == nil {
			c.errors = make(chan error)
		}

		return c.errors
	}

	defer c.emu.RUnlock()
	return c.errors
}

func (c *Client) error(err error) {
	if err == nil {
		return
	}

	c.emu.RLock()
	defer c.emu.RUnlock()

	select {
	case c.errors <- err:
	default:
		if c.errors == nil {
			log.Println(err)
			return
		}

		go func() {
			c.emu.RLock()
			defer c.emu.RUnlock()

			c.errors <- err
		}()
	}
}

func (c *Client) reader() {
	for {
		buf := protoBufferPool.Get().(*proto.Buffer)

		rb := buf.Bytes()
		n, err := c.conn.Read(rb[:cap(rb)])
		buf.SetBuf(rb[:n])

		if err != nil {
			protoBufferPool.Put(buf)

			if op, ok := err.(*net.OpError); ok {
				if op.Err.Error() == "use of closed network connection" {
					return
				}
			}

			c.error(err)
			continue
		}

		var res rpcp.ResponseHeader
		if err = buf.DecodeMessage(&res); err != nil {
			protoBufferPool.Put(buf)

			c.error(err)
			continue
		}

		c.mu.RLock()
		waiter, ok := c.results[res.Id]

		if !ok {
			c.mu.RUnlock()

			protoBufferPool.Put(buf)

			c.error(errors.New("invalid id"))
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
	defer func() {
		if cap(wbuf.Bytes()) == protoBufferCapacity {
			protoBufferPool.Put(wbuf)
		}
	}()
	wbuf.Reset()

	req := &rpcp.RequestHeader{
		Service: service,
		Method:  method,
	}

	var id [8]byte

	for {
		if _, err := rand.Read(id[:]); err != nil {
			return 0, nil, err
		}

		req.Id = binary.LittleEndian.Uint64(id[:])

		if _, used := c.used[req.Id]; !used {
			c.used[req.Id] = struct{}{}
			break
		}
	}

	if err := wbuf.EncodeMessage(req); err != nil {
		return 0, nil, err
	}

	if err := wbuf.Marshal(in.(proto.Message)); err != nil {
		return 0, nil, err
	}

	waiter := make(chan result, 1)

	c.mu.Lock()
	c.results[req.Id] = waiter
	c.mu.Unlock()

	_, err := c.conn.Write(wbuf.Bytes())
	return req.Id, waiter, err
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

	cleanup := func() {
		c.mu.Lock()
		delete(c.results, id)
		c.mu.Unlock()

		close(waiter)

		closed()
	}

	if err != nil {
		if waiter != nil {
			cleanup()
		} else {
			closed()
		}

		return err
	}

	go func() {
		defer cleanup()

		for {
			select {
			case r := <-waiter:
				defer protoBufferPool.Put(r.buf)

				if len(r.res.Error) != 0 {
					c.error(RemoteError(r.res.Error))
					continue
				}

				if r.res.Close {
					return
				}

				out := newOut()

				if err := r.buf.Unmarshal(out.(proto.Message)); err != nil {
					c.error(err)
					continue
				}

				recvdOut(out)
			case <-ctx.Done():
				c.error(ctx.Err())
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
