package main

import (
	"net"
	"os"
	"strconv"
	"time"

	"github.com/tmthrgd/Ne"
	pb "github.com/tmthrgd/Ne/example"
	rpc "github.com/tmthrgd/Ne/rpc"
)

type server struct{}

func (server) SayHello(in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

type notification struct{}

func (notification) Subscribe(in *pb.Channel) (<-chan *pb.Notification, error) {
	out := make(chan *pb.Notification, 3)

	i := 1

	for ; i <= 3; i++ {
		out <- &pb.Notification{Message: "Notification #" + strconv.Itoa(i) + " from " + in.Name}
	}

	go func() {
		defer close(out)

		t := time.NewTicker(time.Second)
		defer t.Stop()

		for range t.C {
			out <- &pb.Notification{Message: "Notification #" + strconv.Itoa(i) + " from " + in.Name}
			i++

			if i > 10 {
				break
			}
		}
	}()

	return out, nil
}

func main() {
	ip, err := Ne.IP("fda2:ea02:479f::/48", 0x9d26f5d4df2cbbb4)

	if err != nil {
		panic(err)
	}

	go func() {
		s := rpc.NewServer()
		pb.RegisterGreeterServer(s, server{})
		pb.RegisterNotificationsServer(s, notification{})

		if err := s.ListenAndServe("udp6", net.JoinHostPort(ip.String(), "http")); err != nil {
			panic(err)
		}
	}()

	path := Ne.Path("/var", 0x9d26f5d4df2cbbb4)
	os.Remove(path)
	defer os.Remove(path)

	s := rpc.NewServer()
	pb.RegisterGreeterServer(s, server{})
	pb.RegisterNotificationsServer(s, notification{})

	if err := s.ListenAndServe("unix", path); err != nil {
		panic(err)
	}
}
