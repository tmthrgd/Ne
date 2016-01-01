package main

import (
	"fmt"
	"net"

	"github.com/tmthrgd/Ne"
	pb "github.com/tmthrgd/Ne/example"
	rpc "github.com/tmthrgd/Ne/rpc"
	"golang.org/x/net/context"
)

func main() {
	ip, err := Ne.IP("fda2:ea02:479f::/48", 0x9d26f5d4df2cbbb4)

	if err != nil {
		panic(err)
	}

	conn, err := rpc.Dial("udp6", net.JoinHostPort(ip.String(), "http"))

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	res, err := pb.NewGreeterClient(conn).SayHello(context.Background(), &pb.HelloRequest{Name: "Bob"})

	if err != nil {
		panic(err)
	}

	fmt.Printf("Greeting: %s\n", res.Message)

	notifications, err := pb.NewNotificationsClient(conn).Subscribe(context.Background(), &pb.Channel{Name: "main"})

	if err != nil {
		panic(err)
	}

	conn, err = rpc.Dial("unix", Ne.Path("/var", 0x9d26f5d4df2cbbb4))

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	res, err = pb.NewGreeterClient(conn).SayHello(context.Background(), &pb.HelloRequest{Name: "Ted"})

	if err != nil {
		panic(err)
	}

	fmt.Printf("Greeting: %s\n", res.Message)

	for not := range notifications {
		fmt.Printf("Notification: %s\n", not.Message)
	}
}
