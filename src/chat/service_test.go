package main

import (
	"testing"
	"time"
)

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

import (
	. "proto"
)

const (
	address = "localhost:50008"
)

var (
	conn *grpc.ClientConn
	err  error
)

func TestChat(t *testing.T) {
	conn, err = grpc.Dial(address)
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}

	c := NewChatServiceClient(conn)

	//注册聊天通道
	_, err = c.Reg(context.Background(), &Chat_Id{Id: 1})
	if err != nil {
		t.Logf("could not query: %v", err)
	}

	const COUNT = 10
	go recv(&Chat_Id{1}, COUNT, t, "by1")
	go recv(&Chat_Id{1}, COUNT, t, "by2")
	time.Sleep(3 * time.Second)
	go send(&Chat_Message{Id: 1, Body: []byte("Hello")}, COUNT, t)
	time.Sleep(3 * time.Second)
}

//发送消息
func send(m *Chat_Message, count int, t *testing.T) {
	c := NewChatServiceClient(conn)
	for {
		if count == 0 {
			return
		}
		_, err := c.Send(context.Background(), m)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("send:", m)
		count--
	}
}

//接收消息
func recv(chat_id *Chat_Id, count int, t *testing.T, tag string) {
	c := NewChatServiceClient(conn)
	//订阅消息, 等待接收
	stream, err := c.Subscribe(context.Background(), chat_id)
	if err != nil {
		t.Fatal(err)
	}
	for {
		if count == 0 {
			return
		}
		//获取数据
		message, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		//println("recv:", count)
		t.Log("recv:", message, " -- ", tag)
		count--
	}
}
