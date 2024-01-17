package main

import (
	"context"
	"fmt"
	"time"

	"github.com/julian776/rcgo"
)

func main() {
	pname := "testPublisher"
	lname := "testListener"

	configs := &rcgo.PublisherConfigs{
		Url:          "amqp://user:password@localhost",
		ReplyTimeout: time.Second * 15,
	}

	p := rcgo.NewPublisher(
		configs,
		pname,
	)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)

	p.Start(ctx)

	data := map[string]string{
		"app":       "reactive-commons-go",
		"shortname": "rcgo",
		"name":      "julian776",
	}
	p.SendCmd(ctx, lname, "testListener.print", data)

	time.Sleep(time.Second * 5)

	edata := map[string]string{
		"id":      "US-NY-5434789",
		"product": "rcgo",
		"price":   "776",
	}
	p.PublishEvent(ctx, "orderPlaced", edata)

	time.Sleep(time.Second * 5)

	qdata := map[string]string{
		"id": "776",
	}

	var res interface{}
	err := p.RequestReply(ctx, lname, "testListener.employees", qdata, &res)
	if err != nil {
		fmt.Errorf("err in RequestReply %s", err.Error())
	}

	fmt.Printf("reply %+v", res)

	time.Sleep(time.Second * 5)
}
