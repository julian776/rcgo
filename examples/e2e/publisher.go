package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/julian776/rcgo"
)

func main() {
	pname := "testPublisher"
	lname := "testListener"

	configs := &rcgo.PublisherConfigs{
		Url:           "amqp://user:password@localhost",
		ReplyTimeout:  time.Second * 15,
		PrefetchCount: 10,
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

	res, err := p.RequestReply(ctx, lname, "testListener.employees", qdata)
	if err != nil {
		fmt.Printf("err in RequestReply %s", err.Error())
		return
	}

	d := make(map[string]interface{})
	err = json.Unmarshal(res, &d)
	if err != nil {
		fmt.Printf("err in unmarshal data %s", err.Error())
		return
	}

	fmt.Printf("reply %+v", d)
}
