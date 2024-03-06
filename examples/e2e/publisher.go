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

	data, err := json.Marshal(map[string]string{
		"app":       "reactive-commons-go",
		"shortname": "rcgo",
		"name":      "julian776",
	})
	if err != nil {
		fmt.Printf("err in marshal data %s", err.Error())
		return
	}

	p.SendCmd(ctx, lname, "testListener.print", data)

	time.Sleep(time.Second * 5)

	edata, err := json.Marshal(map[string]string{
		"id":      "US-NY-5434789",
		"product": "rcgo",
		"price":   "776",
	})
	if err != nil {
		fmt.Printf("err in marshal data %s", err.Error())
		return
	}
	p.PublishEvent(ctx, "orderPlaced", edata)

	time.Sleep(time.Second * 5)

	qdata, err := json.Marshal(map[string]string{
		"id": "776",
	})
	if err != nil {
		fmt.Printf("err in marshal data %s", err.Error())
		return
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
