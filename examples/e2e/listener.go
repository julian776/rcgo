package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/julian776/rcgo"
)

func main() {
	lname := "testListener"

	configs := &rcgo.ListenerConfigs{
		Url:             "amqp://user:password@localhost",
		DelayOnReject:   time.Second * 5,
		AckIfNoHandlers: true,
		Timezone:        time.UTC,
		LogLevel:        "info",
		CmdsWorkers:     1,
		EventsWorkers:   1,
		QueriesWorkers:  1,
		PrefetchCount:   10,
	}

	l := rcgo.NewListener(
		configs,
		lname,
	)

	l.AddCommandHandler(
		"testListener.print",
		func(ctx context.Context, c *rcgo.Cmd) error {
			m := c.Data.(map[string]interface{})

			fmt.Printf("msg received %+v\n", m)

			return nil
		},
	)

	l.AddEventHandler(
		"orderPlaced",
		func(ctx context.Context, e *rcgo.Event) error {
			m := e.Data.(map[string]interface{})

			fmt.Printf("order received %+v\n", m)

			return nil
		},
	)

	l.AddQueryHandler(
		"testListener.employees",
		func(ctx context.Context, q *rcgo.Query) (interface{}, error) {
			req := q.Data.(map[string]interface{})

			id := req["id"]

			fmt.Printf("id received %s\n", id)

			// Fetch data

			return map[string]interface{}{
				"id":   id,
				"name": "julian",
			}, nil
		},
	)

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)

	go l.Listen(ctx)

	<-ctx.Done()

	l.Stop()
}
