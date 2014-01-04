package main

import (
	"fmt"
	"encoding/json"

	"github.com/russellhaering/inundation/queue"
)

type User struct {
	Name string
}

func main() {
	c := queue.QueueManagerConfig{}
	c.CassandraHosts = []string{"localhost"}
	c.CassandraKeyspace = "inundation"
	m, err := queue.NewQueueManager("manager0", c)

	if err != nil {
		fmt.Println("Error instantiating QueueManager", err)
		return
	}

	defer m.Shutdown()

	value, err := json.Marshal(User{"Bob"})

	if err != nil {
		fmt.Println("Error marshaling JSON (what?)")
	}

	values := []queue.QueueItemValue{value, value, value, value}
	idx, err := m.Publish("queue_foo", values)

	if err != nil {
		fmt.Println("Publish error", err)
		return
	}

	fmt.Println("Published", len(values), "values, beginning at index", idx)

	items, err := m.Read("queue_foo", idx, 100)
	if err != nil {
		fmt.Println("Read error", err)
		return
	}

	fmt.Println("Read", len(items), "items")
}
