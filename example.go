package main

import (
	"fmt"

	"github.com/russellhaering/inundation/queue"
)

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

	idx, err := m.Publish("queue_foo", []queue.QueueItem{
		queue.QueueItem{0, 1, 2, 3},
		queue.QueueItem{0, 1, 2, 3},
		queue.QueueItem{0, 1, 2, 3},
		queue.QueueItem{0, 1, 2, 3},
	})

	if err != nil {
		fmt.Println("Publish error", err)
		return
	}

	fmt.Println("Published 4 items, beginning at index", idx)
}
