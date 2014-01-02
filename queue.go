// Copyright 2013 Russell Haering.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queue

import (
	"sync"

	"tux21b.org/v1/gocql"
)

type QueueItem []byte

type Queue struct {
	id           string
	nextIndex    int
	publishLock  sync.Mutex
	pendingItems []QueueItem
}

type QueueManagerConfig struct {
	CassandraHosts    []string
	CassandraKeyspace string
}

type QueueManager struct {
	name       string
	config     QueueManagerConfig
	queuesLock sync.RWMutex
	queues     map[string]*Queue
	db         *gocql.Session
}

func (queue *Queue) Publish(items []QueueItem) (int, error) {
	queue.publishLock.Lock()
	queue.pendingItems = append(queue.pendingItems, items...)

	// TODO: flush pending items
	idx := queue.nextIndex
	queue.nextIndex = idx + len(items)
	queue.publishLock.Unlock()

	return idx, nil
}

func NewQueueManager(name string, config QueueManagerConfig) (*QueueManager, error) {
	cassCluster := gocql.NewCluster(config.CassandraHosts...)
	cassCluster.Keyspace = config.CassandraKeyspace
	cassSession, err := cassCluster.CreateSession()

	if err != nil {
		return nil, err
	}

	return &QueueManager{
		name:   name,
		config: config,
		db:     cassSession,
	}, nil
}

func (mgr *QueueManager) getOrCreateQueue(queueID string) (*Queue, error) {
	// Hot path: just get the queue from the map
	mgr.queuesLock.RLock()
	queue, exists := mgr.queues[queueID]
	mgr.queuesLock.RUnlock()

	if exists {
		return queue, nil
	}

	// TODO: try to register as the queue's manager

	mgr.queuesLock.Lock()
	queue, exists = mgr.queues[queueID]

	if !exists {
		queue = &Queue{
			id: queueID,
		}
		mgr.queues[queueID] = queue
	}

	mgr.queuesLock.Unlock()

	return queue, nil
}

func (mgr *QueueManager) LookupQueue(queueID string) (string, error) {
	// TODO: stop pretending we own every queue
	return mgr.name, nil
}

func (mgr *QueueManager) Publish(queueID string, items []QueueItem) (int, error) {
	queue, err := mgr.getOrCreateQueue(queueID)

	if err != nil {
		return 0, err
	}

	return queue.Publish(items)
}
