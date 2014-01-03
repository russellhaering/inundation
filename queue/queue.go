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
	id          string
	nextIndex   int64
	publishLock sync.Mutex
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
		queues: make(map[string]*Queue),
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
		var lastIndex int64
		err := mgr.db.Query(`SELECT item_id FROM queue_items WHERE queue_id = ? ORDER BY item_id DESC LIMIT 1`, queueID).Scan(&lastIndex)
		// Scan returns an ErrNotFound if the queue didn't previously exist. If
		// that happens we default lastIndex to -1 so that nextIndex will be 0. If
		// any other error occurs, return it.
		if err == gocql.ErrNotFound {
			lastIndex = -1
		} else if err != nil {
			return nil, err
		}

		queue = &Queue{
			id:        queueID,
			nextIndex: lastIndex + int64(1),
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

func (mgr *QueueManager) Publish(queueID string, items []QueueItem) (int64, error) {
	queue, err := mgr.getOrCreateQueue(queueID)

	if err != nil {
		return 0, err
	}

	queue.publishLock.Lock()
	defer queue.publishLock.Unlock()

	// TODO: batch pending items
	idx := queue.nextIndex

	batch := gocql.NewBatch(gocql.UnloggedBatch)

	for i, item := range items {
		itemID := idx + int64(i)
		batch.Query(`INSERT INTO queue_items (queue_id, item_id, item_value) VALUES (?, ?, ?)`, queue.id, itemID, item)
	}

	err = mgr.db.ExecuteBatch(batch)

	if err != nil {
		return 0, err
	}

	queue.nextIndex = idx + int64(len(items))

	return idx, nil
}
