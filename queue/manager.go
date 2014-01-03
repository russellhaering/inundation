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
	"errors"
	"sync"

	"tux21b.org/v1/gocql"
)

var (
	ErroWrongManager = errors.New("queue has another manager")
)

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

	// Slow path: get the write lock, make sure the queue hasn't been created
	// then create it.

	mgr.queuesLock.Lock()
	defer mgr.queuesLock.Unlock()
	queue, exists = mgr.queues[queueID]

	if exists {
		return queue, nil
	}

	// Attempt to register as the manager for this queue
	var actualManager string
	var uselessID string
	applied, err := mgr.db.Query(`INSERT INTO queue_managers (queue_id, manager_id) VALUES (?, ?) IF NOT EXISTS;`, queueID, mgr.name).ScanCAS(&uselessID, &actualManager)

	if err != nil {
		return nil, err
	}

	if !applied && actualManager != mgr.name {
		_ = uselessID
		return nil, ErroWrongManager
	}

	queue, err = NewQueue(mgr.db, queueID)
	if err != nil {
		return nil, err
	}

	mgr.queues[queueID] = queue
	return queue, nil
}

func (mgr *QueueManager) LookupQueue(queueID string) (string, error) {
	// TODO: stop pretending we own every queue
	managerID := ""
	err := mgr.db.Query(`SELECT manager_id FROM queue_managers WHERE queue_id = ?`, queueID).Scan(&managerID)
	if err == gocql.ErrNotFound {
		return "",  nil
	}
	return managerID, err
}

func (mgr *QueueManager) Publish(queueID string, items []QueueItem) (int64, error) {
	queue, err := mgr.getOrCreateQueue(queueID)

	if err != nil {
		return 0, err
	}

	return queue.publish(items)
}
