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
	"time"

	"tux21b.org/v1/gocql"
)

const (
	RESERVATION_TTL = 60
)

var (
	ErrManagerShutdown = errors.New("queue manager is shutdown")
)

type ErrWrongManager struct {
	ActualManager string
}

func (err *ErrWrongManager) Error() string {
	return "queue has another manager: " + err.ActualManager
}

type QueueManagerConfig struct {
	CassandraHosts    []string
	CassandraKeyspace string
}

type QueueManager struct {
	name          string
	config        QueueManagerConfig
	queuesLock    sync.RWMutex
	queues        map[string]*Queue
	db            *gocql.Session
	done          *sync.WaitGroup
	stopKeepAlive chan bool
	stopped       bool
}

func NewQueueManager(name string, config QueueManagerConfig) (*QueueManager, error) {
	cassCluster := gocql.NewCluster(config.CassandraHosts...)
	cassCluster.Keyspace = config.CassandraKeyspace
	cassSession, err := cassCluster.CreateSession()

	if err != nil {
		return nil, err
	}

	mgr := &QueueManager{
		name:          name,
		config:        config,
		queues:        make(map[string]*Queue),
		db:            cassSession,
		done:          &sync.WaitGroup{},
		stopKeepAlive: make(chan bool),
	}

	go mgr.keepAlive()
	return mgr, nil
}

func (mgr *QueueManager) keepAlive() {
	ticker := time.NewTicker((RESERVATION_TTL / 3.0) * time.Second)

	for {
		select {
		case <-mgr.stopKeepAlive:
			break

		case <-ticker.C:
			mgr.heartbeatReservations()
		}
	}

	ticker.Stop()
}

func (mgr *QueueManager) heartbeatReservations() {
	batch := gocql.NewBatch(gocql.UnloggedBatch)

	mgr.queuesLock.RLock()
	for queueID := range mgr.queues {
		batch.Query(`UPDATE queue_managers USING TTL ? SET manager_id = ? WHERE queue_id = ?`,
			RESERVATION_TTL, mgr.name, queueID)
	}
	mgr.queuesLock.RUnlock()

	// TODO: Panic hard if an error occurs
	mgr.db.ExecuteBatch(batch)
}

func (mgr *QueueManager) getOrCreateQueue(queueID string) (*Queue, error) {
	// Hot path: just get the queue from the map
	mgr.queuesLock.RLock()
	if mgr.stopped {
		return nil, ErrManagerShutdown
	}

	queue, exists := mgr.queues[queueID]
	mgr.queuesLock.RUnlock()

	if exists {
		return queue, nil
	}

	// Before we go down the really slow path, see if someone else owns the queue
	actualManager := ""
	err := mgr.db.Query(`SELECT manager_id FROM queue_managers WHERE queue_id = ?`, queueID).Scan(&actualManager)
	if err != nil && err != gocql.ErrNotFound {
		return nil, err
	}

	if actualManager != "" && actualManager != mgr.name {
		return nil, &ErrWrongManager{actualManager}
	}

	// Slow path: get the write lock, make sure the queue hasn't been created
	// then create it.
	mgr.queuesLock.Lock()
	defer mgr.queuesLock.Unlock()

	if mgr.stopped {
		return nil, ErrManagerShutdown
	}

	queue, exists = mgr.queues[queueID]

	if exists {
		return queue, nil
	}

	// Attempt to register as the manager for this queue
	var uselessID string
	applied, err := mgr.db.Query(`INSERT INTO queue_managers (queue_id, manager_id) VALUES (?, ?) IF NOT EXISTS USING TTL ?;`,
		queueID, mgr.name, RESERVATION_TTL).ScanCAS(&uselessID, &actualManager)

	if err != nil {
		return nil, err
	}

	if !applied && actualManager != mgr.name {
		_ = uselessID
		return nil, &ErrWrongManager{actualManager}
	}

	queue, err = NewQueue(mgr, queueID)
	if err != nil {
		return nil, err
	}

	mgr.queues[queueID] = queue
	return queue, nil
}

func (mgr *QueueManager) Publish(queueID string, items []QueueItem) (int64, error) {
	queue, err := mgr.getOrCreateQueue(queueID)

	if err != nil {
		return 0, err
	}

	return queue.publish(items)
}

func (mgr *QueueManager) Shutdown() {
	mgr.queuesLock.Lock()
	mgr.stopped = true
	for _, queue := range mgr.queues {
		queue.shutdown()
	}
	mgr.queuesLock.Unlock()

	// Wait for all queues to drain
	mgr.done.Wait()

	// Kill the heartbeat
	mgr.stopKeepAlive <- true
}
