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
	mu    sync.RWMutex
	writers       map[string]*queueWriter
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
		writers:       make(map[string]*queueWriter),
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

	mgr.mu.RLock()
	for queueID := range mgr.writers {
		batch.Query(`UPDATE queue_managers USING TTL ? SET manager_id = ? WHERE queue_id = ?`,
			RESERVATION_TTL, mgr.name, queueID)
	}
	mgr.mu.RUnlock()

	// TODO: Panic hard if an error occurs
	mgr.db.ExecuteBatch(batch)
}

func (mgr *QueueManager) getOrCreateQueueWriter(queueID string) (*queueWriter, error) {
	// Hot path: just get the queue from the map
	mgr.mu.RLock()
	if mgr.stopped {
		return nil, ErrManagerShutdown
	}

	writer, exists := mgr.writers[queueID]
	mgr.mu.RUnlock()

	if exists {
		return writer, nil
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

	// Slow path: get the write lock, make sure the writer hasn't been created
	// then create it.
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if mgr.stopped {
		return nil, ErrManagerShutdown
	}

	writer, exists = mgr.writers[queueID]

	if exists {
		return writer, nil
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

	writer, err = newQueueWriter(mgr, queueID)
	if err != nil {
		return nil, err
	}

	mgr.writers[queueID] = writer
	return writer, nil
}

func (mgr *QueueManager) Publish(queueID string, items []QueueItemValue) (int64, error) {
	writer, err := mgr.getOrCreateQueueWriter(queueID)

	if err != nil {
		return 0, err
	}

	return writer.publish(items)
}

func (mgr *QueueManager) Read(queueID string, startIndex int64, count int) ([]QueueItem, error) {
	query := mgr.db.Query(`SELECT item_id, item_value FROM queue_items WHERE queue_id = ? AND item_id >= ? LIMIT ?;`,
		queueID, startIndex, count)

	// Disable result paging, we know exactly how much we want
	query.PageSize(0)

	iter := query.Iter()

	items := []QueueItem{}

	var itemID int64
	var itemValue QueueItemValue

	for iter.Scan(&itemID, &itemValue) {
		// We could do better than the built-in append here if it mattered. Common cases are:
		// - no items
		// - a few items
		// - `count` items
		items = append(items, QueueItem{ID: itemID, Value: itemValue})
	}

	err := iter.Close()

	if err != nil {
		return nil, err
	}

	return items, nil
}

func (mgr *QueueManager) Shutdown() {
	mgr.mu.Lock()
	mgr.stopped = true
	for _, writer := range mgr.writers {
		writer.shutdown()
	}
	mgr.mu.Unlock()

	// Wait for all writers to drain
	mgr.done.Wait()

	// Kill the heartbeat
	mgr.stopKeepAlive <- true
}
