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
	ReservationTTL    time.Duration
}

func (cfg *QueueManagerConfig) reservationTTL() time.Duration {
	if cfg.ReservationTTL == 0 {
		return 60
	} else {
		return cfg.ReservationTTL
	}
}

// QueueManager is used to publish to and read from queues. Every QueueManager
// must supply a unique name - this is not enforced, but failure to do so will
// result in Bad Things happening.
//
// Each queue may only be published to by a single QueueManager, which becomes
// the master for that queue. Attempts to publish to a queue not owned by the
// QueueManager will result in an ErrWrongManager error, which indicates the
// name of the QueueManager responsible for that queue. For this reason, it may
// be useful to encode the information needed to reach a QueueManager in its
// name.  For example, if exposed by an HTTP API, the hostname and port of HTTP
// service could be encoded in the manager's name in order to generate an
// appropriate redirect.
//
// Any QueueManager may read from any queue.
type QueueManager struct {
	Name          string
	config        QueueManagerConfig
	mu            sync.RWMutex
	writers       map[string]*queueWriter
	db            *gocql.Session
	done          *sync.WaitGroup
	stopKeepAlive chan bool
	stopped       bool
}

// NewQueueManager constructs a new QueueManager with a given name and
// configuration. The QueueManagerConfig is copied by value, so attempts to
// mutate a configuration already passed to NewQueueManager will have no
// effect.
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
	ticker := time.NewTicker((mgr.config.reservationTTL() / 3.0) * time.Second)

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
			mgr.config.reservationTTL(), mgr.Name, queueID)
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

	if actualManager != "" && actualManager != mgr.Name {
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
		queueID, mgr.Name, mgr.config.reservationTTL()).ScanCAS(&uselessID, &actualManager)

	if err != nil {
		return nil, err
	}

	if !applied && actualManager != mgr.Name {
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

// Publish a batch of values to a queue. Each each value will be assigned an
// integer ID.  Upon success, Publish will return the ID of the first value.
// For example, if 4 values are published and an ID of 17 is returned, then the
// four values have been assigned IDs 17, 18, 19 and 20 respectively.
//
// If the specified queue does not exist, it will be created.
func (mgr *QueueManager) Publish(queueID string, items []QueueItemValue) (int64, error) {
	writer, err := mgr.getOrCreateQueueWriter(queueID)

	if err != nil {
		return 0, err
	}

	return writer.publish(items)
}

// Read values from a queue, starting at the specified index.
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

// Shutdown the queue manager. Subsequent writes will fail with
// ErrManagerShutdown.
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
