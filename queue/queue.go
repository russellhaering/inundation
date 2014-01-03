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

type BatchResult struct {
	idx int64
	err error
}

type QueueItemBatchRequest struct {
	items      []QueueItem
	resultChan chan BatchResult
}

type Queue struct {
	id              string
	nextIndex       int64
	pendingRequests chan *QueueItemBatchRequest
}

func (queue *Queue) publish(items []QueueItem) (int64, error) {
	request := QueueItemBatchRequest{
		items:      items,
		resultChan: make(chan BatchResult),
	}
	queue.pendingRequests <- &request

	result := <-request.resultChan
	return result.idx, result.err
}

func (queue *Queue) process(db *gocql.Session) {
	requests := []*QueueItemBatchRequest{}

	for {
		// Our goal here is to shift any pending requests off of the channel and
		// onto our list, then to batch together all of the requests into a single
		// database write.
		//
		// First, check if any requsts are waiting in the channel
		select {

		// If a request was waiting in the channel, shift it onto the list.
		case request := <-queue.pendingRequests:
			requests = append(requests, request)

		// If no requests were waiting..
		default:
			if len(requests) == 0 {
				// The pending list is also empty. Wait for someone to put a request in
				// the channel, shift it to the list, then loop back to the top.
				request := <-queue.pendingRequests
				requests = append(requests, request)
			} else {
				// No more requests are waiting, but we have some in our list. Go!
				queue.writeBatch(db, requests)
				requests = []*QueueItemBatchRequest{}
			}
		}
	}
}

func (queue *Queue) writeBatch(db *gocql.Session, requests []*QueueItemBatchRequest) {
	dbBatch := gocql.NewBatch(gocql.UnloggedBatch)
	i := int64(0)

	for _, request := range requests {
		for _, item := range request.items {
			itemID := queue.nextIndex + i
			dbBatch.Query(`INSERT INTO queue_items (queue_id, item_id, item_value) VALUES (?, ?, ?)`, queue.id, itemID, item)
			i++
		}
	}

	err := db.ExecuteBatch(dbBatch)

	i = int64(0)

	for _, request := range requests {
		result := BatchResult{
			idx: queue.nextIndex + i,
			err: err,
		}
		request.resultChan <- result
		i += int64(len(request.items))
	}

	queue.nextIndex += i
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
			id:              queueID,
			nextIndex:       lastIndex + int64(1),
			pendingRequests: make(chan *QueueItemBatchRequest),
		}
		go queue.process(mgr.db)
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

	return queue.publish(items)
}
