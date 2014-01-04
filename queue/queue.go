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

type QueueItemValue []byte

type BatchResult struct {
	idx int64
	err error
}

type QueueItemBatchRequest struct {
	items      []QueueItemValue
	resultChan chan BatchResult
}

type Queue struct {
	id              string
	nextIndex       int64
	pendingRequests chan *QueueItemBatchRequest
}

func NewQueue(mgr *QueueManager, id string) (*Queue, error) {
	var lastIndex int64
	err := mgr.db.Query(`SELECT item_id FROM queue_items WHERE queue_id = ? ORDER BY item_id DESC LIMIT 1`, id).Scan(&lastIndex)
	// Scan returns an ErrNotFound if the queue didn't previously exist. If
	// that happens we default lastIndex to -1 so that nextIndex will be 0. If
	// any other error occurs, return it.
	if err == gocql.ErrNotFound {
		lastIndex = -1
	} else if err != nil {
		return nil, err
	}

	queue := &Queue{
		id:              id,
		nextIndex:       lastIndex + int64(1),
		pendingRequests: make(chan *QueueItemBatchRequest),
	}

	go queue.process(mgr.db, mgr.done)
	return queue, nil
}

func (queue *Queue) publish(items []QueueItemValue) (idx int64, err error) {
	request := QueueItemBatchRequest{
		items:      items,
		resultChan: make(chan BatchResult),
	}

	defer func() {
		if r := recover(); r != nil {
			err = ErrManagerShutdown
		}
	}()

	queue.pendingRequests <- &request

	result := <-request.resultChan
	return result.idx, result.err
}

func (queue *Queue) process(db *gocql.Session, done *sync.WaitGroup) {
	done.Add(1)

	for {
		request := <-queue.pendingRequests

		if request == nil {
			break
		}

		// We got a request - drain any additional requests that are pending, then
		// flush them all to the database
		requests := []*QueueItemBatchRequest{request}
		requests = append(requests, queue.drainPending()...)
		queue.writeBatch(db, requests)
	}
	done.Done()
}

func (queue *Queue) shutdown() {
	close(queue.pendingRequests)
}

func (queue *Queue) drainPending() []*QueueItemBatchRequest {
	requests := []*QueueItemBatchRequest{}

	for {
		select {
		case request := <-queue.pendingRequests:
			if request == nil {
				// This will happen (_not_ the default case) if the channel is closed
				return requests
			}
			requests = append(requests, request)

		default:
			return requests
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
	queue.respond(requests, err)
}

func (queue *Queue) respond(requests []*QueueItemBatchRequest, err error) {
	i := int64(0)

	for _, request := range requests {
		result := BatchResult{err: err}

		if err == nil {
			result.idx = queue.nextIndex + i
		}

		request.resultChan <- result
		i += int64(len(request.items))
	}

	queue.nextIndex += i
}
