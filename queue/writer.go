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

type batchResult struct {
	idx int64
	err error
}

type batchRequest struct {
	items      []QueueItemValue
	resultChan chan batchResult
}

type queueWriter struct {
	id              string
	nextIndex       int64
	pendingRequests chan *batchRequest
}

func newQueueWriter(mgr *QueueManager, id string) (*queueWriter, error) {
	var lastIndex int64
	err := mgr.db.Query(`SELECT item_id FROM queue_items WHERE queue_id = ? ORDER BY item_id DESC LIMIT 1`, id).Scan(&lastIndex)
	// Scan returns an ErrNotFound if the writer didn't previously exist. If
	// that happens we default lastIndex to -1 so that nextIndex will be 0. If
	// any other error occurs, return it.
	if err == gocql.ErrNotFound {
		lastIndex = -1
	} else if err != nil {
		return nil, err
	}

	writer := &queueWriter{
		id:              id,
		nextIndex:       lastIndex + int64(1),
		pendingRequests: make(chan *batchRequest),
	}

	go writer.process(mgr.db, mgr.done)
	return writer, nil
}

func (writer *queueWriter) publish(items []QueueItemValue) (idx int64, err error) {
	request := batchRequest{
		items:      items,
		resultChan: make(chan batchResult),
	}

	defer func() {
		if r := recover(); r != nil {
			err = ErrManagerShutdown
		}
	}()

	writer.pendingRequests <- &request

	result := <-request.resultChan
	return result.idx, result.err
}

func (writer *queueWriter) process(db *gocql.Session, done *sync.WaitGroup) {
	done.Add(1)

	for {
		request := <-writer.pendingRequests

		if request == nil {
			break
		}

		// We got a request - drain any additional requests that are pending, then
		// flush them all to the database
		requests := []*batchRequest{request}
		requests = append(requests, writer.drainPending()...)
		writer.writeBatch(db, requests)
	}
	done.Done()
}

func (writer *queueWriter) shutdown() {
	close(writer.pendingRequests)
}

func (writer *queueWriter) drainPending() []*batchRequest {
	requests := []*batchRequest{}

	for {
		select {
		case request := <-writer.pendingRequests:
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

func (writer *queueWriter) writeBatch(db *gocql.Session, requests []*batchRequest) {
	dbBatch := gocql.NewBatch(gocql.UnloggedBatch)
	i := int64(0)

	for _, request := range requests {
		for _, item := range request.items {
			itemID := writer.nextIndex + i
			dbBatch.Query(`INSERT INTO queue_items (queue_id, item_id, item_value) VALUES (?, ?, ?)`, writer.id, itemID, item)
			i++
		}
	}

	err := db.ExecuteBatch(dbBatch)
	writer.respond(requests, err)
}

func (writer *queueWriter) respond(requests []*batchRequest, err error) {
	i := int64(0)

	for _, request := range requests {
		result := batchResult{err: err}

		if err == nil {
			result.idx = writer.nextIndex + i
		}

		request.resultChan <- result
		i += int64(len(request.items))
	}

	writer.nextIndex += i
}
