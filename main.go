package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Request struct {
	ch chan<- string
}

type Queue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	broker   *Broker
	name     string
	messages []string
	requests []*Request
	inited   bool
}

func NewQueue(broker *Broker, name string) *Queue {
	q := &Queue{
		broker:   broker,
		name:     name,
		messages: make([]string, 0),
		requests: make([]*Request, 0),
	}
	q.cond = sync.NewCond(&q.mu)

	go q.dispatcher()

	return q
}

func (q *Queue) dispatcher() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		for len(q.requests) == 0 || len(q.messages) == 0 {
			if q.inited && len(q.requests) == 0 && len(q.messages) == 0 {
				q.Dispose()
				return
			}
			q.cond.Wait()
		}

		request := q.requests[0]
		q.requests = q.requests[1:]

		message := q.messages[0]
		q.messages = q.messages[1:]

		request.ch <- message
	}
}

func (q *Queue) RemoveRequest(request *Request) {
	q.mu.Lock()
	n := 0
	for _, r := range q.requests {
		if r != request {
			q.requests[n] = r
			n++
		}
	}
	q.requests = q.requests[:n]
	q.mu.Unlock()

	q.cond.Signal()
}

func (q *Queue) Dispose() {
	q.broker.RemoveQueue(q.name)
}

func (q *Queue) Put(message string) {
	q.mu.Lock()
	q.messages = append(q.messages, message)
	q.inited = true
	q.mu.Unlock()

	q.cond.Signal()
}

func (q *Queue) Get(timeout time.Duration, done <-chan struct{}) string {
	q.mu.Lock()

	if timeout == 0 {
		if len(q.messages) > 0 && len(q.requests) == 0 {
			message := q.messages[0]
			q.messages = q.messages[1:]
			q.mu.Unlock()
			q.cond.Signal()
			return message
		}
		q.mu.Unlock()
		return ""
	}

	ch := make(chan string)
	request := &Request{ch: ch}

	q.requests = append(q.requests, request)

	q.inited = true
	q.mu.Unlock()
	q.cond.Signal()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case message := <-ch:
		return message
	case <-timer.C:
	case <-done:
	}

	q.RemoveRequest(request)
	return ""
}

type Broker struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

func NewBroker() *Broker {
	broker := &Broker{
		queues: make(map[string]*Queue),
	}

	return broker
}

func (b *Broker) NewQueue(name string) *Queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue := NewQueue(b, name)
	b.queues[name] = queue
	return queue
}

func (b *Broker) GetQueue(name string) *Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.queues[name]
}

func (b *Broker) RemoveQueue(name string) {
	b.mu.Lock()
	delete(b.queues, name)
	b.mu.Unlock()
}

func (b *Broker) Get(name string, timeout time.Duration, done <-chan struct{}) string {
	queue := b.GetQueue(name)
	if queue == nil {
		if timeout == 0 {
			return ""
		}
		queue = b.NewQueue(name)
	}

	return queue.Get(timeout, done)
}

func (b *Broker) Put(name string, message string) {
	queue := b.GetQueue(name)
	if queue == nil {
		queue = b.NewQueue(name)
	}

	queue.Put(message)
}

func main() {
	broker := NewBroker()

	handlePut := func(w http.ResponseWriter, r *http.Request) {
		queue := r.PathValue("queue")
		message := r.URL.Query().Get("v")

		if message == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		broker.Put(queue, message)
		w.WriteHeader(http.StatusOK)
	}

	handleGet := func(w http.ResponseWriter, r *http.Request) {
		queue := r.PathValue("queue")

		var timeout time.Duration
		timeoutStr := r.URL.Query().Get("timeout")
		if timeoutStr != "" {
			seconds, err := strconv.Atoi(timeoutStr)
			if seconds > 0 && err == nil {
				timeout = time.Duration(seconds) * time.Second
			}
		}

		done := r.Context().Done()

		message := broker.Get(queue, timeout, done)

		if message == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(message))
	}

	http.HandleFunc("PUT /{queue}", handlePut)
	http.HandleFunc("GET /{queue}", handleGet)

	port := ":8080"

	if len(os.Args) > 1 {
		port = ":" + os.Args[1]
	}

	if err := http.ListenAndServe(port, nil); err != nil {
		fmt.Printf("Server failed to start: %v\n", err)
		os.Exit(1)
	}
}
