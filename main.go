package main

import (
	"context"
	"flag"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Queue struct {
	Items              map[string][]string
	Waiting            map[string][]chan string
	ItemMutex          sync.Mutex
	WaitingMutex       sync.Mutex
	SkipDefaultTimeout bool
}

// параметры командной строки (все опциональные):
//   - port: вводится как число, например, 8888. По умолчанию 8080
//   - sdto (skip default timeout):
//     если true, то при отсутствии сообщений в очереди сразу приходит ответ 404, если не указан таймаут в запросе;
//     если false, то при отсутствии таймаута в запросе будет использован дефолтный 60 секунд.
//     по умолчанию false
func main() {
	port := flag.String("port", "8080", "port to listen on")
	skipDefaultTimeout := flag.Bool("sdto", false, "skip default timeout")
	flag.Parse()

	q := &Queue{
		Items:              make(map[string][]string),
		Waiting:            make(map[string][]chan string),
		ItemMutex:          sync.Mutex{},
		WaitingMutex:       sync.Mutex{},
		SkipDefaultTimeout: *skipDefaultTimeout,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
		handle(w, r, q)
	})
	err := http.ListenAndServe(":"+*port, mux)
	if err != nil {
		return
	}
}

func handle(w http.ResponseWriter, r *http.Request, q *Queue) {
	if r.Method == "GET" {
		handleGet(w, r, q)
	} else if r.Method == "PUT" {
		handlePut(w, r, q)
	} else {
		handleError(w, http.StatusMethodNotAllowed)
	}
}

func handleGet(w http.ResponseWriter, r *http.Request, q *Queue) {
	key := r.PathValue("key")
	timeoutStr := r.URL.Query().Get("timeout")
	var timeout int

	if (timeoutStr != "") && (timeoutStr != "0") {
		to, err := strconv.Atoi(timeoutStr)
		if err != nil {
			handleError(w, http.StatusBadRequest)
		} else {
			timeout = to
		}
	} else if q.SkipDefaultTimeout {
		timeout = 0
	} else {
		timeout = 60
	}

	first := q.getAndDelete(key)

	if first != "" {
		_, err := w.Write([]byte(first))
		if err != nil {
			// log error
			return
		}
	} else if timeout == 0 {
		handleError(w, http.StatusNotFound)
	} else {
		ch := make(chan string, 1)
		q.Waiting[key] = append(q.Waiting[key], ch)

		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout)*time.Second)
		defer cancel()

		select {
		case value := <-ch:
			_, err := w.Write([]byte(value))
			if err != nil {
				// log error
				return
			}
		case <-ctx.Done():
			q.WaitingMutex.Lock()
			for i, c := range q.Waiting[key] {
				if c == ch {
					q.Waiting[key] = append(q.Waiting[key][:i], q.Waiting[key][i+1:]...)
					q.WaitingMutex.Unlock()
					w.WriteHeader(http.StatusNotFound)
					close(ch)
					break
				}
			}
		}
	}
}

func handlePut(w http.ResponseWriter, r *http.Request, q *Queue) {
	key := r.PathValue("key")
	value := r.URL.Query().Get("v")

	if key == "" || value == "" {
		handleError(w, http.StatusBadRequest)
	}

	q.ItemMutex.Lock()
	defer q.ItemMutex.Unlock()

	if len(q.Waiting) == 0 {
		q.put(key, value)
	} else {
		chans, exists := q.Waiting[key]
		if !exists || len(chans) == 0 {
			q.put(key, value)
		} else {
			ch := chans[0]
			q.Waiting[key] = chans[1:]
			ch <- value
			close(ch)
		}
	}
}

func handleError(w http.ResponseWriter, code int) {
	w.WriteHeader(code)
}

func (q *Queue) put(key string, value string) {
	items, exists := q.Items[key]
	if exists {
		items = append(items, value)
		q.Items[key] = items
	} else {
		q.Items[key] = []string{value}
	}
}

func (q *Queue) getAndDelete(key string) string {
	q.ItemMutex.Lock()
	defer q.ItemMutex.Unlock()
	items, exists := q.Items[key]
	if exists && len(items) > 0 {
		item := items[0]
		q.Items[key] = items[1:]
		return item
	}
	return ""
}
