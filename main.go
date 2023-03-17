package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	WithoutWaiting = -2
	EndlessWaiting = -1
)

type topic string

// База данных очереди.
type DB struct {
	m           sync.Mutex              //мьютекс для синхронизации доступа
	queue       map[topic][]string      // очередь состоит из topic и сообщений в очереди этого топика
	globalQueue map[topic]chan struct{} // сигнализирует, что в очереди чтото появилось
	//consumer map[topic][]chan int// кол-во слушателей которые ожидают данные
}

// Конструктор БД.
func New() *DB {
	db := DB{
		queue:       map[topic][]string{},
		globalQueue: map[topic]chan struct{}{},
	}
	return &db
}

// GetMsg получаем сообщение из очереди
func (db *DB) GetMsg(t topic) (string, bool) {
	db.m.Lock()
	defer db.m.Unlock()
	msg, ok := db.queue[t]
	if !ok {
		db.newTopicChan(t)
		return "", ok
	}
	if len(msg) == 0 {
		return "", false
	}
	db.queue[t] = db.queue[t][1:]
	return msg[0], true
}

// PutMsg добавляем сообщение в очередь
func (db *DB) PutMsg(t topic, n string) {
	db.m.Lock()
	defer db.m.Unlock()
	// если очереди не было, то создаем для нее канал ожидания сообщения
	if _, ok := db.queue[t]; !ok {
		db.newTopicChan(t)
	}
	db.queue[t] = append(db.queue[t], n)
	// сообщаем о наличии сообщения
	go func() {
		db.globalQueue[t] <- struct{}{}
	}()
}

// создаем канал для ожидающих в очереди консьюмеров
func (db *DB) newTopicChan(t topic) {
	if db.globalQueue[t] == nil {
		db.globalQueue[t] = make(chan struct{})
	}
}

// размер БД
func (db *DB) Len() int {
	return len(db.queue)
}

// База данных очереди.
type Server struct {
	db  *DB
	mux *http.ServeMux
}

// Конструктор БД.
func NewServer(db *DB) *Server {
	s := Server{db: db}
	s.mux = http.NewServeMux()
	return &s
}

func (s *Server) msgHandler(w http.ResponseWriter, r *http.Request) {
	tmp := strings.Split(r.URL.Path, "/")
	if !(len(tmp) == 2 && tmp[1] != "") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	t := topic(tmp[1])
	switch r.Method {
	case http.MethodGet:
		// consumer
		// http://127.0.0.1/pet?timeout=N
		// время ожидания в секундах
		timeout, err := strconv.Atoi(r.URL.Query().Get("timeout"))
		if err != nil {
			timeout = EndlessWaiting
		}
		msg, ok := s.consumer(t, timeout)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(msg))
	case http.MethodPut:
		// producer
		// http://127.0.0.1/pet?v=cat
		name := r.URL.Query().Get("v")
		if name == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.db.PutMsg(t, name)
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	db := New()
	serv := NewServer(db)

	serv.mux.HandleFunc("/", serv.msgHandler)
	fmt.Println("Server is listening localhost:" + port)
	http.ListenAndServe("localhost:"+port, serv.mux)
}

// если запрос поступил от consumer
// пытаемся прочитать текущее сообщение в очереди
// если сообщений нет, то ожидаем или выходим
// зависит от параметра timeout
func (s *Server) consumer(t topic, timeout int) (string, bool) {
	// запрашиваем соощение
	msg, ok := s.db.GetMsg(t)
	// если получили, то уходим
	if ok {
		return msg, ok
	}
	if timeout == WithoutWaiting {
		return "", false
	}
	// если сообщений нет, то ожидаем
	// timeout = -1 ожидаем бесконечно, можно убрать и тогда не будет ожидания
	// timeout задан, ожидаем заданное кол-во секунд
	if timeout == EndlessWaiting {
		for {
			_, ok := <-s.db.globalQueue[t]
			if !ok {
				return "", false // 404 ошибка канал закрыт и ничего нет
			}
			msg, ok := s.db.GetMsg(t)
			if ok {
				return msg, ok
			}
		}
	} else {
		for {
			select {
			// ожидаем сигнала о новом сообщении
			case _, ok := <-s.db.globalQueue[t]:
				if !ok {
					return "", false // 404 ошибка канал закрыт и ничего нет
				}
				// вот здесь может произойти так, что ктото другой успеет прочитать сообщение раньше
				msg, ok := s.db.GetMsg(t)
				if ok {
					return msg, ok
				}
			// ожидаем timeout секунд
			case <-time.After(time.Second * time.Duration(timeout)):
				return "", false
			}
		}
	}
}
