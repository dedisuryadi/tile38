package endpoint

import (
	"fmt"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

const (
	nsqExpiresAfter = time.Second * 30
)

// NSQConn is an endpoint connection
type NSQConn struct {
	mu   sync.Mutex
	ep   Endpoint
	ex   bool
	t    time.Time
	conn *nsq.Producer
}

func newNSQConn(ep Endpoint) *NSQConn {
	return &NSQConn{
		ep: ep,
		t:  time.Now(),
	}
}

// Expired returns true if the connection has expired
func (conn *NSQConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Now().Sub(conn.t) > nsqExpiresAfter {
			if conn.conn != nil {
				conn.close()
			}
			conn.ex = true
		}
	}
	return conn.ex
}

func (conn *NSQConn) close() {
	if conn.conn != nil {
		conn.conn.Stop()
		conn.conn = nil
	}
}

// Send sends a message
func (conn *NSQConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.ex {
		return errExpired
	}
	conn.t = time.Now()
	if conn.conn == nil {
		var err error
		addr := fmt.Sprintf("%s:%d", conn.ep.NSQ.Host, conn.ep.NSQ.Port)
		ncfg := nsq.NewConfig()
		conn.conn, err = nsq.NewProducer(addr, ncfg)
		if err != nil {
			conn.close()
			return err
		}
	}
	err := conn.conn.Publish(conn.ep.NSQ.Topic, []byte(msg))
	if err != nil {
		conn.close()
		return err
	}

	return nil
}
