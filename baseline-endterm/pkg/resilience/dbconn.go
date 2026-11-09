package resilience

import (
	"context"
	"database/sql"
	"net"
	"time"

	"github.com/lib/pq"
)

// dbIOTimeout bounds one socket read or write. Context timeouts alone are not
// enough: on cancel lib/pq sends a cancel request to the server and keeps blocking
// on the socket, so a frozen database would hang the caller regardless of context.
// All queries here are short primary-key lookups, so 1.5s means the database is stuck.
const dbIOTimeout = 1500 * time.Millisecond

type deadlineConn struct{ net.Conn }

func (c deadlineConn) Read(b []byte) (int, error) {
	c.Conn.SetReadDeadline(time.Now().Add(dbIOTimeout))
	return c.Conn.Read(b)
}

func (c deadlineConn) Write(b []byte) (int, error) {
	c.Conn.SetWriteDeadline(time.Now().Add(dbIOTimeout))
	return c.Conn.Write(b)
}

type deadlineDialer struct{}

func (deadlineDialer) Dial(network, address string) (net.Conn, error) {
	return deadlineDialer{}.DialTimeout(network, address, dbIOTimeout)
}

func (deadlineDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}
	return deadlineConn{conn}, nil
}

// NewDB builds the pool without touching the network.
func NewDB(conn string) (*sql.DB, error) {
	connector, err := pq.NewConnector(conn)
	if err != nil {
		return nil, err
	}
	connector.Dialer(deadlineDialer{})

	db := sql.OpenDB(connector)
	// Bounded pool: a spike queues here instead of exhausting Postgres connections.
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	return db, nil
}

// OpenDB waits briefly for the database. sql.DB reconnects by itself later, so a
// slow start only delays the first query.
func OpenDB(conn string) (*sql.DB, error) {
	db, err := NewDB(conn)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 5; i++ {
		if DBUp(context.Background(), db) {
			break
		}
		time.Sleep(time.Second)
	}
	return db, nil
}

// DBUp is a bounded ping: a hung database must not hang the caller with it.
func DBUp(ctx context.Context, db *sql.DB) bool {
	if db == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return db.PingContext(ctx) == nil
}
