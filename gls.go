package gls

import (
	"database/sql"
	_ "github.com/bmizerany/pq"
	"io"
	"sync"
)

type LockstepServer struct {
	db     *sql.DB
	tables map[string]*pgTable
	loaded bool
	mu     sync.Mutex // Protects tables + loaded
}

func (l *LockstepServer) Stream(w io.Writer, tableName string) error {
	stopc := make(chan bool)
	defer close(stopc)

	// ignore errc for now
	rs, err := l.Query(tableName, stopc)
	if err != nil {
		return err
	}
	for s := range rs.Results {
		_, err = w.Write([]byte(s["name"].(string)))
		if err != nil {
			stopc <- true
			return err
		}
	}
	return nil
}
