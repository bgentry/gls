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
	c, err := l.Query(tableName, stopc)
	if err != nil {
		return err
	}
	for s := range c {
		_, err = w.Write([]byte(s["name"].(string)))
		if err != nil {
			stopc <- true
			return err
		}
	}
	return nil
}
