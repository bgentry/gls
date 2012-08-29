package gls

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"gls/logger"
	"reflect"
	"sort"
	"strings"
	"sync"
)

func OpenDB(connString string) (dbRef *sql.DB, err error) {
	dbRef, err = sql.Open("postgres", connString)
	if err != nil {
		logger.LogEvent("fatal", "database_connection_error", "error", err.Error())
	}
	return
}

type pgTable struct {
	parent *LockstepServer
	name   string
	types  map[string]reflect.Type
	loaded bool
	mu     sync.Mutex  // Protects types + loaded
}

func (l *LockstepServer) Query(tableName string, finished chan bool) (chan map[string]interface{}, error) {
	if !l.loaded {
		// Table/view names are not loaded yet
		err := l.loadTables()
		if err != nil {
			return nil, err
		}
	}
	if l.tables[tableName] == nil {
		return nil, fmt.Errorf("invalid tableName: %q", tableName)
	}
	t := l.tables[tableName]
	if !t.loaded {
		// Table schema is not loaded, we need it before we can query
		err := t.loadSchema()
		if err != nil {
			return nil, err
		}
	}

	rc := make(chan map[string]interface{})
	go l.query(t, rc)

	return rc, nil
}

func (l *LockstepServer) query(t *pgTable, c chan map[string]interface{}) {
	defer close(c)
	rows, err := t.parent.db.Query(fmt.Sprintf("SELECT * FROM %s", t.name))
	if err != nil {
		// no good way to send this error back?
		fmt.Printf("Error starting lockstep query: %v\n", err)
		return
	}

	// Figure out columns in response
	cols, _ := rows.Columns()
	for i, _ := range cols {
		cols[i] = strings.ToLower(cols[i])
	}

	res := make(map[string]interface{}, len(cols))
	var fargs []interface{}

	for _, name := range cols {
		// TODO: need to make sure that the column is defined in the types map
		res[name] = newValueFor(t.types[name])
		fargs = append(fargs, res[name])
	}

	for rows.Next() {
		err := rows.Scan(fargs...)
		if err != nil {
			// no good way to send this error back?
			fmt.Printf("Error in Scan: %v\n", err)
			return
		}
		for i, name := range cols {
			res[name] = underlyingValue(fargs[i])
		}
		c <- res
	}
}

func (l *LockstepServer) loadTables() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.loaded {
		return nil
	}

	l.tables = make(map[string]*pgTable)

	names, err := getTables(l.db)
	if err != nil {
		return err
	}
	for _, name := range names {
		l.tables[name] = &pgTable{parent: l, name: name}
	}
	return nil
}

func (t *pgTable) loadSchema() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	r, err := describeTable(t.parent.db, t.name)
	if err != nil {
		return err
	}
	t.types = r
	return nil
}

func getTables(db *sql.DB) ([]string, error) {
	var tables []string
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
		return nil, err
	}

	var table string
	for rows.Next() {
		rows.Scan(&table)
		tables = append(tables, table)
	}

	sort.Strings(tables)
	return tables, nil
}

func describeTable(db *sql.DB, name string) (map[string]reflect.Type, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'", name))
	if err != nil {
		return nil, err
	}

	types := make(map[string]reflect.Type)

	var colName, dtype string
	for rows.Next() {
		err := rows.Scan(&colName, &dtype)
		if err != nil {
			return nil, fmt.Errorf("Error describing table %s: %v", name, err.Error())
		}

		types[strings.ToLower(colName)] = getType(dtype)
	}

	return types, nil
}
