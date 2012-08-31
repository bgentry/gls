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
	mu     sync.Mutex // Protects types + loaded
}

type ResultSet struct {
	Results chan map[string]interface{}
	Errors chan error
}

func (rs *ResultSet) Close() {
	close(rs.Results)
	close(rs.Errors)
}

// Query returns a channel of results, a channel an errors
func (l *LockstepServer) Query(tableName string, stopc chan bool) (rs *ResultSet, err error) {
	if !l.loaded {
		// Table/view names are not loaded yet
		err = l.loadTables()
		if err != nil {
			return nil, err
		}
	}
	if l.tables[tableName] == nil {
		return nil, fmt.Errorf("invalid tableName: %q", tableName)
	}
	t := l.tables[tableName]

	rs = &ResultSet{make(chan map[string]interface{}), make(chan error, 10)}

	go t.lockstepQuery(rs, stopc)
	return rs, nil
}

func (t *pgTable) lockstepQuery(rs *ResultSet, stopc chan bool) {
	defer rs.Close()

	if !t.loaded {
		// Table schema is not loaded, we need it before we can query
		err := t.loadSchema()
		if err != nil {
			// no good way to send this error back?
			return
		}
	}

	rows, err := t.parent.db.Query(fmt.Sprintf("SELECT * FROM %s", t.name))
	if err != nil {
		// no good way to send this error back?
		fmt.Printf("Error starting lockstep query: %v\n", err)
		return
	}
	defer rows.Close()

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
		// If we've received a stop request, stop running
		select {
		case <-stopc:
			return
		default:
			rs.Results <- res
		}
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
