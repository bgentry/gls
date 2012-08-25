package gls

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"gls/logger"
	"io"
	"reflect"
	"strings"
	"time"
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
}

type LockstepServer struct {
	db     *sql.DB
	tables map[string]*pgTable
}

func (l *LockstepServer) Stream(w io.Writer, tableName string) error {
	finished := make(chan bool)
	c, err := l.Query(tableName, finished)
	if err != nil {
		return err
	}
	for s := range c {
		_, err = w.Write([]byte(s["name"].(string)))
		if err != nil {
			finished <- true
			return err
		}
	}
	return nil
}

func (l *LockstepServer) Query(tableName string, finished chan bool) (chan map[string]interface{}, error) {
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

	c := make(chan map[string]interface{})
	go func(t *pgTable, c chan map[string]interface{}) {
		defer close(c)
		rows, err := t.startLockstepQuery()
		if err != nil {
			// no good way to send this error back?
			fmt.Printf("Error in startLockstepQuery: %v\n", err)
			return
		}

		// Figure out columns
		cols, _ := rows.Columns()
		for i, _ := range cols {
			cols[i] = strings.ToLower(cols[i])
		}

		res := make(map[string]interface{}, len(cols))
		var fargs []interface{}

		for _, name := range cols {
			if name == "txid_snapshot_xmin" {
				res[name] = new(int64)
				fargs = append(fargs, res[name])
			} else {
				res[name] = newValueFor(t.types[name])
				fargs = append(fargs, res[name])
			}
		}

		for rows.Next() {
			err := rows.Scan(fargs...)
			if err != nil {
				// no good way to send this error back?
				fmt.Printf("Error in Scan: %v\n", err)
				return
			}
			for i, name := range cols {
				switch fargs[i].(type) {
				case **int64:
					res[name] = **(reflect.ValueOf(fargs[i]).Interface().(**int64))
				case **string:
					res[name] = **(reflect.ValueOf(fargs[i]).Interface().(**string))
				case **bool:
					res[name] = **(reflect.ValueOf(fargs[i]).Interface().(**bool))
				}
			}
			c <- res //name
		}
	}(t, c)
	return c, nil
}

func newValueFor(k reflect.Type) interface{} {
	return reflect.New(k).Interface()
}

func (l *LockstepServer) loadTables() error {
	l.tables = make(map[string]*pgTable)

	names, err := getTables(l.db)
	if err != nil {
		return err
	}
	for _, name := range names {
		l.tables[name] = &pgTable{parent: l, name: name}
	}
	for _, t := range l.tables {
		err = t.loadSchema()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTable) loadSchema() error {
	r, err := describeTable(t.parent.db, t.name)
	if err != nil {
		return err
	}
	t.types = r
	return nil
}

func getTables(db *sql.DB) ([]string, error) {
	var tables []string
	var table string
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		rows.Scan(&table)
		tables = append(tables, table)
	}

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

func getType(pgtype string) reflect.Type {
	switch pgtype {
	case "character", "character varying", "text":
		return reflect.TypeOf(new(string))
	case "smallint", "integer", "bigint", "serial", "bigserial":
		return reflect.TypeOf(new(int64))
	case "boolean":
		return reflect.TypeOf(new(bool))
	// don't know how to deal w/ time..
	case "time", "timetz", "timestamp", "timestamptz":
		return reflect.TypeOf(&time.Time{})
	default:
		fmt.Printf("Unknown type: %s\n", pgtype)
	}
	return reflect.TypeOf(new(string))
}

func startLockstepQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	return db.Query(fmt.Sprintf("SELECT txid_snapshot_xmin(txid_current_snapshot()), * FROM %s", tableName))
}

func (t *pgTable) startLockstepQuery() (*sql.Rows, error) {
	return t.parent.db.Query(fmt.Sprintf("SELECT txid_snapshot_xmin(txid_current_snapshot()), * FROM %s", t.name))
}
