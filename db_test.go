package gls

import (
	"database/sql"
	"github.com/bmizerany/pq"
	"reflect"
	"testing"
	"time"
	"strings"
)

func openTestDB(t *testing.T) *sql.DB {
	cs, err := pq.ParseURL("postgres://localhost:5432/gls_test")
	if err != nil {
		t.Fatal(err)
	}
	cs += " sslmode=disable"
	db, err := OpenDB(cs)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

var sqlCreateTable = `
CREATE TABLE domains (
  name text,
  deleted boolean,
	created_at timestamptz,
  txid bigint DEFAULT txid_current()
);`
var sqlCreateView = `
CREATE OR REPLACE VIEW domains_lockstep AS SELECT txid_snapshot_xmin(txid_current_snapshot()) AS current_xmin, name, deleted, txid FROM domains
`
var sqlLoadData = `INSERT into domains ( name, deleted, txid, created_at ) VALUES
 ('a.com', 'f', 0, '2012-08-27 15:04:23-07'),
 ('b.com', 'f', 1, NULL),
 ('c.com', 'f', 2, NULL);
`
var sqlDropGeneratedView = `DROP VIEW IF EXISTS generated_series CASCADE`

func loadTestData(db *sql.DB) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(sqlCreateTable)
	if err != nil {
		return
	}
	_, err = tx.Exec(sqlCreateView)
	if err != nil {
		return
	}
	_, err = tx.Exec(sqlLoadData)
	if err != nil {
		return
	}
	return tx.Commit()
}

func teardownTestDB(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE domains CASCADE")
	if err != nil {
		return err
	}

	_, err = db.Exec(sqlDropGeneratedView)
	return err
}

func handleTestDBPrep(t *testing.T) *sql.DB {
	db := openTestDB(t)
	err := loadTestData(db)
	if err != nil {
		t.Errorf("DB Prep failed: %v", err.Error())
	}
	return db
}

func teardownAndCloseDB(t *testing.T, db *sql.DB) {
	err := teardownTestDB(db)
	if err != nil {
		t.Fatalf("DB teardown failed: %v", err.Error())
	}
	db.Close()
	return
}

func TestSelect(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	var i int
	r := db.QueryRow("SELECT 1")
	err := r.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("SELECT 1 expected: 1, got: %v", i)
	}
}

func TestPrep(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	err := loadTestData(db)
	if err != nil {
		t.Errorf("DB Prep failed: %v", err.Error())
	}
	err = teardownTestDB(db)
	if err != nil {
		t.Fatalf("DB teardown failed: %v", err.Error())
	}
}

func TestGetTable(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)

	tables, err := getTables(db)
	if err != nil {
		t.Errorf("Error from getTables: %v", err.Error())
	}
	expected := []string{"domains", "domains_lockstep"}

	if len(expected) != len(tables) {
		t.Fatalf("getTables length mismatch, expected: %i, got: %i", len(expected), len(tables))
	}

	for i, c := range expected {
		if tables[i] != c {
			t.Errorf("getTables expected index %i: %s, got: %s", i, c, tables[i])
		}
	}
}

var domainscolumns = []struct {
	name     string
	datatype reflect.Type
}{
	{"name", reflect.TypeOf(new(string))},
	{"txid", reflect.TypeOf(new(int64))},
	{"deleted", reflect.TypeOf(new(bool))},
}

func TestDescribeTable(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)

	tables, _ := getTables(db)
	for _, table := range tables {
		data, err := describeTable(db, table)
		if err != nil {
			t.Fatalf("Error from describeTable: %v", err.Error())
		}
		for i, tt := range domainscolumns {
			if data[tt.name] != tt.datatype {
				t.Errorf("%d. describeTable(db, %q)[%q] => %q, want %q", i, table, tt.name, data[tt.name], tt.datatype)
			}
		}
	}
}

func TestLoadTables(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)

	l := LockstepServer{db: db}
	l.loadTables()
	if len(l.tables) != 2 {
		t.Fatalf("loadTables(), len(l.tables) => %d, want 2", len(l.tables))
	}
	expected := []string{"domains", "domains_lockstep"}
	for name, _ := range l.tables {
		success := false
		for _, n2 := range expected {
			if name == n2 {
				success = true
			}
		}
		if !success {
			t.Errorf("loadTables() => %q, not included in list", name)
		}
	}
}

var sqlCreateGeneratedView = `
CREATE OR REPLACE VIEW generated_series AS
  SELECT * FROM generate_series(0, 10) AS id;
`

func TestLockstepQueryStop(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)
	_, err := db.Exec(sqlCreateGeneratedView)
	if err != nil {
		t.Fatalf("Error creating generated view: %v", err)
	}

	l := LockstepServer{db: db}
	l.loadTables()

	rs := &ResultSet{make(chan map[string]interface{}), make(chan error)}
	stopc := make(chan bool)
	defer close(stopc)

	go l.tables["generated_series"].lockstepQuery(rs, stopc)
	stopc <- true

	count := 0
	for {
		select {
		case <-rs.Results:
			count++
			if count > 1 {
				t.Errorf("Received too many results, query did not close")
				return
			}
		default:
			return
		}
	}
}

func TestLockstepQueryError(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)
	_, err := db.Exec(sqlCreateGeneratedView)
	if err != nil {
		t.Fatalf("Error creating generated view: %v", err)
	}

	l := LockstepServer{db: db}
	l.loadTables()

	// Drop the view before trying to query it, ensuring an error
	_, err = db.Exec(sqlDropGeneratedView)
	if err != nil {
		t.Fatalf("Error dropping generated_series view: %v", err)
	}

	rs := &ResultSet{make(chan map[string]interface{}), make(chan error)}
	stopc := make(chan bool)
	defer close(stopc)

	go l.tables["generated_series"].lockstepQuery(rs, stopc)

	select {
	case e := <-rs.Errors:
		if !strings.Contains(e.Error(), "does not exist") {
			t.Errorf("lockstepQuery returned an unexpected error: %v", e)
		}
	case r := <-rs.Results:
		t.Errorf("lockstepQuery() expected an error, returned a result: %v", r)
	case <-time.After(100e6):
		t.Errorf("Error expected, none received")
	}
}
