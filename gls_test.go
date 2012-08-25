package gls

import (
	"database/sql"
	"github.com/bmizerany/pq"
	"reflect"
	"testing"
	"time"
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

var testSchema = `
CREATE TABLE domains (
    name text,
    deleted boolean,
    txid bigint DEFAULT txid_current()
);`
var testData = `INSERT into domains ( name, deleted, txid ) VALUES
 ('a.com', 'f', 0),
 ('b.com', 'f', 1),
 ('c.com', 'f', 2);
`

func loadTestData(db *sql.DB) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(testSchema)
	if err != nil {
		return
	}
	_, err = tx.Exec(testData)
	if err != nil {
		return
	}
	return tx.Commit()
}

func teardownTestDB(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE domains")
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

type testStringWriter struct {
	T        *testing.T
	Expected []string
	n        int
}

func (w *testStringWriter) Write(b []byte) (int, error) {
	if w.Expected[w.n] != string(b) {
		w.T.Errorf("expected to Write: %q, received: %q", w.Expected[w.n], string(b))
	}
	w.n += 1
	return len(b), nil
}

func (w *testStringWriter) failUnlessN(count int) {
	if w.n != count {
		w.T.Errorf("expected to Write %d times, received %d", count, w.n)
	}
}

func TestGetTable(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)

	tables, err := getTables(db)
	if err != nil {
		t.Errorf("Error from getTables: %v", err.Error())
	}
	expected := []string{"domains"}

	if len(expected) != len(tables) {
		t.Fatalf("getTables length mismatch, expected: %i, got: %i", len(expected), len(tables))
	}

	for i, c := range expected {
		if tables[i] != c {
			t.Errorf("getTables expected index %i: %s, got: %s", i, c, tables[i])
		}
	}
}

var typetests = []struct {
	in  string
	out reflect.Type
}{
	{"character", reflect.TypeOf(new(string))},
	{"character varying", reflect.TypeOf(new(string))},
	{"text", reflect.TypeOf(new(string))},
	{"smallint", reflect.TypeOf(new(int64))},
	{"integer", reflect.TypeOf(new(int64))},
	{"bigint", reflect.TypeOf(new(int64))},
	{"serial", reflect.TypeOf(new(int64))},
	{"bigserial", reflect.TypeOf(new(int64))},
	{"boolean", reflect.TypeOf(new(bool))},
	{"time", reflect.TypeOf(new(time.Time))},
	{"timetz", reflect.TypeOf(new(time.Time))},
	{"timestamp", reflect.TypeOf(new(time.Time))},
	{"timestamptz", reflect.TypeOf(new(time.Time))},
}

func TestGetType(t *testing.T) {
	for i, tt := range typetests {
		if getType(tt.in) != tt.out {
			t.Errorf("%d. getType(%q) => %q, want %v", i, tt.in, getType(tt.in), tt.out)
		}
	}
}

var columntypetests = []struct {
	name     string
	datatype reflect.Type
}{
	{"name", reflect.TypeOf(new(string))},
	{"txid", reflect.TypeOf(new(int64))},
	{"deleted", reflect.TypeOf(new(bool))},
	{"character_octet_length", reflect.TypeOf(new(int64))},
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
		for i, tt := range columntypetests {
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
	if len(l.tables) != 1 {
		t.Fatalf("loadTables(), len(l.tables) => %d, want 1", len(l.tables))
	}
	expected := []string{"domains"}
	i := 0
	for name, _ := range l.tables {
		if expected[i] != name {
			t.Errorf("loadTables()[%d] => %q, want %q", i, name, expected[i])
		}
		i++
	}
}

func TestStream(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)

	l := LockstepServer{db: db}
	l.loadTables()

	w := testStringWriter{t, []string{"a.com", "b.com", "c.com"}, 0}
	err := l.Stream(&w, "domains")
	if err != nil {
		t.Fatalf("Error from LockstepStream: %v", err.Error())
	}
	w.failUnlessN(3)
}

func TestStreamAfterDroppingColumn(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)

	l := LockstepServer{db: db}
	l.loadTables()

	w := testStringWriter{t, []string{"a.com", "b.com", "c.com"}, 0}
	err := l.Stream(&w, "domains")
	if err != nil {
		t.Fatalf("Error from LockstepStream: %v", err.Error())
	}
	w.failUnlessN(3)

	_, err = db.Exec("ALTER TABLE domains DROP COLUMN deleted")
	if err != nil {
		t.Fatalf("Error dropping column: %v", err.Error())
	}

	w = testStringWriter{t, []string{"a.com", "b.com", "c.com"}, 0}
	err = l.Stream(&w, "domains")
	if err != nil {
		t.Fatalf("Error from LockstepStream: %v", err.Error())
	}
	w.failUnlessN(3)
}
