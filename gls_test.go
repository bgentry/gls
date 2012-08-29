package gls

import (
	"testing"
)

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

func TestStream(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)

	l := LockstepServer{db: db}

	w := testStringWriter{t, []string{"a.com", "b.com", "c.com"}, 0}
	err := l.Stream(&w, "domains_lockstep")
	if err != nil {
		t.Fatalf("Error from LockstepStream: %v", err.Error())
	}
	w.failUnlessN(3)
}

func TestStreamAfterDroppingColumn(t *testing.T) {
	db := handleTestDBPrep(t)
	defer teardownAndCloseDB(t, db)

	l := LockstepServer{db: db}

	w := testStringWriter{t, []string{"a.com", "b.com", "c.com"}, 0}
	err := l.Stream(&w, "domains")
	if err != nil {
		t.Fatalf("Error from LockstepStream: %v", err.Error())
	}
	w.failUnlessN(3)

	_, err = db.Exec("ALTER TABLE domains DROP COLUMN created_at")
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
