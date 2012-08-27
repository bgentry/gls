package gls

import (
	"reflect"
	"testing"
	"time"
)

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
	{"time without time zone", reflect.TypeOf(new(time.Time))},
	{"time with time zone", reflect.TypeOf(new(time.Time))},
	{"timestamp without time zone", reflect.TypeOf(new(time.Time))},
	{"timestamp with time zone", reflect.TypeOf(new(time.Time))},
}

func TestGetType(t *testing.T) {
	for i, tt := range typetests {
		if getType(tt.in) != tt.out {
			t.Errorf("%d. getType(%q) => %q, want %v", i, tt.in, getType(tt.in), tt.out)
		}
	}
}
