package gls

import (
	"fmt"
	"reflect"
	"time"
)

func newValueFor(k reflect.Type) interface{} {
	return reflect.New(k).Interface()
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
	case "time without time zone", "time with time zone", "timestamp without time zone", "timestamp with time zone":
		return reflect.TypeOf(&time.Time{})
	default:
		fmt.Printf("Unknown type: %s\n", pgtype)
	}
	return reflect.TypeOf(new(string))
}

func underlyingValue(t interface{}) interface{} {
	switch t.(type) {
	case **int64:
		return **(reflect.ValueOf(t).Interface().(**int64))
	case **string:
		return **(reflect.ValueOf(t).Interface().(**string))
	case **bool:
		return **(reflect.ValueOf(t).Interface().(**bool))
	}
	return t
}
