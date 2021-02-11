package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

type aggregateError []error

func (agge aggregateError) Error() string {
	var errStrs []string
	for _, err := range agge {
		errStrs = append(errStrs, err.Error())
	}
	return "manyErrors{ " + strings.Join(errStrs, ", ") + " }"
}

func assertNoError(err error, format string, a ...interface{}) {
	if err == nil {
		return
	}

	txt := fmt.Sprintf(format, a...)
	log.Fatalf("assertion failed (%s): %v", txt, err)
}

func keyList(a ...interface{}) []string {
	out := []string{}
	for _, z := range a {
		switch tZ := z.(type) {
		case string:
			out = append(out, tZ)
		case []string:
			out = append(out, tZ...)
		default:
			log.Fatalf("unexpected value in key list %v", z)
		}
	}
	return out
}

func marshalStringValue(val string) []byte {
	bytes, _ := json.Marshal(val)
	return bytes
}
