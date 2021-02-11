package main

import (
	"fmt"
	"log"
)

type testOp interface {
	ImATestOp()
}

type getOp struct {
	Key      string
	SaveCode string
}

type insertOp struct {
	Key string
}

type replaceOp struct {
	GetCode  string
	SaveCode string
}

type removeOp struct {
	GetCode string
}

func (op getOp) ImATestOp()     {}
func (op insertOp) ImATestOp()  {}
func (op replaceOp) ImATestOp() {}
func (op removeOp) ImATestOp()  {}

func opList(a ...interface{}) []testOp {
	out := []testOp{}
	for _, z := range a {
		switch z := z.(type) {
		case testOp:
			out = append(out, z)
		case []getOp:
			for _, op := range z {
				out = append(out, op)
			}
		case []insertOp:
			for _, op := range z {
				out = append(out, op)
			}
		case []replaceOp:
			for _, op := range z {
				out = append(out, op)
			}
		case []removeOp:
			for _, op := range z {
				out = append(out, op)
			}
		case []testOp:
			out = append(out, z...)
		default:
			log.Fatalf("unexpected value in ops list %v", z)
		}
	}
	return out
}

func batchList(a ...interface{}) [][]testOp {
	out := [][]testOp{}
	for _, z := range a {
		switch tZ := z.(type) {
		case []testOp:
			out = append(out, tZ)
		case [][]testOp:
			out = append(out, tZ...)
		default:
			log.Fatalf("unexpected value in batch list %v", z)
		}
	}
	return out
}

func genKeys(start, num int) []string {
	keys := make([]string, num)
	for i := 0; i < num; i++ {
		keys[i] = fmt.Sprintf("k%04d", start+i)
	}
	return keys
}

func genGetOps(keys []string) []testOp {
	ops := make([]testOp, len(keys))
	for i, key := range keys {
		ops[i] = getOp{
			Key:      key,
			SaveCode: key,
		}
	}
	return ops
}

func genRepOps(keys []string) []testOp {
	ops := make([]testOp, len(keys))
	for i, key := range keys {
		ops[i] = replaceOp{
			GetCode: key,
		}
	}
	return ops
}

func genGetRepBatches(keys []string, useOpti bool, useAsync bool) [][]testOp {
	batches := batchList()

	if useOpti {
		if useAsync {
			batches = append(batches, opList(genGetOps(keys)))
			batches = append(batches, opList(genRepOps(keys)))
		} else {
			for _, key := range keys {
				batches = append(batches, opList(getOp{
					Key:      key,
					SaveCode: key,
				}))
			}
			for _, key := range keys {
				batches = append(batches, opList(replaceOp{
					GetCode: key,
				}))
			}
		}
	} else {
		if useAsync {
			panic("invalid configuration of async/opti")
		}

		for _, key := range keys {
			batches = append(batches, opList(getOp{
				Key:      key,
				SaveCode: key,
			}))
			batches = append(batches, opList(replaceOp{
				GetCode: key,
			}))
		}
	}

	return batches
}
