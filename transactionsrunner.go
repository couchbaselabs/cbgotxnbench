package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/couchbase/gocbcore/v9"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

type transactionRunner struct {
	lock          sync.Mutex
	storedResults map[string]*coretxns.GetResult
	txn           *coretxns.Transaction
	agent         *gocbcore.Agent
}

func (tr *transactionRunner) execGetAsync(op getOp, cb func(error)) {
	err := tr.txn.Get(coretxns.GetOptions{
		Agent:          tr.agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            []byte(op.Key),
	}, func(result *coretxns.GetResult, err error) {
		if err != nil {
			cb(err)
			return
		}

		if op.SaveCode != "" {
			tr.lock.Lock()
			tr.storedResults[op.SaveCode] = result
			tr.lock.Unlock()
		}

		cb(nil)
	})
	if err != nil {
		cb(err)
	}
}

func (tr *transactionRunner) execInsertAsync(op insertOp, cb func(error)) {
	err := tr.txn.Insert(coretxns.InsertOptions{
		Agent:          tr.agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            []byte(op.Key),
		Value:          []byte(`{"v":0}`),
	}, func(result *coretxns.GetResult, err error) {
		if err != nil {
			cb(err)
			return
		}

		cb(nil)
	})
	if err != nil {
		cb(err)
	}
}

func (tr *transactionRunner) execReplaceAsync(op replaceOp, cb func(error)) {
	tr.lock.Lock()
	getRes := tr.storedResults[op.GetCode]
	tr.lock.Unlock()

	var docVal struct {
		Value int `json:"v"`
	}
	err := json.Unmarshal(getRes.Value, &docVal)
	if err != nil {
		cb(err)
		return
	}

	docVal.Value++

	newBytes, err := json.Marshal(docVal)
	if err != nil {
		cb(err)
		return
	}

	err = tr.txn.Replace(coretxns.ReplaceOptions{
		Document: getRes,
		Value:    newBytes,
	}, func(result *coretxns.GetResult, err error) {
		if err != nil {
			cb(err)
			return
		}

		if op.SaveCode != "" {
			tr.lock.Lock()
			tr.storedResults[op.SaveCode] = result
			tr.lock.Unlock()
		}

		cb(nil)
	})
	if err != nil {
		cb(err)
	}
}

func (tr *transactionRunner) execRemoveAsync(op removeOp, cb func(error)) {
	tr.lock.Lock()
	getRes := tr.storedResults[op.GetCode]
	tr.lock.Unlock()

	err := tr.txn.Remove(coretxns.RemoveOptions{
		Document: getRes,
	}, func(result *coretxns.GetResult, err error) {
		if err != nil {
			cb(err)
			return
		}

		cb(nil)
	})
	if err != nil {
		cb(err)
	}
}

func (tr *transactionRunner) RunOps(ops []testOp) error {
	waitCh := make(chan error, len(ops))
	for _, op := range ops {
		op := op

		switch op := op.(type) {
		case getOp:
			tr.execGetAsync(op, func(err error) {
				waitCh <- err
			})
		case insertOp:
			tr.execInsertAsync(op, func(err error) {
				waitCh <- err
			})
		case replaceOp:
			tr.execReplaceAsync(op, func(err error) {
				waitCh <- err
			})
		case removeOp:
			tr.execRemoveAsync(op, func(err error) {
				waitCh <- err
			})
		default:
			log.Fatalf("invalid op type: %T", op)
		}
	}

	var errs []error
	for range ops {
		err := <-waitCh
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return aggregateError(errs)
	}

	return nil
}

func (tr *transactionRunner) Commit() error {
	waitCh := make(chan error, 1)
	err := tr.txn.Commit(func(err error) {
		waitCh <- err
	})
	if err != nil {
		return err
	}
	return <-waitCh
}

func (tr *transactionRunner) Rollback() error {
	waitCh := make(chan error, 1)
	err := tr.txn.Rollback(func(err error) {
		waitCh <- err
	})
	if err != nil {
		return err
	}
	return <-waitCh
}
