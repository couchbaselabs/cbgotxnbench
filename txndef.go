package main

import (
	"log"
	"time"

	"github.com/couchbase/gocbcore/v9"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

type txnDef struct {
	SetupKeys []string
	Ops       [][]testOp
	NoCommit  bool
	SleepTime time.Duration
}

func runTxn(agent *gocbcore.Agent, transactions *coretxns.Manager, deadline time.Time, name string, def txnDef) (stageStats, error) {
	stime := time.Now()

	txn, err := transactions.BeginTransaction(&coretxns.PerTransactionConfig{})
	assertNoError(err, "failed to start transaction")

	tr := &transactionRunner{
		storedResults: make(map[string]*coretxns.GetResult),
		txn:           txn,
		agent:         agent,
	}

	var numAttempts int
	var numOps int
	var lastOpError error
	var lastError error

	for {
		numAttempts++

		err := txn.NewAttempt()
		assertNoError(err, "failed to start attempt")

		shouldCommit := !def.NoCommit
		for _, opBatch := range def.Ops {
			numOps += len(opBatch)

			err := tr.RunOps(opBatch)
			if err != nil {
				lastError = err
				lastOpError = err
				shouldCommit = false
				break
			}
		}

		if txn.CanCommit() && shouldCommit {
			err := tr.Commit()
			if err != nil {
				lastError = err
			} else {
				lastError = nil
				lastOpError = nil
				break
			}
		} else if txn.ShouldRollback() {
			err := tr.Rollback()
			if err != nil {
				lastError = err
			}
		}

		if !txn.ShouldRetry() {
			break
		}
	}

	etime := time.Now()
	dtime := etime.Sub(stime)

	stats := stageStats{IsFilled: true}
	stats.NumOps = numOps
	stats.SumTime = dtime
	stats.SumRealTime = dtime
	stats.MinTime = dtime
	stats.MaxTime = dtime
	stats.NumAttempts = numAttempts
	if lastError != nil {
		stats.NumCommitError = 1
	} else {
		stats.NumCommitSuccess = 1
	}

	if lastError != nil {
		log.Printf("txn %s failed:\n  Last Error: %+v\n  Last Op Error: %+v", name, lastError, lastOpError)
	}

	//log.Printf("transaction %s stats: %s", name, stats.String())

	return stats, nil
}
