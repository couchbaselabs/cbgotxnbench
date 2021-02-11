package main

import (
	"fmt"
	"log"
	"time"

	"github.com/couchbase/gocbcore/v9"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

type testDef struct {
	Transactions []txnDef
	Duration     time.Duration
}

func runTests(agent *gocbcore.Agent, transactions *coretxns.Manager, def testDef) (stageStats, error) {
	deadline := time.Now().Add(def.Duration)

	stime := time.Now()

	testStats := stageStats{}

	waitCh := make(chan stageStats, len(def.Transactions))
	for i, def := range def.Transactions {
		i := i
		def := def

		go func() {
			log.Printf("starting worker %d", i)

			wstime := time.Now()

			wrkrStats := stageStats{}

			for j := 0; ; j++ {
				txnName := fmt.Sprintf("%d-%d", i, j)

				txnStats, err := runTxn(agent, transactions, deadline, txnName, def)
				wrkrStats = wrkrStats.Add(txnStats)

				if err != nil {
					log.Printf("txn run failed: %v", err)
					break
				}

				if !time.Now().Add(def.SleepTime).Before(deadline) {
					break
				}

				time.Sleep(def.SleepTime)
			}

			wetime := time.Now()
			wdtime := wetime.Sub(wstime)
			wrkrStats.SumRealTime = wdtime

			waitCh <- wrkrStats
		}()
	}
	for i := range def.Transactions {
		wrkrStats := <-waitCh
		testStats = testStats.Add(wrkrStats)

		log.Printf("worker %d stats: %s", i, wrkrStats.String())
	}

	etime := time.Now()
	dtime := etime.Sub(stime)
	testStats.SumRealTime = dtime

	return testStats, nil
}

func genGetRepTxn(keys []string, useOpti bool, useAsync bool) txnDef {
	return txnDef{
		Ops: batchList(
			genGetRepBatches(keys, useOpti, useAsync),
		),
	}
}

func genGetRepTxns(numThreads, keysPerThread int, useOpti bool, useAsync bool) []txnDef {
	var defs []txnDef
	for i := 0; i < numThreads; i++ {
		defs = append(defs, txnDef{
			Ops: batchList(
				genGetRepBatches(genKeys(i*keysPerThread, keysPerThread), useOpti, useAsync),
			),
		})
	}
	return defs
}
