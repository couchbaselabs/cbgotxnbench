package main

import (
	"log"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v9"

	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

func main() {
	cluster, err := gocb.Connect("couchbase://192.168.0.249?kv_pool_size=8&max_queue_size=100000", gocb.ClusterOptions{
		Username: "Administrator",
		Password: "password",
	})
	assertNoError(err, "failed to connect to cluster")

	bucket := cluster.Bucket("default")
	//collection := bucket.DefaultCollection()

	err = bucket.WaitUntilReady(15*time.Second, nil)
	assertNoError(err, "failed to ready bucket")

	agent, err := bucket.Internal().IORouter()
	assertNoError(err, "failed to get agent")
	if err != nil {
		log.Fatalf("failed to get agent: %v", err)
	}

	config := &coretxns.Config{
		BucketAgentProvider: func(bucketName string) (*gocbcore.Agent, error) {
			return agent, nil
		},
	}

	config.DurabilityLevel = coretxns.DurabilityLevelNone
	config.Internal.EnableMutationCaching = true

	transactions, err := coretxns.Init(config)
	assertNoError(err, "failed to initialize transactions")

	runAllTests(agent, transactions)
}

func runAllTests(agent *gocbcore.Agent, transactions *coretxns.Manager) {
	threadCount := 200
	keysPerThread := 5
	useOpti := true
	useAsync := true

	neededKeys := keyList(genKeys(0, threadCount*keysPerThread), "kC")
	removeKeys(agent, neededKeys)
	createKeys(agent, neededKeys)

	stats, _ := runTests(agent, transactions, testDef{
		Transactions: genGetRepTxns(threadCount, keysPerThread, useOpti, useAsync),
		Duration:     15 * time.Second,
	})
	log.Printf("test stats: %s", stats.String())
}
