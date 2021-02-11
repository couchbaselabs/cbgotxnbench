package main

import (
	"log"

	"github.com/couchbase/gocbcore/v9"
)

func removeKeys(agent *gocbcore.Agent, keys []string) error {
	waitCh := make(chan error, len(keys))
	for _, key := range keys {
		key := key

		agent.Delete(gocbcore.DeleteOptions{
			ScopeName:      "_default",
			CollectionName: "_default",
			Key:            []byte(key),
		}, func(result *gocbcore.DeleteResult, err error) {
			if err != nil {
				waitCh <- err
				return
			}

			waitCh <- nil
		})
	}
	for range keys {
		<-waitCh
	}

	return nil
}

func createKeys(agent *gocbcore.Agent, keys []string) error {
	waitCh := make(chan error, len(keys))
	for _, key := range keys {
		key := key

		agent.Add(gocbcore.AddOptions{
			ScopeName:      "_default",
			CollectionName: "_default",
			Key:            []byte(key),
			Value:          []byte(`{"v":0}`),
		}, func(result *gocbcore.StoreResult, err error) {
			if err != nil {
				waitCh <- err
				return
			}

			waitCh <- nil
		})
	}
	for range keys {
		err := <-waitCh
		if err != nil {
			log.Fatalf("create key failed: %v", err)
		}
	}

	return nil
}
