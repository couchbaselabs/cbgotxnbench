package main

import (
	"fmt"
	"strings"
	"time"
)

type stageStats struct {
	IsFilled bool

	NumOps int

	SumTime     time.Duration
	MinTime     time.Duration
	MaxTime     time.Duration
	SumRealTime time.Duration

	NumAttempts      int
	NumCommitSuccess int
	NumCommitError   int
}

func (ss stageStats) Add(ostats stageStats) stageStats {
	if !ss.IsFilled {
		return ostats
	}
	if !ostats.IsFilled {
		return ss
	}

	astats := stageStats{IsFilled: true}

	astats.NumOps = ss.NumOps + ostats.NumOps

	astats.SumTime = ss.SumTime + ostats.SumTime
	if ostats.MinTime < ss.MinTime {
		astats.MinTime = ostats.MinTime
	} else {
		astats.MinTime = ss.MinTime
	}
	if ostats.MaxTime > ss.MaxTime {
		astats.MaxTime = ostats.MaxTime
	} else {
		astats.MaxTime = ss.MaxTime
	}

	astats.NumAttempts = ss.NumAttempts + ostats.NumAttempts
	astats.NumCommitSuccess = ss.NumCommitSuccess + ostats.NumCommitSuccess
	astats.NumCommitError = ss.NumCommitError + ostats.NumCommitError

	return astats
}

func (ss *stageStats) String() string {
	if !ss.IsFilled {
		return "--invalid--"
	}

	elapsedMs := float64(ss.SumTime) / float64(time.Millisecond)
	realElapsedSecs := float64(ss.SumRealTime) / float64(time.Second)

	outStrs := make([]string, 0)
	outStrs = append(outStrs, fmt.Sprintf("fail-rate:%.2f%%", float64(ss.NumCommitError)/float64(ss.NumCommitError+ss.NumCommitSuccess)*100))
	outStrs = append(outStrs, fmt.Sprintf("tmin:%dms", ss.MinTime/time.Millisecond))
	outStrs = append(outStrs, fmt.Sprintf("tmax:%dms", ss.MaxTime/time.Millisecond))
	outStrs = append(outStrs, fmt.Sprintf("tavg:%.2fms", elapsedMs/float64(ss.NumCommitSuccess)))
	outStrs = append(outStrs, fmt.Sprintf("wps:%.2f", float64(ss.NumOps)/realElapsedSecs))
	outStrs = append(outStrs, fmt.Sprintf("aps:%.2f", float64(ss.NumAttempts)/realElapsedSecs))
	outStrs = append(outStrs, fmt.Sprintf("tps:%.2f", float64(ss.NumCommitSuccess)/realElapsedSecs))
	return strings.Join(outStrs, ", ")
}
