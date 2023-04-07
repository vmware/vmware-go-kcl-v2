/*
 * Copyright (c) 2018 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

// Package worker
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package worker

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	chk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/checkpoint"
	kcl "github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/metrics"
)

const (
	kinesisReadTPSLimit = 5
	MaxBytes            = 10000000
	MaxBytesPerSecond   = 2000000
	BytesToMbConversion = 1000000
)

var (
	rateLimitTimeNow      = time.Now
	rateLimitTimeSince    = time.Since
	localTPSExceededError = errors.New("Error GetRecords TPS Exceeded")
	maxBytesExceededError = errors.New("Error GetRecords Max Bytes For Call Period Exceeded")
)

// PollingShardConsumer is responsible for polling data records from a (specified) shard.
// Note: PollingShardConsumer only deal with one shard.
type PollingShardConsumer struct {
	commonShardConsumer
	streamName    string
	stop          *chan struct{}
	consumerID    string
	mService      metrics.MonitoringService
	currTime      time.Time
	callsLeft     int
	remBytes      int
	lastCheckTime time.Time
	bytesRead     int
}

func (sc *PollingShardConsumer) getShardIterator() (*string, error) {
	startPosition, err := sc.getStartingPosition()
	if err != nil {
		return nil, err
	}

	shardIterArgs := &kinesis.GetShardIteratorInput{
		ShardId:                &sc.shard.ID,
		ShardIteratorType:      startPosition.Type,
		StartingSequenceNumber: startPosition.SequenceNumber,
		Timestamp:              startPosition.Timestamp,
		StreamName:             &sc.streamName,
	}

	iterResp, err := sc.kc.GetShardIterator(context.TODO(), shardIterArgs)
	if err != nil {
		return nil, err
	}

	return iterResp.ShardIterator, nil
}

// getRecords continuously poll one shard for data record
// Precondition: it currently has the lease on the shard.
func (sc *PollingShardConsumer) getRecords() error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer func() {
		// cancel renewLease()
		cancelFunc()
		sc.releaseLease(sc.shard.ID)
	}()

	log := sc.kclConfig.Logger

	// If the shard is child shard, need to wait until the parent finished.
	if err := sc.waitOnParentShard(); err != nil {
		// If parent shard has been deleted by Kinesis system already, just ignore the error.
		if err != chk.ErrSequenceIDNotFound {
			log.Errorf("Error in waiting for parent shard: %v to finish. Error: %+v", sc.shard.ParentShardId, err)
			return err
		}
	}

	shardIterator, err := sc.getShardIterator()
	if err != nil {
		log.Errorf("Unable to get shard iterator for %s: %v", sc.shard.ID, err)
		return err
	}

	// Start processing events and notify record processor on shard and starting checkpoint
	input := &kcl.InitializationInput{
		ShardId:                sc.shard.ID,
		ExtendedSequenceNumber: &kcl.ExtendedSequenceNumber{SequenceNumber: aws.String(sc.shard.GetCheckpoint())},
	}
	sc.recordProcessor.Initialize(input)

	recordCheckpointer := NewRecordProcessorCheckpoint(sc.shard, sc.checkpointer)
	retriedErrors := 0

	// define API call rate limit starting window
	sc.currTime = rateLimitTimeNow()
	sc.callsLeft = kinesisReadTPSLimit
	sc.bytesRead = 0
	sc.remBytes = MaxBytes

	// starting async lease renewal thread
	leaseRenewalErrChan := make(chan error, 1)
	go func() {
		leaseRenewalErrChan <- sc.renewLease(ctx)
	}()
	for {
		getRecordsStartTime := time.Now()

		log.Debugf("Trying to read %d record from iterator: %v", sc.kclConfig.MaxRecords, aws.ToString(shardIterator))

		// Get records from stream and retry as needed
		getRecordsArgs := &kinesis.GetRecordsInput{
			Limit:         aws.Int32(int32(sc.kclConfig.MaxRecords)),
			ShardIterator: shardIterator,
		}
		getResp, coolDownPeriod, err := sc.callGetRecordsAPI(getRecordsArgs)
		if err != nil {
			//aws-sdk-go-v2 https://github.com/aws/aws-sdk-go-v2/blob/main/CHANGELOG.md#error-handling
			var throughputExceededErr *types.ProvisionedThroughputExceededException
			var kmsThrottlingErr *types.KMSThrottlingException
			if errors.As(err, &throughputExceededErr) {
				retriedErrors++
				if retriedErrors > sc.kclConfig.MaxRetryCount {
					log.Errorf("message", "Throughput Exceeded Error: "+
						"reached max retry count getting records from shard",
						"shardId", sc.shard.ID,
						"retryCount", retriedErrors,
						"error", err)
					return err
				}
				// If there is insufficient provisioned throughput on the stream,
				// subsequent calls made within the next 1 second throw ProvisionedThroughputExceededException.
				// ref: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
				sc.waitASecond(sc.currTime)
				continue
			}
			if err == localTPSExceededError {
				log.Infof("localTPSExceededError so sleep for a second")
				sc.waitASecond(sc.currTime)
				continue
			}
			if err == maxBytesExceededError {
				log.Infof("maxBytesExceededError so sleep for %+v seconds", coolDownPeriod)
				time.Sleep(time.Duration(coolDownPeriod) * time.Second)
				continue
			}
			if errors.As(err, &kmsThrottlingErr) {
				log.Errorf("Error getting records from shard %v: %+v", sc.shard.ID, err)
				retriedErrors++
				// Greater than MaxRetryCount so we get the last retry
				if retriedErrors > sc.kclConfig.MaxRetryCount {
					log.Errorf("message", "KMS Throttling Error: "+
						"reached max retry count getting records from shard",
						"shardId", sc.shard.ID,
						"retryCount", retriedErrors,
						"error", err)
					return err
				}
				// exponential backoff
				// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff
				time.Sleep(time.Duration(math.Exp2(float64(retriedErrors))*100) * time.Millisecond)
				continue
			}
			log.Errorf("Error getting records from Kinesis that cannot be retried: %+v Request: %s", err, getRecordsArgs)
			return err
		}
		// reset the retry count after success
		retriedErrors = 0

		sc.processRecords(getRecordsStartTime, getResp.Records, getResp.MillisBehindLatest, recordCheckpointer)

		// The shard has been closed, so no new records can be read from it
		if getResp.NextShardIterator == nil {
			log.Infof("Shard %s closed", sc.shard.ID)
			shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.TERMINATE, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		}
		shardIterator = getResp.NextShardIterator

		// Idle between each read, the user is responsible for checkpoint the progress
		// This value is only used when no records are returned; if records are returned, it should immediately
		// retrieve the next set of records.
		if len(getResp.Records) == 0 && aws.ToInt64(getResp.MillisBehindLatest) < int64(sc.kclConfig.IdleTimeBetweenReadsInMillis) {
			time.Sleep(time.Duration(sc.kclConfig.IdleTimeBetweenReadsInMillis) * time.Millisecond)
		}

		select {
		case <-*sc.stop:
			shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.REQUESTED, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		case leaseRenewalErr := <-leaseRenewalErrChan:
			return leaseRenewalErr
		default:
		}
	}
}

func (sc *PollingShardConsumer) waitASecond(timePassed time.Time) {
	waitTime := time.Since(timePassed)
	if waitTime < time.Second {
		time.Sleep(time.Second - waitTime)
	}
}

func (sc *PollingShardConsumer) checkCoolOffPeriod() (int, error) {
	// Each shard can support up to a maximum total data read rate of 2 MB per second via GetRecords.
	// If a call to GetRecords returns 10 MB, subsequent calls made within the next 5 seconds throw an exception.
	// ref: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
	// check for overspending of byte budget from getRecords call
	currentTime := rateLimitTimeNow()
	secondsPassed := currentTime.Sub(sc.lastCheckTime).Seconds()
	sc.lastCheckTime = currentTime
	sc.remBytes += int(secondsPassed * MaxBytesPerSecond)

	if sc.remBytes > MaxBytes {
		sc.remBytes = MaxBytes
	}
	if sc.remBytes < 1 {
		// Wait until cool down period has passed to prevent ProvisionedThroughputExceededException
		coolDown := sc.bytesRead / MaxBytesPerSecond
		if sc.bytesRead%MaxBytesPerSecond > 0 {
			coolDown++
		}
		return coolDown, maxBytesExceededError
	} else {
		sc.remBytes -= sc.bytesRead
	}
	return 0, nil
}

func (sc *PollingShardConsumer) callGetRecordsAPI(gri *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, int, error) {
	if sc.bytesRead != 0 {
		coolDownPeriod, err := sc.checkCoolOffPeriod()
		if err != nil {
			return nil, coolDownPeriod, err
		}
	}
	// every new second, we get a fresh set of calls
	if rateLimitTimeSince(sc.currTime) > time.Second {
		sc.callsLeft = kinesisReadTPSLimit
		sc.currTime = rateLimitTimeNow()
	}

	if sc.callsLeft < 1 {
		return nil, 0, localTPSExceededError
	}
	getResp, err := sc.kc.GetRecords(context.TODO(), gri)
	sc.callsLeft--

	if err != nil {
		return getResp, 0, err
	}

	// Calculate size of records from read transaction
	sc.bytesRead = 0
	for _, record := range getResp.Records {
		sc.bytesRead += len(record.Data)
	}
	if sc.lastCheckTime.IsZero() {
		sc.lastCheckTime = rateLimitTimeNow()
	}

	return getResp, 0, err
}

func (sc *PollingShardConsumer) renewLease(ctx context.Context) error {
	renewDuration := time.Duration(sc.kclConfig.LeaseRefreshWaitTime) * time.Millisecond
	for {
		timer := time.NewTimer(renewDuration)
		select {
		case <-timer.C:
			log.Debugf("Refreshing lease on shard: %s for worker: %s", sc.shard.ID, sc.consumerID)
			err := sc.checkpointer.GetLease(sc.shard, sc.consumerID)
			if err != nil {
				// log and return error
				log.Errorf("Error in refreshing lease on shard: %s for worker: %s. Error: %+v",
					sc.shard.ID, sc.consumerID, err)
				return err
			}
			// log metric for renewed lease for worker
			sc.mService.LeaseRenewed(sc.shard.ID)
		case <-ctx.Done():
			// clean up timer resources
			if !timer.Stop() {
				<-timer.C
			}
			log.Debugf("renewLease was canceled")
			return nil
		}
	}
}
