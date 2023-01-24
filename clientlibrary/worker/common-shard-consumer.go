/*
 * Copyright (c) 2021 VMware, Inc.
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
package worker

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	deagg "github.com/awslabs/kinesis-aggregation/go/v2/deaggregator"

	chk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/checkpoint"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	kcl "github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/metrics"
	par "github.com/vmware/vmware-go-kcl-v2/clientlibrary/partition"
)

type shardConsumer interface {
	getRecords() error
}

type KinesisSubscriberGetter interface {
	SubscribeToShard(ctx context.Context, params *kinesis.SubscribeToShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SubscribeToShardOutput, error)
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
}

// commonShardConsumer implements common functionality for regular and enhanced fan-out consumers
type commonShardConsumer struct {
	shard           *par.ShardStatus
	kc              KinesisSubscriberGetter
	checkpointer    chk.Checkpointer
	recordProcessor kcl.IRecordProcessor
	kclConfig       *config.KinesisClientLibConfiguration
	mService        metrics.MonitoringService
}

// Cleanup the internal lease cache
func (sc *commonShardConsumer) releaseLease(shard string) {
	log := sc.kclConfig.Logger
	log.Infof("Release lease for shard %s", sc.shard.ID)
	sc.shard.SetLeaseOwner("")

	// Release the lease by wiping out the lease owner for the shard
	// Note: we don't need to do anything in case of error here and shard lease will eventually be expired.
	if err := sc.checkpointer.RemoveLeaseOwner(sc.shard.ID); err != nil {
		log.Errorf("Failed to release shard lease or shard: %s Error: %+v", sc.shard.ID, err)
	}

	// reporting lease lose metrics
	sc.mService.DeleteMetricMillisBehindLatest(shard)
	sc.mService.LeaseLost(sc.shard.ID)
}

// getStartingPosition gets kinesis stating position.
// First try to fetch checkpoint. If checkpoint is not found use InitialPositionInStream
func (sc *commonShardConsumer) getStartingPosition() (*types.StartingPosition, error) {
	err := sc.checkpointer.FetchCheckpoint(sc.shard)
	if err != nil && err != chk.ErrSequenceIDNotFound {
		return nil, err
	}

	checkpoint := sc.shard.GetCheckpoint()
	if checkpoint != "" {
		sc.kclConfig.Logger.Debugf("Start shard: %v at checkpoint: %v", sc.shard.ID, checkpoint)
		return &types.StartingPosition{
			Type:           types.ShardIteratorTypeAfterSequenceNumber,
			SequenceNumber: &checkpoint,
		}, nil
	}

	shardIteratorType := config.InitalPositionInStreamToShardIteratorType(sc.kclConfig.InitialPositionInStream)
	sc.kclConfig.Logger.Debugf("No checkpoint recorded for shard: %v, starting with: %v", sc.shard.ID, aws.ToString(shardIteratorType))
	if sc.kclConfig.InitialPositionInStream == config.AT_TIMESTAMP {
		return &types.StartingPosition{
			Type:      types.ShardIteratorTypeAtTimestamp,
			Timestamp: sc.kclConfig.InitialPositionInStreamExtended.Timestamp,
		}, nil
	}

	if *shardIteratorType == "TRIM_HORIZON" {
		return &types.StartingPosition{
			Type: types.ShardIteratorTypeTrimHorizon,
		}, nil
	}

	return &types.StartingPosition{
		Type: types.ShardIteratorTypeLatest,
	}, nil
}

// Need to wait until the parent shard finished
func (sc *commonShardConsumer) waitOnParentShard() error {
	if len(sc.shard.ParentShardId) == 0 {
		return nil
	}

	pshard := &par.ShardStatus{
		ID:  sc.shard.ParentShardId,
		Mux: &sync.RWMutex{},
	}

	for {
		if err := sc.checkpointer.FetchCheckpoint(pshard); err != nil {
			return err
		}

		// Parent shard is finished.
		if pshard.GetCheckpoint() == chk.ShardEnd {
			return nil
		}

		time.Sleep(time.Duration(sc.kclConfig.ParentShardPollIntervalMillis) * time.Millisecond)
	}
}

func (sc *commonShardConsumer) processRecords(getRecordsStartTime time.Time, records []types.Record, millisBehindLatest *int64, recordCheckpointer kcl.IRecordProcessorCheckpointer) {
	log := sc.kclConfig.Logger

	getRecordsTime := time.Since(getRecordsStartTime).Milliseconds()
	sc.mService.RecordGetRecordsTime(sc.shard.ID, float64(getRecordsTime))

	log.Debugf("Received %d original records.", len(records))

	// De-aggregate the records if they were published by the KPL.
	dars, err := deagg.DeaggregateRecords(records)
	if err != nil {
		// The error is caused by bad KPL publisher and just skip the bad records
		// instead of being stuck here.
		log.Errorf("Error in de-aggregating KPL records: %+v", err)
	}

	input := &kcl.ProcessRecordsInput{
		Records:            dars,
		MillisBehindLatest: *millisBehindLatest,
		Checkpointer:       recordCheckpointer,
	}

	recordLength := len(input.Records)
	recordBytes := int64(0)
	log.Debugf("Received %d de-aggregated records, MillisBehindLatest: %v", recordLength, input.MillisBehindLatest)

	for _, r := range input.Records {
		recordBytes += int64(len(r.Data))
	}

	if recordLength > 0 || sc.kclConfig.CallProcessRecordsEvenForEmptyRecordList {
		processRecordsStartTime := time.Now()

		// Delivery the events to the record processor
		input.CacheEntryTime = &getRecordsStartTime
		input.CacheExitTime = &processRecordsStartTime
		sc.recordProcessor.ProcessRecords(input)

		processedRecordsTiming := time.Since(processRecordsStartTime).Milliseconds()
		sc.mService.RecordProcessRecordsTime(sc.shard.ID, float64(processedRecordsTiming))
	}

	sc.mService.IncrRecordsProcessed(sc.shard.ID, recordLength)
	sc.mService.IncrBytesProcessed(sc.shard.ID, recordBytes)
	sc.mService.MillisBehindLatest(sc.shard.ID, float64(*millisBehindLatest))
}
