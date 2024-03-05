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

// Package cloudwatch
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package cloudwatch

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwatch "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/vmware/vmware-go-kcl-v2/logger"
)

// DefaultCloudwatchMetricsBufferDuration Buffer metrics for at most this long before publishing to CloudWatch.
const DefaultCloudwatchMetricsBufferDuration = 10 * time.Second

type MonitoringService struct {
	appName     string
	streamName  string
	workerID    string
	region      string
	credentials aws.CredentialsProvider
	logger      logger.Logger

	// control how often to publish to CloudWatch
	bufferDuration time.Duration

	stop         *chan struct{}
	waitGroup    *sync.WaitGroup
	svc          *cwatch.Client
	shardMetrics *sync.Map
}

type cloudWatchMetrics struct {
	sync.Mutex

	processedRecords   int64
	processedBytes     int64
	behindLatestMillis []float64
	leasesHeld         int64
	leaseRenewals      int64
	getRecordsTime     []float64
	processRecordsTime []float64
}

// NewMonitoringService returns a Monitoring service publishing metrics to CloudWatch.
func NewMonitoringService(region string, creds aws.CredentialsProvider) *MonitoringService {
	return NewMonitoringServiceWithOptions(region, creds, logger.GetDefaultLogger(), DefaultCloudwatchMetricsBufferDuration)
}

// NewMonitoringServiceWithOptions returns a Monitoring service publishing metrics to
// CloudWatch with the provided credentials, buffering duration and logger.
func NewMonitoringServiceWithOptions(region string, creds aws.CredentialsProvider, logger logger.Logger, bufferDur time.Duration) *MonitoringService {
	return &MonitoringService{
		region:         region,
		credentials:    creds,
		logger:         logger,
		bufferDuration: bufferDur,
	}
}

func (cw *MonitoringService) Init(appName, streamName, workerID string) error {
	cw.appName = appName
	cw.streamName = streamName
	cw.workerID = workerID

	cfg := &aws.Config{Region: cw.region}
	cfg.Credentials = cw.credentials

	cw.svc = cwatch.NewFromConfig(*cfg)
	cw.shardMetrics = &sync.Map{}

	stopChan := make(chan struct{})
	cw.stop = &stopChan
	wg := sync.WaitGroup{}
	cw.waitGroup = &wg

	return nil
}

func (cw *MonitoringService) Start() error {
	cw.waitGroup.Add(1)
	// entering eventloop for sending metrics to CloudWatch
	go cw.eventloop()
	return nil
}

func (cw *MonitoringService) Shutdown() {
	cw.logger.Infof("Shutting down cloudwatch metrics system...")
	close(*cw.stop)
	cw.waitGroup.Wait()
	cw.logger.Infof("Cloudwatch metrics system has been shutdown.")
}

// eventloop start daemon to flush metrics periodically
func (cw *MonitoringService) eventloop() {
	defer cw.waitGroup.Done()

	for {
		if err := cw.flush(); err != nil {
			cw.logger.Errorf("Error sending metrics to CloudWatch. %+v", err)
		}

		select {
		case <-*cw.stop:
			cw.logger.Infof("Shutting down monitoring system")
			if err := cw.flush(); err != nil {
				cw.logger.Errorf("Error sending metrics to CloudWatch. %+v", err)
			}
			return
		case <-time.After(cw.bufferDuration):
		}
	}
}

func (cw *MonitoringService) flushShard(shard string, metric *cloudWatchMetrics) bool {
	metric.Lock()
	defaultDimensions := []types.Dimension{
		{
			Name:  aws.String("Shard"),
			Value: &shard,
		},
		{
			Name:  aws.String("KinesisStreamName"),
			Value: &cw.streamName,
		},
	}

	leaseDimensions := []types.Dimension{
		{
			Name:  aws.String("Shard"),
			Value: &shard,
		},
		{
			Name:  aws.String("KinesisStreamName"),
			Value: &cw.streamName,
		},
		{
			Name:  aws.String("WorkerID"),
			Value: &cw.workerID,
		},
	}
	metricTimestamp := time.Now()

	data := []types.MetricDatum{
		{
			Dimensions: defaultDimensions,
			MetricName: aws.String("RecordsProcessed"),
			Unit:       types.StandardUnitCount,
			Timestamp:  &metricTimestamp,
			Value:      aws.Float64(float64(metric.processedRecords)),
		},
		{
			Dimensions: defaultDimensions,
			MetricName: aws.String("DataBytesProcessed"),
			Unit:       types.StandardUnitBytes,
			Timestamp:  &metricTimestamp,
			Value:      aws.Float64(float64(metric.processedBytes)),
		},
		{
			Dimensions: leaseDimensions,
			MetricName: aws.String("RenewLease.Success"),
			Unit:       types.StandardUnitCount,
			Timestamp:  &metricTimestamp,
			Value:      aws.Float64(float64(metric.leaseRenewals)),
		},
		{
			Dimensions: leaseDimensions,
			MetricName: aws.String("CurrentLeases"),
			Unit:       types.StandardUnitCount,
			Timestamp:  &metricTimestamp,
			Value:      aws.Float64(float64(metric.leasesHeld)),
		},
	}

	if len(metric.behindLatestMillis) > 0 {
		data = append(data, types.MetricDatum{
			Dimensions: defaultDimensions,
			MetricName: aws.String("MillisBehindLatest"),
			Unit:       types.StandardUnitMilliseconds,
			Timestamp:  &metricTimestamp,
			StatisticValues: &types.StatisticSet{
				SampleCount: aws.Float64(float64(len(metric.behindLatestMillis))),
				Sum:         sumFloat64(metric.behindLatestMillis),
				Maximum:     maxFloat64(metric.behindLatestMillis),
				Minimum:     minFloat64(metric.behindLatestMillis),
			}})
	}

	if len(metric.getRecordsTime) > 0 {
		data = append(data, types.MetricDatum{
			Dimensions: defaultDimensions,
			MetricName: aws.String("KinesisDataFetcher.getRecords.Time"),
			Unit:       types.StandardUnitMilliseconds,
			Timestamp:  &metricTimestamp,
			StatisticValues: &types.StatisticSet{
				SampleCount: aws.Float64(float64(len(metric.getRecordsTime))),
				Sum:         sumFloat64(metric.getRecordsTime),
				Maximum:     maxFloat64(metric.getRecordsTime),
				Minimum:     minFloat64(metric.getRecordsTime),
			}})
	}

	if len(metric.processRecordsTime) > 0 {
		data = append(data, types.MetricDatum{
			Dimensions: defaultDimensions,
			MetricName: aws.String("RecordProcessor.processRecords.Time"),
			Unit:       types.StandardUnitMilliseconds,
			Timestamp:  &metricTimestamp,
			StatisticValues: &types.StatisticSet{
				SampleCount: aws.Float64(float64(len(metric.processRecordsTime))),
				Sum:         sumFloat64(metric.processRecordsTime),
				Maximum:     maxFloat64(metric.processRecordsTime),
				Minimum:     minFloat64(metric.processRecordsTime),
			}})
	}

	// Publish metrics data to cloud watch
	_, err := cw.svc.PutMetricData(context.TODO(), &cwatch.PutMetricDataInput{
		Namespace:  aws.String(cw.appName),
		MetricData: data,
	})

	if err == nil {
		metric.processedRecords = 0
		metric.processedBytes = 0
		metric.behindLatestMillis = []float64{}
		metric.leaseRenewals = 0
		metric.getRecordsTime = []float64{}
		metric.processRecordsTime = []float64{}
	} else {
		cw.logger.Errorf("Error in publishing cloudwatch metrics. Error: %+v", err)
	}

	metric.Unlock()
	return true
}

func (cw *MonitoringService) flush() error {
	cw.logger.Debugf("Flushing metrics data. Stream: %s, Worker: %s", cw.streamName, cw.workerID)
	// publish per shard metrics
	cw.shardMetrics.Range(func(k, v interface{}) bool {
		shard, metric := k.(string), v.(*cloudWatchMetrics)
		return cw.flushShard(shard, metric)
	})

	return nil
}

func (cw *MonitoringService) IncrRecordsProcessed(shard string, count int) {
	m := cw.getOrCreatePerShardMetrics(shard)
	m.Lock()
	defer m.Unlock()
	m.processedRecords += int64(count)
}

func (cw *MonitoringService) IncrBytesProcessed(shard string, count int64) {
	m := cw.getOrCreatePerShardMetrics(shard)
	m.Lock()
	defer m.Unlock()
	m.processedBytes += count
}

func (cw *MonitoringService) MillisBehindLatest(shard string, millSeconds float64) {
	m := cw.getOrCreatePerShardMetrics(shard)
	m.Lock()
	defer m.Unlock()
	m.behindLatestMillis = append(m.behindLatestMillis, millSeconds)
}

func (cw *MonitoringService) LeaseGained(shard string) {
	m := cw.getOrCreatePerShardMetrics(shard)
	m.Lock()
	defer m.Unlock()
	m.leasesHeld++
}

func (cw *MonitoringService) LeaseLost(shard string) {
	m := cw.getOrCreatePerShardMetrics(shard)
	m.Lock()
	defer m.Unlock()
	m.leasesHeld--
}

func (cw *MonitoringService) LeaseRenewed(shard string) {
	m := cw.getOrCreatePerShardMetrics(shard)
	m.Lock()
	defer m.Unlock()
	m.leaseRenewals++
}

func (cw *MonitoringService) RecordGetRecordsTime(shard string, time float64) {
	m := cw.getOrCreatePerShardMetrics(shard)
	m.Lock()
	defer m.Unlock()
	m.getRecordsTime = append(m.getRecordsTime, time)
}
func (cw *MonitoringService) RecordProcessRecordsTime(shard string, time float64) {
	m := cw.getOrCreatePerShardMetrics(shard)
	m.Lock()
	defer m.Unlock()
	m.processRecordsTime = append(m.processRecordsTime, time)
}

func (cw *MonitoringService) DeleteMetricMillisBehindLatest(shard string) {
	// not implemented
}

func (cw *MonitoringService) getOrCreatePerShardMetrics(shard string) *cloudWatchMetrics {
	var i interface{}
	var ok bool
	if i, ok = cw.shardMetrics.Load(shard); !ok {
		m := &cloudWatchMetrics{}
		cw.shardMetrics.Store(shard, m)
		return m
	}

	return i.(*cloudWatchMetrics)
}

func sumFloat64(slice []float64) *float64 {
	sum := float64(0)
	for _, num := range slice {
		sum += num
	}
	return &sum
}

func maxFloat64(slice []float64) *float64 {
	if len(slice) < 1 {
		return aws.Float64(0)
	}
	max := slice[0]
	for _, num := range slice {
		if num > max {
			max = num
		}
	}
	return &max
}

func minFloat64(slice []float64) *float64 {
	if len(slice) < 1 {
		return aws.Float64(0)
	}
	min := slice[0]
	for _, num := range slice {
		if num < min {
			min = num
		}
	}
	return &min
}
