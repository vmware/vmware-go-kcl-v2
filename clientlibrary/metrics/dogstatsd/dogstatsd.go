package dogstatsd

import (
	"sync/atomic"

	"github.com/DataDog/datadog-go/v5/statsd"

	"github.com/vmware/vmware-go-kcl-v2/logger"
)

const (
	// Labels attached to metric submissions
	streamNameLabel      = "kinesis.stream"
	shardIdLabel         = "kinesis.shardId"
	workerIdLabel        = "kcl.workerId"
	applicationNameLabel = "kcl.applicationName"

	// Names for the metrics submitted below
	recordsProcessedMetric   = "kcl.records_processed"
	bytesProcessedMetric     = "kcl.bytes_processed"
	millisBehindLatestMetric = "kcl.millis_behind_latest"
	shardLeasesOwnedMetric   = "kcl.shard_leases_owned"
	getRecordsTimeMetric     = "kcl.get_records_time"
	processRecordsTimeMetric = "kcl.process_records_time"
)

// MonitoringService publishes KCL metrics to Datadog using the dogstatsd client
// package.
type MonitoringService struct {
	Client       statsd.ClientInterface // Client used to push metrics
	SamplingRate float64                // Sampling rate for all metrics
	Logger       logger.Logger          // Logger used for metric push errors

	leaseCount int64    // Number of leases current held
	tags       []string // List of tags to send with every metric
}

// New creates a new dogstatsd monitoring service. The given sampling rate will
// be used for all submitted metrics. The logger is only used if submissions
// fail.
func New(client statsd.ClientInterface, samplingRate float64, logger logger.Logger) *MonitoringService {
	return &MonitoringService{
		Client:       client,
		SamplingRate: samplingRate,
		Logger:       logger,
		leaseCount:   0,
		tags:         []string{},
	}
}

func (s *MonitoringService) Init(appName, streamName, workerID string) (err error) {
	s.tags = []string{
		applicationNameLabel + ":" + appName,
		streamNameLabel + ":" + streamName,
		workerIdLabel + ":" + workerID,
	}
	return err
}

func (s *MonitoringService) Start() error {
	return nil
}

func (s *MonitoringService) Shutdown() {}

// If the error is non-nil, log it. This should be inlined by the
// compiler so no overhead, and simplifies the metrics methods below,
// since the log entry is essentially always the same.
func (s *MonitoringService) logFailure(shard string, metric string, err error) {
	if err != nil {
		s.Logger.WithFields(logger.Fields{
			"error":   err,
			"shardId": shard,
			"metric":  metric,
		}).Errorf("failed to push metric")
	}
}

// Add the tags for a specific metric to the global monitoring service tags
func (s *MonitoringService) buildTags(tags ...string) []string {
	return append(tags, s.tags...)
}

func (s *MonitoringService) IncrRecordsProcessed(shard string, count int) {
	err := s.Client.Count(
		recordsProcessedMetric,
		int64(count),
		s.buildTags(shardIdLabel+":"+shard),
		s.SamplingRate,
	)
	s.logFailure(shard, recordsProcessedMetric, err)
}

func (s *MonitoringService) IncrBytesProcessed(shard string, count int64) {
	err := s.Client.Count(
		bytesProcessedMetric,
		count,
		s.buildTags(shardIdLabel+":"+shard),
		s.SamplingRate,
	)
	s.logFailure(shard, recordsProcessedMetric, err)
}

func (s *MonitoringService) MillisBehindLatest(shard string, millis float64) {
	err := s.Client.Gauge(
		millisBehindLatestMetric,
		millis,
		s.buildTags(shardIdLabel+":"+shard),
		s.SamplingRate,
	)
	s.logFailure(shard, millisBehindLatestMetric, err)
}

func (s *MonitoringService) DeleteMetricMillisBehindLatest(shard string) {
	s.MillisBehindLatest(shard, 0)
}

func (s *MonitoringService) LeaseGained(shard string) {
	leaseCount := atomic.AddInt64(&s.leaseCount, 1)
	err := s.Client.Gauge(
		shardLeasesOwnedMetric,
		float64(leaseCount),
		s.buildTags(shardIdLabel+":"+shard),
		s.SamplingRate,
	)
	s.logFailure(shard, shardLeasesOwnedMetric, err)
}

func (s *MonitoringService) LeaseLost(shard string) {
	leaseCount := atomic.AddInt64(&s.leaseCount, -1)
	err := s.Client.Gauge(
		shardLeasesOwnedMetric,
		float64(leaseCount),
		s.buildTags(shardIdLabel+":"+shard),
		s.SamplingRate,
	)
	s.logFailure(shard, shardLeasesOwnedMetric, err)
}

func (s *MonitoringService) LeaseRenewed(shard string) {
	leaseCount := atomic.LoadInt64(&s.leaseCount)
	err := s.Client.Gauge(
		shardLeasesOwnedMetric,
		float64(leaseCount),
		s.buildTags(shardIdLabel+":"+shard),
		s.SamplingRate,
	)
	s.logFailure(shard, shardLeasesOwnedMetric, err)
}

func (s *MonitoringService) RecordGetRecordsTime(shard string, time float64) {
	err := s.Client.Count(
		getRecordsTimeMetric,
		int64(time),
		s.buildTags(shardIdLabel+":"+shard),
		s.SamplingRate,
	)
	s.logFailure(shard, getRecordsTimeMetric, err)
}

func (s *MonitoringService) RecordProcessRecordsTime(shard string, time float64) {
	err := s.Client.Count(
		processRecordsTimeMetric,
		int64(time),
		s.buildTags(shardIdLabel+":"+shard),
		s.SamplingRate,
	)
	s.logFailure(shard, processRecordsTimeMetric, err)
}
