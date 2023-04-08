/*
 * Copyright (c) 2023 VMware, Inc.
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

package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	chk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/checkpoint"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	kcl "github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/metrics"
	par "github.com/vmware/vmware-go-kcl-v2/clientlibrary/partition"
	"github.com/vmware/vmware-go-kcl-v2/logger"
)

var (
	testGetRecordsError = errors.New("GetRecords Error")
	getLeaseTestFailure = errors.New("GetLease test failure")
)

func TestCallGetRecordsAPI(t *testing.T) {
	// basic happy path
	m1 := MockKinesisSubscriberGetter{}
	ret := kinesis.GetRecordsOutput{}
	m1.On("GetRecords", mock.Anything, mock.Anything, mock.Anything).Return(&ret, nil)
	psc := PollingShardConsumer{
		commonShardConsumer: commonShardConsumer{kc: &m1},
	}
	gri := kinesis.GetRecordsInput{
		ShardIterator: aws.String("shard-iterator-01"),
	}
	out, _, err := psc.callGetRecordsAPI(&gri)
	assert.Nil(t, err)
	assert.Equal(t, &ret, out)
	m1.AssertExpectations(t)

	// check that localTPSExceededError is thrown when trying more than 5 TPS
	m2 := MockKinesisSubscriberGetter{}
	psc2 := PollingShardConsumer{
		commonShardConsumer: commonShardConsumer{kc: &m2},
		callsLeft:           0,
	}
	rateLimitTimeSince = func(t time.Time) time.Duration {
		return 500 * time.Millisecond
	}
	out2, _, err2 := psc2.callGetRecordsAPI(&gri)
	assert.Nil(t, out2)
	assert.ErrorIs(t, err2, localTPSExceededError)
	m2.AssertExpectations(t)

	// check that getRecords is called normally in bytesRead = 0 case
	m3 := MockKinesisSubscriberGetter{}
	ret3 := kinesis.GetRecordsOutput{}
	m3.On("GetRecords", mock.Anything, mock.Anything, mock.Anything).Return(&ret3, nil)
	psc3 := PollingShardConsumer{
		commonShardConsumer: commonShardConsumer{kc: &m3},
		callsLeft:           2,
		bytesRead:           0,
	}
	rateLimitTimeSince = func(t time.Time) time.Duration {
		return 2 * time.Second
	}
	out3, checkSleepVal, err3 := psc3.callGetRecordsAPI(&gri)
	assert.Nil(t, err3)
	assert.Equal(t, checkSleepVal, 0)
	assert.Equal(t, &ret3, out3)
	m3.AssertExpectations(t)

	// check that correct cool off period is taken for 10mb in 1 second
	testTime := time.Now()
	m4 := MockKinesisSubscriberGetter{}
	ret4 := kinesis.GetRecordsOutput{Records: nil}
	m4.On("GetRecords", mock.Anything, mock.Anything, mock.Anything).Return(&ret4, nil)
	psc4 := PollingShardConsumer{
		commonShardConsumer: commonShardConsumer{kc: &m4},
		callsLeft:           2,
		bytesRead:           MaxBytes,
		lastCheckTime:       testTime,
		remBytes:            MaxBytes,
	}
	rateLimitTimeSince = func(t time.Time) time.Duration {
		return 2 * time.Second
	}
	rateLimitTimeNow = func() time.Time {
		return testTime.Add(time.Second)
	}
	out4, checkSleepVal2, err4 := psc4.callGetRecordsAPI(&gri)
	assert.Nil(t, err4)
	assert.Equal(t, &ret4, out4)
	m4.AssertExpectations(t)
	if checkSleepVal2 != 0 {
		t.Errorf("Incorrect Cool Off Period: %v", checkSleepVal2)
	}

	// check that no cool off period is taken for 6mb in 3 seconds
	testTime2 := time.Now()
	m5 := MockKinesisSubscriberGetter{}
	ret5 := kinesis.GetRecordsOutput{}
	m5.On("GetRecords", mock.Anything, mock.Anything, mock.Anything).Return(&ret5, nil)
	psc5 := PollingShardConsumer{
		commonShardConsumer: commonShardConsumer{kc: &m5},
		callsLeft:           2,
		bytesRead:           MaxBytesPerSecond * 3,
		lastCheckTime:       testTime2,
		remBytes:            MaxBytes,
	}
	rateLimitTimeSince = func(t time.Time) time.Duration {
		return 3 * time.Second
	}
	rateLimitTimeNow = func() time.Time {
		return testTime2.Add(time.Second * 3)
	}
	out5, checkSleepVal3, err5 := psc5.callGetRecordsAPI(&gri)
	assert.Nil(t, err5)
	assert.Equal(t, checkSleepVal3, 0)
	assert.Equal(t, &ret5, out5)
	m5.AssertExpectations(t)

	// check for correct cool off period with 8mb in .2 seconds with 6mb remaining
	testTime3 := time.Now()
	m6 := MockKinesisSubscriberGetter{}
	ret6 := kinesis.GetRecordsOutput{Records: nil}
	m6.On("GetRecords", mock.Anything, mock.Anything, mock.Anything).Return(&ret6, nil)
	psc6 := PollingShardConsumer{
		commonShardConsumer: commonShardConsumer{kc: &m6},
		callsLeft:           2,
		bytesRead:           MaxBytesPerSecond * 4,
		lastCheckTime:       testTime3,
		remBytes:            MaxBytesPerSecond * 3,
	}
	rateLimitTimeSince = func(t time.Time) time.Duration {
		return 3 * time.Second
	}
	rateLimitTimeNow = func() time.Time {
		return testTime3.Add(time.Second / 5)
	}
	out6, checkSleepVal4, err6 := psc6.callGetRecordsAPI(&gri)
	assert.Nil(t, err6)
	assert.Equal(t, &ret6, out6)
	m5.AssertExpectations(t)
	if checkSleepVal4 != 0 {
		t.Errorf("Incorrect Cool Off Period: %v", checkSleepVal4)
	}

	// case where getRecords throws error
	m7 := MockKinesisSubscriberGetter{}
	ret7 := kinesis.GetRecordsOutput{Records: nil}
	m7.On("GetRecords", mock.Anything, mock.Anything, mock.Anything).Return(&ret7, testGetRecordsError)
	psc7 := PollingShardConsumer{
		commonShardConsumer: commonShardConsumer{kc: &m7},
		callsLeft:           2,
		bytesRead:           0,
	}
	rateLimitTimeSince = func(t time.Time) time.Duration {
		return 2 * time.Second
	}
	out7, checkSleepVal7, err7 := psc7.callGetRecordsAPI(&gri)
	assert.Equal(t, err7, testGetRecordsError)
	assert.Equal(t, checkSleepVal7, 0)
	assert.Equal(t, out7, &ret7)
	m7.AssertExpectations(t)

	// restore original func
	rateLimitTimeNow = time.Now
	rateLimitTimeSince = time.Since

}

type MockKinesisSubscriberGetter struct {
	mock.Mock
}

func (m *MockKinesisSubscriberGetter) GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	ret := m.Called(ctx, params, optFns)

	return ret.Get(0).(*kinesis.GetRecordsOutput), ret.Error(1)
}

func (m *MockKinesisSubscriberGetter) GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	ret := m.Called(ctx, params, optFns)
	return ret.Get(0).(*kinesis.GetShardIteratorOutput), ret.Error(1)
}

func (m *MockKinesisSubscriberGetter) SubscribeToShard(ctx context.Context, params *kinesis.SubscribeToShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
	return nil, nil
}

func TestPollingShardConsumer_checkCoolOffPeriod(t *testing.T) {
	refTime := time.Now()
	type fields struct {
		lastCheckTime time.Time
		remBytes      int
		bytesRead     int
	}
	tests := []struct {
		name    string
		fields  fields
		timeNow time.Time
		want    int
		wantErr bool
	}{
		{
			"zero time max bytes to spend",
			fields{
				time.Time{},
				0,
				0,
			},
			refTime,
			0,
			false,
		},
		{
			"same second, bytes still left to spend",
			fields{
				refTime,
				MaxBytesPerSecond,
				MaxBytesPerSecond - 1,
			},
			refTime,
			0,
			false,
		},
		{
			"same second, not many but some bytes still left to spend",
			fields{
				refTime,
				8,
				MaxBytesPerSecond,
			},
			refTime,
			0,
			false,
		},
		{
			"same second, 1 byte still left to spend",
			fields{
				refTime,
				1,
				MaxBytesPerSecond,
			},
			refTime,
			0,
			false,
		},
		{
			"next second, bytes still left to spend",
			fields{
				refTime,
				42,
				1024,
			},
			refTime.Add(1 * time.Second),
			0,
			false,
		},
		{
			"same second, max bytes per second already spent",
			fields{
				refTime,
				0,
				MaxBytesPerSecond,
			},
			refTime,
			1,
			true,
		},
		{
			"same second, more than max bytes per second already spent",
			fields{
				refTime,
				0,
				MaxBytesPerSecond + 1,
			},
			refTime,
			2,
			true,
		},

		// Kinesis prevents reading more than 10 MiB at once
		{
			"same second, 10 MiB read all at once",
			fields{
				refTime,
				0,
				10 * 1024 * 1024,
			},
			refTime,
			6,
			true,
		},

		{
			"same second, 10 MB read all at once",
			fields{
				refTime,
				0,
				10 * 1000 * 1000,
			},
			refTime,
			5,
			true,
		},
		{
			"5 seconds ago, 10 MB read all at once",
			fields{
				refTime,
				0,
				10 * 1000 * 1000,
			},
			refTime.Add(5 * time.Second),
			0,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &PollingShardConsumer{
				lastCheckTime: tt.fields.lastCheckTime,
				remBytes:      tt.fields.remBytes,
				bytesRead:     tt.fields.bytesRead,
			}
			rateLimitTimeNow = func() time.Time {
				return tt.timeNow
			}
			got, err := sc.checkCoolOffPeriod()
			if (err != nil) != tt.wantErr {
				t.Errorf("PollingShardConsumer.checkCoolOffPeriod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("PollingShardConsumer.checkCoolOffPeriod() = %v, want %v", got, tt.want)
			}
		})
	}

	// restore original time.Now
	rateLimitTimeNow = time.Now
}

func TestPollingShardConsumer_renewLease(t *testing.T) {
	type fields struct {
		checkpointer chk.Checkpointer
		kclConfig    *config.KinesisClientLibConfiguration
		mService     metrics.MonitoringService
	}
	tests := []struct {
		name            string
		fields          fields
		testMillis      time.Duration
		expRenewalCalls int
		expRenewals     int
		expErr          error
	}{
		{
			"renew once",
			fields{
				&mockCheckpointer{},
				&config.KinesisClientLibConfiguration{
					LeaseRefreshWaitTime: 10,
				},
				&mockMetrics{},
			},
			15,
			1,
			1,
			nil,
		},
		{
			"renew some",
			fields{
				&mockCheckpointer{},
				&config.KinesisClientLibConfiguration{
					LeaseRefreshWaitTime: 50,
				},
				&mockMetrics{},
			},
			50*5 + 10,
			5,
			5,
			nil,
		},
		{
			"renew twice every 2.5 seconds",
			fields{
				&mockCheckpointer{},
				&config.KinesisClientLibConfiguration{
					LeaseRefreshWaitTime: 2500,
				},
				&mockMetrics{},
			},
			5100,
			2,
			2,
			nil,
		},
		{
			"lease error",
			fields{
				&mockCheckpointer{fail: true},
				&config.KinesisClientLibConfiguration{
					LeaseRefreshWaitTime: 500,
				},
				&mockMetrics{},
			},
			1100,
			1,
			0,
			getLeaseTestFailure,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &PollingShardConsumer{
				commonShardConsumer: commonShardConsumer{
					shard:        &par.ShardStatus{},
					checkpointer: tt.fields.checkpointer,
					kclConfig:    tt.fields.kclConfig,
				},
				mService: tt.fields.mService,
			}
			ctx, cancel := context.WithCancel(context.Background())
			leaseRenewalErrChan := make(chan error, 1)
			go func() {
				leaseRenewalErrChan <- sc.renewLease(ctx)
			}()
			time.Sleep(tt.testMillis * time.Millisecond)
			cancel()
			err := <-leaseRenewalErrChan
			assert.Equal(t, tt.expErr, err)
			assert.Equal(t, tt.expRenewalCalls, sc.checkpointer.(*mockCheckpointer).getLeaseCalledTimes)
			assert.Equal(t, tt.expRenewals, sc.mService.(*mockMetrics).leaseRenewedCalledTimes)
		})
	}
}

func TestPollingShardConsumer_getRecordsRenewLease(t *testing.T) {
	log := logger.GetDefaultLogger()
	type fields struct {
		checkpointer chk.Checkpointer
		kclConfig    *config.KinesisClientLibConfiguration
		mService     metrics.MonitoringService
	}
	tests := []struct {
		name   string
		fields fields

		// testMillis must be at least 200ms or you'll trigger the localTPSExceededError
		testMillis      time.Duration
		expRenewalCalls int
		expRenewals     int
		shardClosed     bool
		expErr          error
	}{
		{
			"renew once",
			fields{
				&mockCheckpointer{},
				&config.KinesisClientLibConfiguration{
					LeaseRefreshWaitTime:    200,
					Logger:                  log,
					InitialPositionInStream: config.LATEST,
				},
				&mockMetrics{},
			},
			250,
			1,
			1,
			false,
			nil,
		},
		{
			"renew some",
			fields{
				&mockCheckpointer{},
				&config.KinesisClientLibConfiguration{
					LeaseRefreshWaitTime:    50,
					Logger:                  log,
					InitialPositionInStream: config.LATEST,
				},
				&mockMetrics{},
			},
			50*5 + 10,
			5,
			5,
			false,
			nil,
		},
		{
			"renew twice every 2.5 seconds",
			fields{
				&mockCheckpointer{},
				&config.KinesisClientLibConfiguration{
					LeaseRefreshWaitTime:    2500,
					Logger:                  log,
					InitialPositionInStream: config.LATEST,
				},
				&mockMetrics{},
			},
			5100,
			2,
			2,
			false,
			nil,
		},
		{
			"lease error",
			fields{
				&mockCheckpointer{fail: true},
				&config.KinesisClientLibConfiguration{
					LeaseRefreshWaitTime:    500,
					Logger:                  log,
					InitialPositionInStream: config.LATEST,
				},
				&mockMetrics{},
			},
			1100,
			1,
			0,
			false,
			getLeaseTestFailure,
		},
	}
	iterator := "test-iterator"
	nextIt := "test-next-iterator"
	millisBehind := int64(0)
	stopChan := make(chan struct{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mk := MockKinesisSubscriberGetter{}
			gro := kinesis.GetRecordsOutput{
				Records: []types.Record{
					{
						Data:                        []byte{},
						PartitionKey:                new(string),
						SequenceNumber:              new(string),
						ApproximateArrivalTimestamp: &time.Time{},
						EncryptionType:              "",
					},
				},
				MillisBehindLatest: &millisBehind,
			}
			if !tt.shardClosed {
				gro.NextShardIterator = &nextIt
			}
			mk.On("GetRecords", mock.Anything, mock.Anything, mock.Anything).Return(&gro, nil)
			mk.On("GetShardIterator", mock.Anything, mock.Anything, mock.Anything).Return(&kinesis.GetShardIteratorOutput{ShardIterator: &iterator}, nil)
			rp := mockRecordProcessor{
				processDurationMillis: tt.testMillis,
			}
			sc := &PollingShardConsumer{
				commonShardConsumer: commonShardConsumer{
					shard: &par.ShardStatus{
						ID:  "test-shard-id",
						Mux: &sync.RWMutex{},
					},
					checkpointer:    tt.fields.checkpointer,
					kclConfig:       tt.fields.kclConfig,
					kc:              &mk,
					recordProcessor: &rp,
					mService:        tt.fields.mService,
				},
				stop:     &stopChan,
				mService: tt.fields.mService,
			}

			// Send the stop signal a little before the total time it should
			// take to get records and process them. This prevents test time
			// errors due to the threads running longer than the test case
			// expects.
			go func() {
				time.Sleep((tt.testMillis - 1) * time.Millisecond)
				stopChan <- struct{}{}
			}()

			err := sc.getRecords()

			assert.Equal(t, tt.expErr, err)
			assert.Equal(t, tt.expRenewalCalls, sc.checkpointer.(*mockCheckpointer).readGetLeaseCalledTimes())
			assert.Equal(t, tt.expRenewals, sc.mService.(*mockMetrics).readLeaseRenewedCalledTimes())
			mk.AssertExpectations(t)
		})
	}
}

type mockCheckpointer struct {
	getLeaseCalledTimes int
	gLCTMu              sync.Mutex
	fail                bool
}

func (m *mockCheckpointer) readGetLeaseCalledTimes() int {
	m.gLCTMu.Lock()
	defer m.gLCTMu.Unlock()
	return m.getLeaseCalledTimes
}
func (m *mockCheckpointer) Init() error { return nil }
func (m *mockCheckpointer) GetLease(*par.ShardStatus, string) error {
	m.gLCTMu.Lock()
	defer m.gLCTMu.Unlock()
	m.getLeaseCalledTimes++
	if m.fail {
		return getLeaseTestFailure
	}
	return nil
}
func (m *mockCheckpointer) CheckpointSequence(*par.ShardStatus) error { return nil }
func (m *mockCheckpointer) FetchCheckpoint(*par.ShardStatus) error    { return nil }
func (m *mockCheckpointer) RemoveLeaseInfo(string) error              { return nil }
func (m *mockCheckpointer) RemoveLeaseOwner(string) error             { return nil }
func (m *mockCheckpointer) GetLeaseOwner(string) (string, error)      { return "", nil }
func (m *mockCheckpointer) ListActiveWorkers(map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	return map[string][]*par.ShardStatus{}, nil
}
func (m *mockCheckpointer) ClaimShard(*par.ShardStatus, string) error { return nil }

type mockRecordProcessor struct {
	processDurationMillis time.Duration
}

func (m mockRecordProcessor) Initialize(initializationInput *kcl.InitializationInput) {}
func (m mockRecordProcessor) ProcessRecords(processRecordsInput *kcl.ProcessRecordsInput) {
	time.Sleep(time.Millisecond * m.processDurationMillis)
}
func (m mockRecordProcessor) Shutdown(shutdownInput *kcl.ShutdownInput) {}

type mockMetrics struct {
	leaseRenewedCalledTimes int
	lRCTMu                  sync.Mutex
}

func (m *mockMetrics) readLeaseRenewedCalledTimes() int {
	m.lRCTMu.Lock()
	defer m.lRCTMu.Unlock()
	return m.leaseRenewedCalledTimes
}
func (m *mockMetrics) Init(appName, streamName, workerID string) error       { return nil }
func (m *mockMetrics) Start() error                                          { return nil }
func (m *mockMetrics) IncrRecordsProcessed(shard string, count int)          {}
func (m *mockMetrics) IncrBytesProcessed(shard string, count int64)          {}
func (m *mockMetrics) MillisBehindLatest(shard string, milliSeconds float64) {}
func (m *mockMetrics) DeleteMetricMillisBehindLatest(shard string)           {}
func (m *mockMetrics) LeaseGained(shard string)                              {}
func (m *mockMetrics) LeaseLost(shard string)                                {}
func (m *mockMetrics) LeaseRenewed(shard string) {
	m.lRCTMu.Lock()
	defer m.lRCTMu.Unlock()
	m.leaseRenewedCalledTimes++
}
func (m *mockMetrics) RecordGetRecordsTime(shard string, time float64)     {}
func (m *mockMetrics) RecordProcessRecordsTime(shard string, time float64) {}
func (m *mockMetrics) Shutdown()                                           {}
