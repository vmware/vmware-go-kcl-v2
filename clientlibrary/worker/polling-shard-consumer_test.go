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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	testGetRecordsError = errors.New("GetRecords Error")
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
	return nil, nil
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
