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
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	out, err := psc.callGetRecordsAPI(&gri)
	assert.Nil(t, err)
	assert.Equal(t, &ret, out)
	m1.AssertExpectations(t)
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
