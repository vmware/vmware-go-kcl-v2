package streams

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type KinesisClient struct {
	internalClient *kinesis.Client
}

func (k KinesisClient) RegisterStreamConsumer(ctx context.Context, params *kinesis.RegisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error) {
	return k.internalClient.RegisterStreamConsumer(ctx, params, optFns...)
}

func (k KinesisClient) PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	return k.internalClient.PutRecords(ctx, params, optFns...)
}

func (k KinesisClient) DisableEnhancedMonitoring(ctx context.Context, params *kinesis.DisableEnhancedMonitoringInput, optFns ...func(*kinesis.Options)) (*kinesis.DisableEnhancedMonitoringOutput, error) {
	return k.internalClient.DisableEnhancedMonitoring(ctx, params, optFns...)
}

func (k KinesisClient) DescribeLimits(ctx context.Context, params *kinesis.DescribeLimitsInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeLimitsOutput, error) {
	return k.internalClient.DescribeLimits(ctx, params, optFns...)
}

func (k KinesisClient) ListStreams(ctx context.Context, params *kinesis.ListStreamsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListStreamsOutput, error) {
	return k.internalClient.ListStreams(ctx, params, optFns...)
}

func (k KinesisClient) GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	return k.internalClient.GetShardIterator(ctx, params, optFns...)
}

func (k KinesisClient) AddTagsToStream(ctx context.Context, params *kinesis.AddTagsToStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.AddTagsToStreamOutput, error) {
	return k.AddTagsToStream(ctx, params, optFns...)
}

func (k KinesisClient) DescribeStreamConsumer(ctx context.Context, params *kinesis.DescribeStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
	return k.DescribeStreamConsumer(ctx, params, optFns...)
}

func (k KinesisClient) UpdateStreamMode(ctx context.Context, params *kinesis.UpdateStreamModeInput, optFns ...func(*kinesis.Options)) (*kinesis.UpdateStreamModeOutput, error) {
	return k.internalClient.UpdateStreamMode(ctx, params, optFns...)
}

func (k KinesisClient) StopStreamEncryption(ctx context.Context, params *kinesis.StopStreamEncryptionInput, optFns ...func(*kinesis.Options)) (*kinesis.StopStreamEncryptionOutput, error) {
	return k.internalClient.StopStreamEncryption(ctx, params, optFns...)
}

func (k KinesisClient) StartStreamEncryption(ctx context.Context, params *kinesis.StartStreamEncryptionInput, optFns ...func(*kinesis.Options)) (*kinesis.StartStreamEncryptionOutput, error) {
	return k.internalClient.StartStreamEncryption(ctx, params, optFns...)
}

func (k KinesisClient) SplitShard(ctx context.Context, params *kinesis.SplitShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SplitShardOutput, error) {
	return k.internalClient.SplitShard(ctx, params, optFns...)
}

func (k KinesisClient) RemoveTagsFromStream(ctx context.Context, params *kinesis.RemoveTagsFromStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.RemoveTagsFromStreamOutput, error) {
	return k.RemoveTagsFromStream(ctx, params, optFns...)
}

func (k KinesisClient) ListStreamConsumers(ctx context.Context, params *kinesis.ListStreamConsumersInput, optFns ...func(*kinesis.Options)) (*kinesis.ListStreamConsumersOutput, error) {
	return k.internalClient.ListStreamConsumers(ctx, params, optFns...)
}

func (k KinesisClient) DeleteStream(ctx context.Context, params *kinesis.DeleteStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DeleteStreamOutput, error) {
	return k.internalClient.DeleteStream(ctx, params, optFns...)
}

func (k KinesisClient) EnableEnhancedMonitoring(ctx context.Context, params *kinesis.EnableEnhancedMonitoringInput, optFns ...func(*kinesis.Options)) (*kinesis.EnableEnhancedMonitoringOutput, error) {
	return k.internalClient.EnableEnhancedMonitoring(ctx, params, optFns...)
}

func (k KinesisClient) IncreaseStreamRetentionPeriod(ctx context.Context, params *kinesis.IncreaseStreamRetentionPeriodInput, optFns ...func(*kinesis.Options)) (*kinesis.IncreaseStreamRetentionPeriodOutput, error) {
	return k.internalClient.IncreaseStreamRetentionPeriod(ctx, params, optFns...)
}

func (k KinesisClient) DescribeStreamSummary(ctx context.Context, params *kinesis.DescribeStreamSummaryInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamSummaryOutput, error) {
	return k.internalClient.DescribeStreamSummary(ctx, params, optFns...)
}

func (k KinesisClient) SubscribeToShard(ctx context.Context, params *kinesis.SubscribeToShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
	return k.internalClient.SubscribeToShard(ctx, params, optFns...)
}

func (k KinesisClient) PutRecord(ctx context.Context, params *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error) {
	return k.internalClient.PutRecord(ctx, params, optFns...)
}

func (k KinesisClient) CreateStream(ctx context.Context, params *kinesis.CreateStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.CreateStreamOutput, error) {
	return k.internalClient.CreateStream(ctx, params, optFns...)
}

func (k KinesisClient) DeregisterStreamConsumer(ctx context.Context, params *kinesis.DeregisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DeregisterStreamConsumerOutput, error) {
	return k.internalClient.DeregisterStreamConsumer(ctx, params, optFns...)
}

func (k KinesisClient) GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	return k.internalClient.GetRecords(ctx, params, optFns...)
}

func (k KinesisClient) MergeShards(ctx context.Context, params *kinesis.MergeShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.MergeShardsOutput, error) {
	return k.internalClient.MergeShards(ctx, params, optFns...)
}

func (k KinesisClient) DecreaseStreamRetentionPeriod(ctx context.Context, params *kinesis.DecreaseStreamRetentionPeriodInput, optFns ...func(*kinesis.Options)) (*kinesis.DecreaseStreamRetentionPeriodOutput, error) {
	return k.internalClient.DecreaseStreamRetentionPeriod(ctx, params, optFns...)
}

func (k KinesisClient) ListTagsForStream(ctx context.Context, params *kinesis.ListTagsForStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.ListTagsForStreamOutput, error) {
	return k.internalClient.ListTagsForStream(ctx, params, optFns...)
}

func (k KinesisClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	return k.internalClient.ListShards(ctx, params, optFns...)
}

func (k KinesisClient) UpdateShardCount(ctx context.Context, params *kinesis.UpdateShardCountInput, optFns ...func(*kinesis.Options)) (*kinesis.UpdateShardCountOutput, error) {
	return k.internalClient.UpdateShardCount(ctx, params, optFns...)
}

func (k KinesisClient) DescribeStream(ctx context.Context, params *kinesis.DescribeStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
	return k.internalClient.DescribeStream(ctx, params, optFns...)
}

func NewKinesisClient() Client {
	return &KinesisClient{}
}
