package streams

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	log "github.com/sirupsen/logrus"
)

type DynamodbStreamAdapterClient struct {
	internalClient *dynamodbstreams.Client
	tableName      *string
}

func (d DynamodbStreamAdapterClient) ListStreams(ctx context.Context, params *kinesis.ListStreamsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListStreamsOutput, error) {
	req, _ := json.Marshal(params)
	log.Info(fmt.Sprintf("ListStreams request %v", string(req)))
	listStreamsOutput, err := d.internalClient.ListStreams(ctx, d.convertListStreamsInput(params))
	out, _ := json.Marshal(listStreamsOutput)
	log.Info(fmt.Sprintf("ListStreams >>> response %v", string(out)))
	if err != nil {
		return nil, err
	}
	return d.convertListStreamsOutput(listStreamsOutput), nil
}

func (d DynamodbStreamAdapterClient) GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	requestParamsString, _ := json.Marshal(params)
	log.Info(fmt.Sprintf("GetShardIterator >>> request %v", string(requestParamsString)))
	shardIteratorOutput, err := d.internalClient.GetShardIterator(ctx, d.convertShardIteratorInput(params))
	out, _ := json.Marshal(shardIteratorOutput)
	log.Info(fmt.Sprintf("GetShardIterator >>> response %v", string(out)))
	if err != nil {
		return nil, err
	}
	return d.convertShardIteratorOutput(shardIteratorOutput), nil
}

func (d DynamodbStreamAdapterClient) GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	req, _ := json.Marshal(params)
	log.Info(fmt.Sprintf(fmt.Sprintf("GetRecords >>> request %v", string(req))))
	getRecordsOutput, err := d.internalClient.GetRecords(ctx, d.convertGetRecordsInput(params))
	out, _ := json.Marshal(getRecordsOutput)
	log.Info(fmt.Sprintf("GetRecords >>> output %v", string(out)))
	if err != nil {
		return nil, err
	}
	return d.convertGetRecordsOutput(getRecordsOutput), nil
}

func (d DynamodbStreamAdapterClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	req, _ := json.Marshal(params)
	log.Info(fmt.Sprintf("ListShards >>> request %v", string(req)))
	var maxResults int32 = 100
	if params.MaxResults != nil {
		if 100 >= *params.MaxResults {
			params.MaxResults = &maxResults
		}
	}
	dynamoOutput, err := d.internalClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		ExclusiveStartShardId: params.ExclusiveStartShardId,
		Limit:                 params.MaxResults,
		StreamArn:             params.StreamName,
	})
	out, _ := json.Marshal(dynamoOutput)
	log.Info(fmt.Sprintf("ListShards >>> output %v", string(out)))
	if err != nil {
		return nil, err
	}
	return d.convertListShardsOutput(dynamoOutput), nil
}

func (d DynamodbStreamAdapterClient) DescribeStream(ctx context.Context, params *kinesis.DescribeStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
	dynamoOutput, err := d.internalClient.DescribeStream(ctx, d.convertDescribeStreamInput(params))
	if err != nil {
		return nil, err
	}
	return d.convertDescribeStreamOutput(dynamoOutput), nil
}

func (d DynamodbStreamAdapterClient) RegisterStreamConsumer(ctx context.Context, params *kinesis.RegisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) DisableEnhancedMonitoring(ctx context.Context, params *kinesis.DisableEnhancedMonitoringInput, optFns ...func(*kinesis.Options)) (*kinesis.DisableEnhancedMonitoringOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) DescribeLimits(ctx context.Context, params *kinesis.DescribeLimitsInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeLimitsOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) AddTagsToStream(ctx context.Context, params *kinesis.AddTagsToStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.AddTagsToStreamOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) DescribeStreamConsumer(ctx context.Context, params *kinesis.DescribeStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) UpdateStreamMode(ctx context.Context, params *kinesis.UpdateStreamModeInput, optFns ...func(*kinesis.Options)) (*kinesis.UpdateStreamModeOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) StopStreamEncryption(ctx context.Context, params *kinesis.StopStreamEncryptionInput, optFns ...func(*kinesis.Options)) (*kinesis.StopStreamEncryptionOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) StartStreamEncryption(ctx context.Context, params *kinesis.StartStreamEncryptionInput, optFns ...func(*kinesis.Options)) (*kinesis.StartStreamEncryptionOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) SplitShard(ctx context.Context, params *kinesis.SplitShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SplitShardOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) RemoveTagsFromStream(ctx context.Context, params *kinesis.RemoveTagsFromStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.RemoveTagsFromStreamOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) ListStreamConsumers(ctx context.Context, params *kinesis.ListStreamConsumersInput, optFns ...func(*kinesis.Options)) (*kinesis.ListStreamConsumersOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) DeleteStream(ctx context.Context, params *kinesis.DeleteStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DeleteStreamOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) EnableEnhancedMonitoring(ctx context.Context, params *kinesis.EnableEnhancedMonitoringInput, optFns ...func(*kinesis.Options)) (*kinesis.EnableEnhancedMonitoringOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) IncreaseStreamRetentionPeriod(ctx context.Context, params *kinesis.IncreaseStreamRetentionPeriodInput, optFns ...func(*kinesis.Options)) (*kinesis.IncreaseStreamRetentionPeriodOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) DescribeStreamSummary(ctx context.Context, params *kinesis.DescribeStreamSummaryInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamSummaryOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) SubscribeToShard(ctx context.Context, params *kinesis.SubscribeToShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) PutRecord(ctx context.Context, params *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) CreateStream(ctx context.Context, params *kinesis.CreateStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.CreateStreamOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) DeregisterStreamConsumer(ctx context.Context, params *kinesis.DeregisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DeregisterStreamConsumerOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) MergeShards(ctx context.Context, params *kinesis.MergeShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.MergeShardsOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) DecreaseStreamRetentionPeriod(ctx context.Context, params *kinesis.DecreaseStreamRetentionPeriodInput, optFns ...func(*kinesis.Options)) (*kinesis.DecreaseStreamRetentionPeriodOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) ListTagsForStream(ctx context.Context, params *kinesis.ListTagsForStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.ListTagsForStreamOutput, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamodbStreamAdapterClient) UpdateShardCount(ctx context.Context, params *kinesis.UpdateShardCountInput, optFns ...func(*kinesis.Options)) (*kinesis.UpdateShardCountOutput, error) {
	//TODO implement me
	panic("implement me")
}

func NewDynamodbStreamClient(client *dynamodbstreams.Client) Client {
	return &DynamodbStreamAdapterClient{
		internalClient: client,
	}
}

func (d DynamodbStreamAdapterClient) convertListStreamsInput(kinesisInput *kinesis.ListStreamsInput) *dynamodbstreams.ListStreamsInput {
	dynamoInput := dynamodbstreams.ListStreamsInput{}

	// Set ExclusiveStartStreamArn field using ExclusiveStartStreamName field from kinesisInput
	dynamoInput.ExclusiveStartStreamArn = nil
	if kinesisInput.ExclusiveStartStreamName != nil {
		dynamoInput.ExclusiveStartStreamArn = kinesisInput.ExclusiveStartStreamName
	}

	// Set Limit field
	if kinesisInput.Limit != nil {
		dynamoInput.Limit = kinesisInput.Limit
	}

	// Set TableName field to nil since it does not exist in kinesis.ListStreamsInput
	dynamoInput.TableName = d.tableName

	return &dynamoInput
}

func (d DynamodbStreamAdapterClient) convertListStreamsOutput(dOutput *dynamodbstreams.ListStreamsOutput) *kinesis.ListStreamsOutput {
	kinesisOutput := kinesis.ListStreamsOutput{}
	hasMoreStreams := dOutput.LastEvaluatedStreamArn != nil
	// Set HasMoreStreams field
	kinesisOutput.HasMoreStreams = &hasMoreStreams

	// Set StreamNames field using StreamDescriptionList field from output
	kinesisOutput.StreamNames = nil
	if dOutput.Streams != nil {
		kinesisOutput.StreamNames = make([]string, len(dOutput.Streams))
		for i, stream := range dOutput.Streams {
			kinesisOutput.StreamNames[i] = *stream.StreamArn
		}
	}
	kinesisOutput.ResultMetadata = dOutput.ResultMetadata
	return &kinesisOutput
}

func (d DynamodbStreamAdapterClient) convertDescribeStreamInput(params *kinesis.DescribeStreamInput) *dynamodbstreams.DescribeStreamInput {
	dynamoStreamInput := dynamodbstreams.DescribeStreamInput{}
	if params != nil {
		dynamoStreamInput.StreamArn = params.StreamName
		dynamoStreamInput.Limit = params.Limit
		dynamoStreamInput.ExclusiveStartShardId = params.ExclusiveStartShardId
	}
	return &dynamoStreamInput
}

func (d DynamodbStreamAdapterClient) convertShardIteratorOutput(output *dynamodbstreams.GetShardIteratorOutput) *kinesis.GetShardIteratorOutput {
	kinesisOutput := kinesis.GetShardIteratorOutput{}
	if output != nil {
		kinesisOutput.ShardIterator = output.ShardIterator
		kinesisOutput.ResultMetadata = output.ResultMetadata
	}
	return &kinesisOutput
}

func (d DynamodbStreamAdapterClient) convertShardIteratorInput(kinesisInput *kinesis.GetShardIteratorInput) *dynamodbstreams.GetShardIteratorInput {
	if kinesisInput.ShardIteratorType == ktypes.ShardIteratorTypeAtTimestamp {
		kinesisInput.ShardIteratorType = ktypes.ShardIteratorTypeLatest
	}
	dynamodbInput := &dynamodbstreams.GetShardIteratorInput{
		ShardId:           kinesisInput.ShardId,
		ShardIteratorType: types.ShardIteratorType(kinesisInput.ShardIteratorType),
		StreamArn:         kinesisInput.StreamName,
		SequenceNumber:    kinesisInput.StartingSequenceNumber,
	}
	return dynamodbInput
}

func (d DynamodbStreamAdapterClient) convertGetRecordsInput(params *kinesis.GetRecordsInput) *dynamodbstreams.GetRecordsInput {
	var dynamoMaxLimit int32 = 1000
	if params.Limit != nil {
		if *params.Limit >= 10000 {
			params.Limit = &dynamoMaxLimit
		}
	}
	dynamoInput := &dynamodbstreams.GetRecordsInput{
		Limit:         params.Limit,
		ShardIterator: params.ShardIterator,
	}
	return dynamoInput
}

func (d DynamodbStreamAdapterClient) convertGetRecordsOutput(dynamoOutput *dynamodbstreams.GetRecordsOutput) *kinesis.GetRecordsOutput {
	kinesisOutput := kinesis.GetRecordsOutput{}
	if dynamoOutput != nil && len(dynamoOutput.Records) != 0 {
		kinesisOutput.Records = make([]ktypes.Record, len(dynamoOutput.Records))
		kinesisOutput.ResultMetadata = dynamoOutput.ResultMetadata
		kinesisOutput.NextShardIterator = dynamoOutput.NextShardIterator
	}
	for _, record := range dynamoOutput.Records {
		reqBodyBytes := new(bytes.Buffer)
		json.NewEncoder(reqBodyBytes).Encode(record)
		kinesisOutput.Records = append(kinesisOutput.Records, ktypes.Record{
			Data:                        reqBodyBytes.Bytes(),
			SequenceNumber:              record.Dynamodb.SequenceNumber,
			ApproximateArrivalTimestamp: record.Dynamodb.ApproximateCreationDateTime,
			EncryptionType:              ktypes.EncryptionTypeNone,
		})
	}
	return &kinesisOutput
}

func (d DynamodbStreamAdapterClient) convertListShardsOutput(output *dynamodbstreams.DescribeStreamOutput) *kinesis.ListShardsOutput {
	kinesisOutput := kinesis.ListShardsOutput{}
	if output != nil && output.StreamDescription.StreamStatus == types.StreamStatusDisabled {
		return &kinesisOutput
	}
	if output != nil {
		if output.StreamDescription != nil && len(output.StreamDescription.Shards) != 0 {
			kinesisOutput.Shards = make([]ktypes.Shard, len(output.StreamDescription.Shards))
		}
		for _, shard := range output.StreamDescription.Shards {
			kinesisOutput.Shards = append(kinesisOutput.Shards, ktypes.Shard{
				SequenceNumberRange: &ktypes.SequenceNumberRange{
					StartingSequenceNumber: shard.SequenceNumberRange.StartingSequenceNumber,
					EndingSequenceNumber:   shard.SequenceNumberRange.EndingSequenceNumber,
				},
				ShardId:       shard.ShardId,
				ParentShardId: shard.ParentShardId,
			})
		}
		kinesisOutput.ResultMetadata = output.ResultMetadata
	}
	return &kinesisOutput
}

func (d DynamodbStreamAdapterClient) convertDescribeStreamOutput(output *dynamodbstreams.DescribeStreamOutput) *kinesis.DescribeStreamOutput {
	kinesisOutput := kinesis.DescribeStreamOutput{}
	if output != nil && output.StreamDescription.StreamStatus == types.StreamStatusDisabled {
		return &kinesisOutput
	}
	if output != nil {
		if output.StreamDescription != nil && len(output.StreamDescription.Shards) != 0 {
			kinesisOutput.StreamDescription = &ktypes.StreamDescription{
				StreamARN:               output.StreamDescription.StreamArn,
				StreamName:              output.StreamDescription.StreamArn,
				StreamStatus:            ktypes.StreamStatus(output.StreamDescription.StreamStatus),
				StreamCreationTimestamp: output.StreamDescription.CreationRequestDateTime,
				Shards:                  make([]ktypes.Shard, len(output.StreamDescription.Shards)),
			}
		}
		for _, shard := range output.StreamDescription.Shards {
			kinesisOutput.StreamDescription.Shards = append(kinesisOutput.StreamDescription.Shards, ktypes.Shard{
				SequenceNumberRange: &ktypes.SequenceNumberRange{
					StartingSequenceNumber: shard.SequenceNumberRange.StartingSequenceNumber,
					EndingSequenceNumber:   shard.SequenceNumberRange.EndingSequenceNumber,
				},
				ShardId:       shard.ShardId,
				ParentShardId: shard.ParentShardId,
			})
		}
		kinesisOutput.ResultMetadata = output.ResultMetadata
	}
	return &kinesisOutput
}
