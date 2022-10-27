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
package test

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"

	cfg "github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/metrics"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/metrics/cloudwatch"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/metrics/prometheus"
	wk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/worker"
	"github.com/vmware/vmware-go-kcl-v2/logger"
	zaplogger "github.com/vmware/vmware-go-kcl-v2/logger/zap"
)

const (
	appName         = "appName"
	streamName      = "kcl-test"
	regionName      = "us-west-2"
	workerID        = "test-worker"
	consumerName    = "enhanced-fan-out-consumer"
	kinesisEndpoint = "https://kinesis.eu-west-1.amazonaws.com"
	dynamoEndpoint  = "https://dynamodb.eu-west-1.amazonaws.com"
)

const metricsSystem = "cloudwatch"

var shardID string

func TestWorker(t *testing.T) {
	// At minimal. use standard logrus logger
	// log := logger.NewLogrusLogger(logrus.StandardLogger())
	//
	// In order to have precise control over logging. Use logger with config
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Error,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	}
	// Use logrus logger
	log := logger.NewLogrusLoggerWithConfig(config)

	kclConfig := cfg.NewKinesisClientLibConfig(appName, streamName, regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(8).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLogger(log).
		WithKinesisEndpoint(kinesisEndpoint)

	runTest(kclConfig, false, t)
}

func TestWorkerWithTimestamp(t *testing.T) {
	// In order to have precise control over logging. Use logger with config
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
	}
	// Use logrus logger
	log := logger.NewLogrusLoggerWithConfig(config)

	ts := time.Now().Add(time.Second * 5)
	kclConfig := cfg.NewKinesisClientLibConfig(appName, streamName, regionName, workerID).
		WithTimestampAtInitialPositionInStream(&ts).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLogger(log).
		WithKinesisEndpoint(kinesisEndpoint)

	runTest(kclConfig, false, t)
}

func TestWorkerWithSigInt(t *testing.T) {
	// At miminal. use standard zap logger
	//zapLogger, err := zap.NewProduction()
	//assert.Nil(t, err)
	//log := zaplogger.NewZapLogger(zapLogger.Sugar())
	//
	// In order to have precise control over logging. Use logger with config.
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: true,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	}
	// use zap logger
	log := zaplogger.NewZapLoggerWithConfig(config)

	kclConfig := cfg.NewKinesisClientLibConfig(appName, streamName, regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLogger(log).
		WithKinesisEndpoint(kinesisEndpoint)

	runTest(kclConfig, true, t)
}

func TestWorkerStatic(t *testing.T) {
	//t.Skip("Need to provide actual credentials")

	// Fill in the credentials for accessing Kinesis and DynamoDB.
	// Note: use empty string as SessionToken for long-term credentials.
	kinesisCreds := credentials.NewStaticCredentialsProvider("", "", "")
	dynamoCreds := credentials.NewStaticCredentialsProvider("", "", "")

	kclConfig := cfg.NewKinesisClientLibConfigWithCredentials(appName, streamName, regionName, workerID, &kinesisCreds, &dynamoCreds).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithKinesisEndpoint(kinesisEndpoint).
		WithDynamoDBEndpoint(dynamoEndpoint)

	runTest(kclConfig, false, t)
}

func TestWorkerAssumeRole(t *testing.T) {
	t.Skip("Need to provide actual roleARN")

	// Initial credentials loaded from SDK's default credential chain. Such as
	// the environment, shared credentials (~/.aws/credentials), or EC2 Instance
	// Role. These credentials will be used to make the STS Assume Role API.
	//sess := session.Must(session.NewSession())

	// Create the credentials from AssumeRoleProvider to assume the role
	// referenced by the "myRoleARN" ARN.
	//kinesisCreds := stscreds.NewAssumeRoleProvider(sess, "arn:aws:iam::*:role/kcl-test-publisher")
	kinesisCreds := credentials.NewStaticCredentialsProvider("", "", "")
	dynamoCreds := credentials.NewStaticCredentialsProvider("", "", "")

	kclConfig := cfg.NewKinesisClientLibConfigWithCredentials(appName, streamName, regionName, workerID, &kinesisCreds, &dynamoCreds).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithKinesisEndpoint(kinesisEndpoint).
		WithDynamoDBEndpoint(dynamoEndpoint)

	runTest(kclConfig, false, t)
}

func TestEnhancedFanOutConsumer(t *testing.T) {
	// At minimal, use standard logrus logger
	// log := logger.NewLogrusLogger(logrus.StandardLogger())
	//
	// In order to have precise control over logging. Use logger with config
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	}
	// Use logrus logger
	log := logger.NewLogrusLoggerWithConfig(config)

	kclConfig := cfg.NewKinesisClientLibConfig(appName, streamName, regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithEnhancedFanOutConsumerName(consumerName).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLogger(log)

	runTest(kclConfig, false, t)
}

func TestEnhancedFanOutConsumerDefaultConsumerName(t *testing.T) {
	// At minimal, use standard logrus logger
	// log := logger.NewLogrusLogger(logrus.StandardLogger())
	//
	// In order to have precise control over logging. Use logger with config
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	}
	// Use logrus logger
	log := logger.NewLogrusLoggerWithConfig(config)

	kclConfig := cfg.NewKinesisClientLibConfig(appName, streamName, regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithEnhancedFanOutConsumer(true).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLogger(log)

	runTest(kclConfig, false, t)
}

func TestEnhancedFanOutConsumerARN(t *testing.T) {
	t.Skip("Need to provide actual consumerARN")

	consumerARN := "arn:aws:kinesis:*:stream/kcl-test/consumer/fanout-poc-consumer-test:*"
	// At minimal, use standard logrus logger
	// log := logger.NewLogrusLogger(logrus.StandardLogger())
	//
	// In order to have precise control over logging. Use logger with config
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	}
	// Use logrus logger
	log := logger.NewLogrusLoggerWithConfig(config)

	kclConfig := cfg.NewKinesisClientLibConfig(appName, streamName, regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithEnhancedFanOutConsumerARN(consumerARN).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLogger(log)

	runTest(kclConfig, false, t)
}

func runTest(kclConfig *cfg.KinesisClientLibConfiguration, triggersig bool, t *testing.T) {
	assert.Equal(t, regionName, kclConfig.RegionName)
	assert.Equal(t, streamName, kclConfig.StreamName)

	// configure cloudwatch as metrics system
	kclConfig.WithMonitoringService(getMetricsConfig(kclConfig, metricsSystem))

	// Put some data into stream.
	kc := NewKinesisClient(t, regionName, kclConfig.KinesisEndpoint, kclConfig.KinesisCredentials)
	// publishSomeData(t, kc)
	stop := continuouslyPublishSomeData(t, kc)
	defer stop()

	worker := wk.NewWorker(recordProcessorFactory(t), kclConfig)
	err := worker.Start()
	assert.Nil(t, err)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Signal processing.
	go func() {
		sig := <-sigs
		t.Logf("Received signal %s. Exiting", sig)
		worker.Shutdown()
		// some other processing before exit.
		//os.Exit(0)
	}()

	if triggersig {
		t.Log("Trigger signal SIGINT")
		p, _ := os.FindProcess(os.Getpid())
		_ = p.Signal(os.Interrupt)
	}

	// wait a few seconds before shutdown processing
	time.Sleep(30 * time.Second)

	switch metricsSystem {
	case "prometheus":
		res, err := http.Get("http://localhost:8080/metrics")
		if err != nil {
			t.Fatalf("Error scraping Prometheus endpoint %s", err)
		}

		var parser expfmt.TextParser
		parsed, err := parser.TextToMetricFamilies(res.Body)
		_ = res.Body.Close()
		if err != nil {
			t.Errorf("Error reading monitoring response %s", err)
		}

		t.Logf("Prometheus: %+v", parsed)
	}

	t.Log("Calling normal shutdown at the end of application.")
	worker.Shutdown()
}

// configure different metrics system
func getMetricsConfig(kclConfig *cfg.KinesisClientLibConfiguration, service string) metrics.MonitoringService {

	if service == "cloudwatch" {
		return cloudwatch.NewMonitoringServiceWithOptions(kclConfig.RegionName,
			kclConfig.KinesisCredentials,
			kclConfig.Logger,
			cloudwatch.DefaultCloudwatchMetricsBufferDuration)
	}

	if service == "prometheus" {
		return prometheus.NewMonitoringService(":8080", regionName, kclConfig.Logger)
	}

	return nil
}
