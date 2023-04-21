package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/sirupsen/logrus"
	kcl_config "github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	kcl_interfaces "github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
	kcl_worker "github.com/vmware/vmware-go-kcl-v2/clientlibrary/worker"
)

type SampleConfig struct {
	Stream                  string `json:"stream"`
	Application             string `json:"application"`
	Region                  string `json:"region"`
	WorkerID                string `json:"worker_id"`
	CheckpointInterval      int    `json:"checkpoint_interval"`
	CheckpointRetries       int    `json:"checkpoint_retries"`
	CheckpointRetryInterval int    `json:"checkpoint_retry_interval"`
}

type RecordProcessor struct {
	CheckpointInterval      time.Duration
	CheckpointRetryInterval time.Duration
	CheckpointRetries       int
	LastCheckpoint          time.Time
	LargestSequenceNumber   *big.Int
}

// Called for each record that is pass to ProcessRecords
func (p *RecordProcessor) processRecord(data []byte, partitionKey string, sequenceNumber *big.Int) {
	// Insert your processing logic here
	logrus.Infof(
		"Record (Partition Key: %v, Sequence Number: %d, Data Size: %v)",
		partitionKey,
		sequenceNumber,
		len(data),
	)
}

// Mostly useless, but copied from official AWS KCL Python example
func (p *RecordProcessor) shouldUpdateSequence(sequenceNumber *big.Int) bool {
	return sequenceNumber.Cmp(p.LargestSequenceNumber) > 0
}

// Checkpoints with retries on retryable exceptions
func (p *RecordProcessor) checkpoint(checkpointer kcl_interfaces.IRecordProcessorCheckpointer, sequenceNumber *big.Int) {
	// Convert the integer sequence number back to an AWS string
	seq := aws.String(fmt.Sprintf("%d", sequenceNumber))

	// Try to checkpoint
	for n := 0; n < p.CheckpointRetries; n++ {
		// NOTE: I don't know how to distinguish between retryable and non-retryable errors here
		err := checkpointer.Checkpoint(seq)
		if err != nil {
			logrus.Warnf("error while checkpointing: %v", err)
			time.Sleep(p.CheckpointRetryInterval)
			continue
		} else {
			// Checkpoint successful, so we are done
			p.LastCheckpoint = time.Now()
			return
		}
	}

	// We failed over all retries
	logrus.Warnf("all checkpoint retries failed")
}

// IRecordProcessor implementation of ProcessRecords. Called for all incoming
// batches of records from Kinesis by the KCL
func (p *RecordProcessor) ProcessRecords(input *kcl_interfaces.ProcessRecordsInput) {
	for _, record := range input.Records {
		// Parse the sequence number to an integer
		seq := big.NewInt(0)
		seq, ok := seq.SetString(*record.SequenceNumber, 10)
		if !ok || seq == nil {
			logrus.Infof(
				"error: faield to parse sequence number to int: %v",
				*record.SequenceNumber,
			)
		}

		// Process the record
		p.processRecord(record.Data, *record.PartitionKey, seq)

		if p.shouldUpdateSequence(seq) {
			p.LargestSequenceNumber = seq
		}
	}

	if time.Since(p.LastCheckpoint) >= p.CheckpointInterval {
		p.checkpoint(input.Checkpointer, p.LargestSequenceNumber)
	}
}

func (p *RecordProcessor) Initialize(input *kcl_interfaces.InitializationInput) {
	p.LargestSequenceNumber = big.NewInt(0)
	p.LastCheckpoint = time.Now()
}

func (p *RecordProcessor) Shutdown(input *kcl_interfaces.ShutdownInput) {
	if input.ShutdownReason != kcl_interfaces.TERMINATE {
		return
	} else if err := input.Checkpointer.Checkpoint(nil); err != nil {
		logrus.Errorf("shutdown checkpoint failed: %v", err)
	}
}

// A simple processor factory which wraps a function in the
// appropriate KCL interface
type RecordProcessorFactory func() kcl_interfaces.IRecordProcessor

func (factory RecordProcessorFactory) CreateProcessor() kcl_interfaces.IRecordProcessor {
	return factory()
}

func main() {
	if len(os.Args) != 2 {
		logrus.Fatalf("usage: %v path/to/config.json", os.Args[0])
	}

	hostname, err := os.Hostname()
	if err != nil {
		logrus.Fatalf("failed to get worker hostname: %v", err)
	}

	// Default sample configuration
	sampleConfig := SampleConfig{
		WorkerID:                hostname,
		CheckpointInterval:      60,
		CheckpointRetries:       5,
		CheckpointRetryInterval: 5,
	}

	// Load configuration
	stream, err := os.Open(os.Args[1])
	if err != nil {
		logrus.Fatalf("%v: could not open config: %v", os.Args[1], err)
	}

	// Ensure we close the config
	defer stream.Close()

	if err := json.NewDecoder(stream).Decode(&sampleConfig); err != nil {
		logrus.Fatalf("%v: could not parse config: %v", os.Args[1], err)
	}

	// Create the KCL configuration from our sample config
	config := kcl_config.NewKinesisClientLibConfig(
		sampleConfig.Application,
		sampleConfig.Stream,
		sampleConfig.Region,
		sampleConfig.WorkerID,
	)

	// Create our record processor factory
	factory := RecordProcessorFactory(func() kcl_interfaces.IRecordProcessor {
		return &RecordProcessor{
			CheckpointInterval:      time.Duration(sampleConfig.CheckpointInterval) * time.Second,
			CheckpointRetries:       sampleConfig.CheckpointRetries,
			CheckpointRetryInterval: time.Duration(sampleConfig.CheckpointRetryInterval) * time.Second,
			LastCheckpoint:          time.Now(),
			LargestSequenceNumber:   big.NewInt(0),
		}
	})

	// Create the KCL worker
	worker := kcl_worker.NewWorker(factory, config)

	logrus.Infof(
		"Starting KCL Worker (Application Name: %v, Stream Name: %v, Worker ID: %v)",
		sampleConfig.Application,
		sampleConfig.Stream,
		sampleConfig.WorkerID,
	)

	// Start the KCL worker
	if err := worker.Start(); err != nil {
		logrus.Fatalf("failed to start kcl worker: %v", err)
	}

	// Ensure we shutdown
	defer worker.Shutdown()

	// Wait for an exit signal
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, os.Interrupt, os.Kill)
	<-sigchan
}
