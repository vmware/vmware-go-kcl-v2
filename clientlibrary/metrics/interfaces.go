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

// Package metrics
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package metrics

type MonitoringService interface {
	Init(appName, streamName, workerID string) error
	Start() error
	IncrRecordsProcessed(shard string, count int)
	IncrBytesProcessed(shard string, count int64)
	MillisBehindLatest(shard string, milliSeconds float64)
	DeleteMetricMillisBehindLatest(shard string)
	LeaseGained(shard string)
	LeaseLost(shard string)
	LeaseRenewed(shard string)
	RecordGetRecordsTime(shard string, time float64)
	RecordProcessRecordsTime(shard string, time float64)
	Shutdown()
}

// NoopMonitoringService implements MonitoringService by does nothing.
type NoopMonitoringService struct{}

func (NoopMonitoringService) Init(_, _, _ string) error { return nil }
func (NoopMonitoringService) Start() error              { return nil }
func (NoopMonitoringService) Shutdown()                 {}

func (NoopMonitoringService) IncrRecordsProcessed(_ string, _ int)         {}
func (NoopMonitoringService) IncrBytesProcessed(_ string, _ int64)         {}
func (NoopMonitoringService) MillisBehindLatest(_ string, _ float64)       {}
func (NoopMonitoringService) DeleteMetricMillisBehindLatest(_ string)      {}
func (NoopMonitoringService) LeaseGained(_ string)                         {}
func (NoopMonitoringService) LeaseLost(_ string)                           {}
func (NoopMonitoringService) LeaseRenewed(_ string)                        {}
func (NoopMonitoringService) RecordGetRecordsTime(_ string, _ float64)     {}
func (NoopMonitoringService) RecordProcessRecordsTime(_ string, _ float64) {}
