// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// VectorizedStatsCollector collects VectorizedStats on Operators.
//
// If two Operators are connected (i.e. one is an input to another), the
// corresponding VectorizedStatsCollectors are also "connected" by sharing a
// StopWatch.
type VectorizedStatsCollector struct {
	colexecbase.Operator
	NonExplainable
	execpb.VectorizedStats
	idTagKey string

	// inputWatch is a single stop watch that is shared with all the input
	// Operators. If the Operator doesn't have any inputs (like colBatchScan),
	// it is not shared with anyone. It is used by the wrapped Operator to
	// measure its stall or execution time.
	inputWatch *timeutil.StopWatch

	// parentStatsCollector is a pointer to the stats collector of the parent
	// operator of this one.
	parentStatsCollector *VectorizedStatsCollector

	childStatsCollectors []*VectorizedStatsCollector

	memMonitors  []*mon.BytesMonitor
	diskMonitors []*mon.BytesMonitor
}

var _ colexecbase.Operator = &VectorizedStatsCollector{}

// NewVectorizedStatsCollector creates a new VectorizedStatsCollector which
// wraps 'op' that corresponds to a component with either ProcessorID or
// StreamID 'id' (with 'idTagKey' distinguishing between the two). 'isStall'
// indicates whether stall or execution time is being measured. 'inputWatch'
// must be non-nil.
func NewVectorizedStatsCollector(
	op colexecbase.Operator,
	id int32,
	idTagKey string,
	isStall bool,
	inputWatch *timeutil.StopWatch,
	memMonitors []*mon.BytesMonitor,
	diskMonitors []*mon.BytesMonitor,
) *VectorizedStatsCollector {
	if inputWatch == nil {
		colexecerror.InternalError("input watch for VectorizedStatsCollector is nil")
	}
	return &VectorizedStatsCollector{
		Operator:        op,
		VectorizedStats: execpb.VectorizedStats{ID: id, Stall: isStall},
		idTagKey:        idTagKey,
		inputWatch:      inputWatch,
		memMonitors:     memMonitors,
		diskMonitors:    diskMonitors,
	}
}

// SetParentStatsCollector sets the stats collectors of the parents of this
// operator.
// For example, consider the following tree:
//
// TableReader        TableReader
// StatsCollector1    StatsCollector2
//       \                /
//          JoinOperator
//          StatsCollector3
//
// The intended use of this method is to call
// SetParentStatsCollector on StatsCollector1 and StatsCollector2 with
// StatsCollector3.
func (vsc *VectorizedStatsCollector) SetParentStatsCollector(
	parentStatCollector *VectorizedStatsCollector,
) {
	vsc.parentStatsCollector = parentStatCollector
}

func (vsc *VectorizedStatsCollector) SetChildStatsCollectors(
	collectors []*VectorizedStatsCollector,
) {
	vsc.childStatsCollectors = collectors
}

// Next is part of Operator interface.
func (vsc *VectorizedStatsCollector) Next(ctx context.Context) coldata.Batch {
	if vsc.parentStatsCollector != nil {
		// vsc.outputWatch is non-nil which means that this Operator is outputting
		// the batches into another one. In order to avoid double counting the time
		// actually spent in the current "input" Operator, we're stopping the stop
		// watch of the other "output" Operator before doing any computations here.
		vsc.parentStatsCollector.inputWatch.Stop()
	}

	var batch coldata.Batch
	if vsc.VectorizedStats.Stall {
		// We're measuring stall time, so there are no inputs into the wrapped
		// Operator, and we need to start the stop watch ourselves.
		vsc.inputWatch.Start()
	}
	// Do the work of our wrapped operator.
	batch = vsc.Operator.Next(ctx)
	if batch.Length() > 0 {
		vsc.NumBatches++
		vsc.NumTuples += int64(batch.Length())
	}

	numOutputTuplesFromParents := int64(0)
	for i := range vsc.childStatsCollectors {
		numOutputTuplesFromParents += vsc.childStatsCollectors[i].NumTuples
	}
	vsc.NumInputTuples = numOutputTuplesFromParents

	vsc.inputWatch.Stop()
	if vsc.parentStatsCollector != nil {
		// vsc.outputWatch is non-nil which means that this Operator is outputting
		// the batches into another one. To allow for measuring the execution time
		// of that other Operator, we're starting the stop watch right before
		// returning batch.
		vsc.parentStatsCollector.inputWatch.Start()
	}
	return batch
}

// finalizeStats records the time measured by the stop watch into the stats as
// well as the memory and disk usage.
func (vsc *VectorizedStatsCollector) finalizeStats() {
	vsc.Time = vsc.inputWatch.Elapsed()
	for _, memMon := range vsc.memMonitors {
		vsc.MaxAllocatedMem += memMon.MaximumBytes()
	}
	for _, diskMon := range vsc.diskMonitors {
		vsc.MaxAllocatedDisk += diskMon.MaximumBytes()
	}
}

// OutputStats outputs the vectorized stats collected by vsc into ctx.
func (vsc *VectorizedStatsCollector) OutputStats(
	ctx context.Context, flowID string, deterministicStats bool,
) {
	if vsc.ID < 0 {
		// Ignore this stats collector since it is not associated with any
		// component.
		return
	}
	// We're creating a new span for every component setting the appropriate
	// tag so that it is displayed correctly on the flow diagram.
	// TODO(yuzefovich): these spans are created and finished right away which
	// is not the way they are supposed to be used, so this should be fixed.
	_, span := tracing.ChildSpan(ctx, fmt.Sprintf("%T", vsc.Operator))
	span.SetTag(execinfrapb.FlowIDTagKey, flowID)
	span.SetTag(vsc.idTagKey, vsc.ID)
	vsc.finalizeStats()
	if deterministicStats {
		vsc.VectorizedStats.Time = 0
		vsc.MaxAllocatedMem = 0
		vsc.MaxAllocatedDisk = 0
		vsc.NumBatches = 0
	}
	tracing.SetSpanStats(span, &vsc.VectorizedStats)
	span.Finish()
}
