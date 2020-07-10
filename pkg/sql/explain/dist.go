// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type flowWithNode struct {
	nodeID roachpb.NodeID
	flow   *execinfrapb.FlowSpec
}

// WriteDistributedExplain takes in a map from node id to FlowSpec, and writes
// a textual distributed plan to the given io.Writer.
func WriteDistributedExplain(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) (string, error) {
	sortedFlows := make([]flowWithNode, 0, len(flows))
	for nodeID, flow := range flows {
		sortedFlows = append(sortedFlows, flowWithNode{nodeID: nodeID, flow: flow})
	}
	// Sort backward, since the first thing you add to a treeprinter will come last.
	sort.Slice(sortedFlows, func(i, j int) bool { return sortedFlows[i].nodeID < sortedFlows[j].nodeID })
	tp := treeprinter.NewWithIndent(false /* leftPad */, true /* rightPad */, 0 /* edgeLength */)
	root := tp.Child("")

	inputStreamMap := make(map[execinfrapb.StreamID]*execinfrapb.ProcessorSpec)
	for _, flow := range flows {
		for _, processor := range flow.Processors {
			for _, input := range processor.Input {
				for _, inputStream := range input.Streams {
					inputStreamMap[inputStream.StreamID] = &processor
				}
			}
		}
	}

	q := make([]*execinfrapb.ProcessorSpec, 0)
	for nodeID, flow := range flows {
		node := root.Childf("Node %d", nodeID)
		q = q[:0]
		for i := range flow.Processors {
			processor := &flow.Processors[i]
			if len(processor.Output) == 0 {
				printOutput(inputStreamMap, processor, node)
				// We have a leaf processor (with no further outputs), so add it to the
				// queue of processors to print out about.
				q = append(q, processor)
			}
		}
	}

	return root.String(), nil
}

func printOutput(
	streamMap map[execinfrapb.StreamID]*execinfrapb.ProcessorSpec,
	processor *execinfrapb.ProcessorSpec,
	tp treeprinter.Node,
) {
	title, summary := processor.Core.GetValue().(execinfrapb.DiagramCellType).Summary()
	node := tp.Child(title)
	for _, s := range summary {
		node.AddLine(s)
	}
	for i := range processor.Input {
		input := &processor.Input[i]
		for j := range input.Streams {
			streamID := input.Streams[j].StreamID
			parent := streamMap[streamID]
			printOutput(streamMap, parent, node)
		}
	}
}
