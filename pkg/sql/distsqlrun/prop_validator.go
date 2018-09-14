package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type propValidator struct {
	ProcessorBase
	input RowSource

	spec *PropValidatorSpec
}

var _ Processor = &propValidator{}
var _ RowSource = &propValidator{}

const propValidatorName = "propValidator"

func newPropValidator(
	flowCtx *FlowCtx,
	processorID int32,
	spec *PropValidatorSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*propValidator, error) {
	n := &propValidator{input: input, spec: spec}
	if err := n.Init(
		n,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		ProcStateOpts{InputsToDrain: []RowSource{n.input}},
	); err != nil {
		return nil, err
	}
	return n, nil
}

// Start is part of the RowSource interface.
func (n *propValidator) Start(ctx context.Context) context.Context {
	n.input.Start(ctx)
	return n.StartInternal(ctx, propValidatorName)
}

// Next is part of the RowSource interface.
func (n *propValidator) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for n.State == StateRunning {
		row, meta := n.input.Next()

		if meta != nil {
			if meta.Err != nil {
				n.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			n.MoveToDraining(nil /* err */)
			break
		}

		if outRow := n.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, n.DrainHelper()
}

// ConsumerDone is part of the RowSource interface.
func (n *propValidator) ConsumerDone() {
	n.MoveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (n *propValidator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	n.InternalClose()
}
