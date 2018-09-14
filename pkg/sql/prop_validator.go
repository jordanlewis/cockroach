package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type propValidator struct {
	plan planNode

	spec *distsqlrun.Props
}

func (p *propValidator) Next(params runParams) (bool, error) {
	return p.plan.Next(params)
}

func (p *propValidator) Values() tree.Datums {
	return p.plan.Values()
}

func (p *propValidator) Close(ctx context.Context) {
	p.plan.Close(ctx)
}
