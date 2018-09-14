package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type propValidator struct {
	plan planNode

	spec *distsqlrun.PropValidatorSpec
}

func (propValidator) Next(params runParams) (bool, error) {
	panic("implement me")
}

func (propValidator) Values() tree.Datums {
	panic("implement me")
}

func (propValidator) Close(ctx context.Context) {
	panic("implement me")
}
