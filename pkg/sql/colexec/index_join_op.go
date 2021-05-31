// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
)

type indexJoinOp struct {
	colexecop.OneInputNode
}

func (i indexJoinOp) Init(ctx context.Context) {
	panic("implement me")
}

func (i indexJoinOp) Next() coldata.Batch {
	b := i.Input.Next()
	panic("implement me")
}
