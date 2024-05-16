// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opgen

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register(
		(*scpb.Namespace)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Namespace) *scop.SetNameInDescriptor {
					if strings.HasPrefix(this.Name, catconstants.PgTempSchemaName) {
						return nil
					}
					return &scop.SetNameInDescriptor{
						DescriptorID: this.DescriptorID,
						Name:         this.Name,
					}
				}),
				emit(func(this *scpb.Namespace) *scop.AddDescriptorName {
					return &scop.AddDescriptorName{
						Namespace: *protoutil.Clone(this).(*scpb.Namespace),
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.Namespace) *scop.DrainDescriptorName {
					return &scop.DrainDescriptorName{
						Namespace: *protoutil.Clone(this).(*scpb.Namespace),
					}
				}),
			),
		),
	)
}
