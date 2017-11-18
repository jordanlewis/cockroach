// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package batcheval

import (
	"golang.org/x/net/context"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

// Scan scans the key range specified by start key through end key
// in ascending order up to some maximum number of results. maxKeys
// stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func Scan(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ScanResponse)

	var rows []roachpb.KeyValue
	var prefix roachpb.Key
	var resumeSpan *roachpb.Span
	var intents []roachpb.Intent
	var err error
	if args.WantPrefix {
		fmt.Println("Doing the prefix thing son")
		rows, prefix, resumeSpan, intents, err = engine.MVCCScanWithPrefix(ctx, batch, args.Key, args.EndKey,
			cArgs.MaxKeys, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	} else {
		rows, resumeSpan, intents, err = engine.MVCCScan(ctx, batch, args.Key, args.EndKey,
			cArgs.MaxKeys, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	}
	if err != nil {
		return result.Result{}, err
	}

	reply.NumKeys = int64(len(rows))
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}
	if len(prefix) != 0 {
		reply.Prefix = prefix
	}
	reply.Rows = rows
	if args.ReturnIntents {
		reply.IntentRows, err = CollectIntentRows(ctx, batch, cArgs, intents)
	}
	return result.FromIntents(intents, args, true /* alwaysReturn */), err
}
