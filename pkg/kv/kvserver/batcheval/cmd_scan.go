// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
)

func init() {
	RegisterReadOnlyCommand(roachpb.Scan, DefaultDeclareIsolatedKeys, Scan)
}

// Scan scans the key range specified by start key through end key
// in ascending order up to some maximum number of results. maxKeys
// stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func Scan(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ScanResponse)

	var res result.Result
	var scanRes storage.MVCCScanResult
	var err error

	avoidExcess := cArgs.EvalCtx.ClusterSettings().Version.IsActive(ctx,
		clusterversion.TargetBytesAvoidExcess)
	opts := storage.MVCCScanOptions{
		Inconsistent:           h.ReadConsistency != roachpb.CONSISTENT,
		Txn:                    h.Txn,
		Uncertainty:            cArgs.Uncertainty,
		MaxKeys:                h.MaxSpanRequestKeys,
		MaxIntents:             storage.MaxIntentsPerWriteIntentError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		TargetBytes:            h.TargetBytes,
		TargetBytesAvoidExcess: h.AllowEmpty || avoidExcess, // AllowEmpty takes precedence
		AllowEmpty:             h.AllowEmpty,
		WholeRowsOfSize:        h.WholeRowsOfSize,
		FailOnMoreRecent:       args.KeyLocking != lock.None,
		Reverse:                false,
		MemoryAccount:          cArgs.EvalCtx.GetResponseMemoryAccount(),
	}

	switch args.ScanFormat {
	case roachpb.BATCH_RESPONSE:
		scanRes, err = storage.MVCCScanToBytes(
			ctx, reader, args.Key, args.EndKey, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.BatchResponses = scanRes.KVData
	case roachpb.KEY_VALUES:
		scanRes, err = storage.MVCCScan(
			ctx, reader, args.Key, args.EndKey, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.Rows = scanRes.KVs
	case roachpb.COL_BATCH_RESPONSE:
		var msg proto.Message
		var da types.DynamicAny
		if err := types.UnmarshalAny(args.ScanSpec.ScanSpec, &da); err != nil {
			return result.Result{}, err
		}
		msg = da.Message
		/*
			switch ss := args.ScanSpec.ScanSpec.(type) {
			case *types.Any:
				var da types.DynamicAny
				if err := types.UnmarshalAny(ss, &da); err != nil {
					return result.Result{}, err
				}
				msg = da.Message
			case *execinfrapb.ProcessorSpec:
				msg = ss
			}

		*/
		scanRes, err = storage.MVCCScanToCols(
			ctx, args.TenantId, reader, msg,
			args.Key, args.EndKey, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		if scanRes.KVData != nil {
			reply.BatchResponses = scanRes.KVData
		}
		if scanRes.ColBatches != nil {
			reply.ColBatches.ColBatches = make([]interface{}, len(scanRes.ColBatches))
			for i := range scanRes.ColBatches {
				reply.ColBatches.ColBatches[i] = scanRes.ColBatches[i]
			}
		}
	default:
		panic(fmt.Sprintf("Unknown scanFormat %d", args.ScanFormat))
	}

	reply.NumKeys = scanRes.NumKeys
	reply.NumBytes = scanRes.NumBytes

	if scanRes.ResumeSpan != nil {
		reply.ResumeSpan = scanRes.ResumeSpan
		reply.ResumeReason = scanRes.ResumeReason
		reply.ResumeNextBytes = scanRes.ResumeNextBytes
	}

	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		// NOTE: MVCCScan doesn't use a Prefix iterator, so we don't want to use
		// one in CollectIntentRows either so that we're guaranteed to use the
		// same cached iterator and observe a consistent snapshot of the engine.
		const usePrefixIter = false
		reply.IntentRows, err = CollectIntentRows(ctx, reader, usePrefixIter, scanRes.Intents)
		if err != nil {
			return result.Result{}, err
		}
	}

	if args.KeyLocking != lock.None && h.Txn != nil {
		err = acquireUnreplicatedLocksOnKeys(&res, h.Txn, args.ScanFormat, &scanRes)
		if err != nil {
			return result.Result{}, err
		}
	}
	res.Local.EncounteredIntents = scanRes.Intents
	return res, nil
}
