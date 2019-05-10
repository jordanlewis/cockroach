// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package tpch

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"golang.org/x/exp/rand"
)

const (
	numNation           = 25
	numRegion           = 5
	numPartPerSF        = 200000
	numPartSuppPerPart  = 4
	numSupplierPerSF    = 10000
	numCustomerPerSF    = 150000
	numOrderPerCustomer = 10
	numLineItemPerSF    = 6001215
)

type tpch struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed        uint64
	scaleFactor int

	distsql bool

	queriesRaw      string
	selectedQueries []string

	textPool   []byte
	localsPool *sync.Pool
}

func init() {
	workload.Register(tpchMeta)
}

// FromWarehouses returns a tpch generator pre-configured with the specified
// scale factor.
func FromScaleFactor(scaleFactor int) workload.Generator {
	return workload.FromFlags(tpchMeta, fmt.Sprintf(`--scale-factor=%d`, scaleFactor))
}

var tpchMeta = workload.Meta{
	Name:        `tpch`,
	Description: `TPC-H is a read-only workload of "analytics" queries on large datasets.`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &tpch{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcc`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`queries`:  {RuntimeOnly: true},
			`dist-sql`: {RuntimeOnly: true},
		}
		g.flags.Uint64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.scaleFactor, `scale-factor`, 1,
			`Linear scale of how much data to use (each SF is ~1GB)`)
		g.flags.StringVar(&g.queriesRaw, `queries`, `1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22`,
			`Queries to run. Use a comma separated list of query numbers`)
		g.flags.BoolVar(&g.distsql, `dist-sql`, true, `Use DistSQL for query execution`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*tpch) Meta() workload.Meta { return tpchMeta }

// Flags implements the Flagser interface.
func (w *tpch) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *tpch) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			for _, queryName := range strings.Split(w.queriesRaw, `,`) {
				if _, ok := queriesByName[queryName]; !ok {
					return errors.Errorf(`unknown query: %s`, queryName)
				}
				w.selectedQueries = append(w.selectedQueries, queryName)
			}
			return nil
		},
	}
}

type generateLocals struct {
	rng *rand.Rand

	// namePerm is a slice of ordinals into randPartNames.
	namePerm []int

	orderData *orderSharedRandomData
}

func intToDate(i interface{}) interface{} {
	i64 := i.(int64)
	d, err := pgdate.MakeDateFromUnixEpoch(i64)
	if err != nil {
		panic(err)
	}
	return d.String()
}

// Tables implements the Generator interface.
func (w *tpch) Tables() []workload.Table {
	if w.localsPool == nil {
		w.localsPool = &sync.Pool{
			New: func() interface{} {
				namePerm := make([]int, len(randPartNames))
				for i := range namePerm {
					namePerm[i] = i
				}
				return &generateLocals{
					rng:      rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano()))),
					namePerm: namePerm,
					orderData: &orderSharedRandomData{
						partKeys:   make([]int, 0, 7),
						shipDates:  make([]int64, 0, 7),
						quantities: make([]float32, 0, 7),
						discount:   make([]float32, 0, 7),
						tax:        make([]float32, 0, 7),
					},
				}
			},
		}
	}

	var err error
	w.textPool, err = ioutil.ReadFile("/Users/jordan/go/src/github.com/cockroachdb/cockroach/pool.txt")
	if err != nil {
		panic(fmt.Sprintf("Couldn't open pool.txt: %v", err))
	}
	nation := workload.Table{
		Name:   `nation`,
		Schema: tpchNationSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numNation,
			FillBatch:  w.tpchNationInitialRowBatch,
		},
	}
	region := workload.Table{
		Name:   `region`,
		Schema: tpchRegionSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numRegion,
			FillBatch:  w.tpchRegionInitialRowBatch,
		},
	}
	supplier := workload.Table{
		Name:   `supplier`,
		Schema: tpchSupplierSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numSupplierPerSF * w.scaleFactor,
			FillBatch:  w.tpchSupplierInitialRowBatch,
		},
	}
	part := workload.Table{
		Name:   `part`,
		Schema: tpchPartSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numPartPerSF * w.scaleFactor,
			FillBatch:  w.tpchPartInitialRowBatch,
		},
	}
	partsupp := workload.Table{
		Name:   `partsupp`,
		Schema: tpchPartSuppSchema,
		InitialRows: workload.BatchedTuples{
			// We'll do 1 batch per part, hence numPartPerSF and not numPartSuppPerSF.
			NumBatches: numPartPerSF * w.scaleFactor,
			FillBatch:  w.tpchPartSuppInitialRowBatch,
		},
	}
	customer := workload.Table{
		Name:   `customer`,
		Schema: tpchCustomerSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numCustomerPerSF * w.scaleFactor,
			FillBatch:  w.tpchCustomerInitialRowBatch,
		},
	}
	orders := workload.Table{
		Name:   `orders`,
		Schema: tpchOrdersSchema,
		InitialRows: workload.BatchedTuples{
			// 1 batch per customer.
			NumBatches: numCustomerPerSF * w.scaleFactor,
			FillBatch:  w.tpchOrdersInitialRowBatch,
			DataTransformers: map[int]func(interface{}) interface{}{
				// o_orderdate
				4: intToDate,
			},
		},
	}
	lineitem := workload.Table{
		Name:   `lineitem`,
		Schema: tpchLineItemSchema,
		InitialRows: workload.BatchedTuples{
			// 1 batch per customer.
			NumBatches: numCustomerPerSF * w.scaleFactor,
			FillBatch:  w.tpchLineItemInitialRowBatch,
			DataTransformers: map[int]func(interface{}) interface{}{
				// l_shipdate, l_commitdate, l_reciptdate
				10: intToDate,
				11: intToDate,
				12: intToDate,
			},
		},
	}

	return []workload.Table{
		nation, region, part, supplier, partsupp, customer, orders, lineitem,
	}
}

// Ops implements the Opser interface.
func (w *tpch) Ops(urls []string, reg *histogram.Registry) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		worker := &worker{
			config: w,
			hists:  reg.GetHandle(),
			db:     db,
		}
		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}
	return ql, nil
}

type worker struct {
	config *tpch
	hists  *histogram.Histograms
	db     *gosql.DB
	ops    int
}

func (w *worker) run(ctx context.Context) error {
	queryName := w.config.selectedQueries[w.ops%len(w.config.selectedQueries)]
	w.ops++

	var query string
	if w.config.distsql {
		query = `SET DISTSQL = 'always'; ` + queriesByName[queryName]
	} else {
		query = `SET DISTSQL = 'off'; ` + queriesByName[queryName]
	}

	start := timeutil.Now()
	rows, err := w.db.Query(query)
	if err != nil {
		return err
	}
	var numRows int
	for rows.Next() {
		numRows++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	elapsed := timeutil.Since(start)
	w.hists.Get(queryName).Record(elapsed)
	log.Infof(ctx, "[%s] return %d rows after %4.2f seconds:\n  %s",
		queryName, numRows, elapsed.Seconds(), query)
	return nil
}

const (
	tpchNationSchema = `(
		n_nationkey       INTEGER NOT NULL PRIMARY KEY,
		n_name            CHAR(25) NOT NULL,
		n_regionkey       INTEGER NOT NULL,
		n_comment         VARCHAR(152),
		INDEX n_rk (n_regionkey ASC)
	)`
	tpchRegionSchema = `(
		r_regionkey       INTEGER NOT NULL PRIMARY KEY,
		r_name            CHAR(25) NOT NULL,
		r_comment         VARCHAR(152)
	)`
	tpchPartSchema = `(
		p_partkey         INTEGER NOT NULL PRIMARY KEY,
		p_name            VARCHAR(55) NOT NULL,
		p_mfgr            CHAR(25) NOT NULL,
		p_brand           CHAR(10) NOT NULL,
		p_type            VARCHAR(25) NOT NULL,
		p_size            INTEGER NOT NULL,
		p_container       CHAR(10) NOT NULL,
		p_retailprice     FLOAT NOT NULL,
		p_comment         VARCHAR(23) NOT NULL
	)`
	tpchSupplierSchema = `(
		s_suppkey         INTEGER NOT NULL PRIMARY KEY,
		s_name            CHAR(25) NOT NULL,
		s_address         VARCHAR(40) NOT NULL,
		s_nationkey       INTEGER NOT NULL,
		s_phone           CHAR(15) NOT NULL,
		s_acctbal         FLOAT NOT NULL,
		s_comment         VARCHAR(101) NOT NULL,
		INDEX s_nk (s_nationkey ASC)
	)`
	tpchPartSuppSchema = `(
		ps_partkey            INTEGER NOT NULL,
		ps_suppkey            INTEGER NOT NULL,
		ps_availqty           INTEGER NOT NULL,
		ps_supplycost         FLOAT NOT NULL,
		ps_comment            VARCHAR(199) NOT NULL,
		PRIMARY KEY (ps_partkey ASC, ps_suppkey ASC),
		INDEX ps_sk (ps_suppkey ASC)
	)`
	tpchCustomerSchema = `(
		c_custkey         INTEGER NOT NULL PRIMARY KEY,
		c_name            VARCHAR(25) NOT NULL,
		c_address         VARCHAR(40) NOT NULL,
		c_nationkey       INTEGER NOT NULL,
		c_phone           CHAR(15) NOT NULL,
		c_acctbal         FLOAT NOT NULL,
		c_mktsegment      CHAR(10) NOT NULL,
		c_comment         VARCHAR(117) NOT NULL,
		INDEX c_nk (c_nationkey ASC)
	)`
	tpchOrdersSchema = `(
		o_orderkey           INTEGER NOT NULL PRIMARY KEY,
		o_custkey            INTEGER NOT NULL,
		o_orderstatus        CHAR(1) NOT NULL,
		o_totalprice         FLOAT NOT NULL,
		o_orderdate          DATE NOT NULL,
		o_orderpriority      CHAR(15) NOT NULL,
		o_clerk              CHAR(15) NOT NULL,
		o_shippriority       INTEGER NOT NULL,
		o_comment            VARCHAR(79) NOT NULL,
		INDEX o_ck (o_custkey ASC),
		INDEX o_od (o_orderdate ASC)
	)`
	tpchLineItemSchema = `(
		l_orderkey      INTEGER NOT NULL,
		l_partkey       INTEGER NOT NULL,
		l_suppkey       INTEGER NOT NULL,
		l_linenumber    INTEGER NOT NULL,
		l_quantity      FLOAT NOT NULL,
		l_extendedprice FLOAT NOT NULL,
		l_discount      FLOAT NOT NULL,
		l_tax           FLOAT NOT NULL,
		l_returnflag    CHAR(1) NOT NULL,
		l_linestatus    CHAR(1) NOT NULL,
		l_shipdate      DATE NOT NULL,
		l_commitdate    DATE NOT NULL,
		l_receiptdate   DATE NOT NULL,
		l_shipinstruct  CHAR(25) NOT NULL,
		l_shipmode      CHAR(10) NOT NULL,
		l_comment       VARCHAR(44) NOT NULL,
		PRIMARY KEY (l_orderkey, l_linenumber),
		INDEX l_ok (l_orderkey ASC),
		INDEX l_pk (l_partkey ASC),
		INDEX l_sk (l_suppkey ASC),
		INDEX l_sd (l_shipdate ASC),
		INDEX l_cd (l_commitdate ASC),
		INDEX l_rd (l_receiptdate ASC),
		INDEX l_pk_sk (l_partkey ASC, l_suppkey ASC),
		INDEX l_sk_pk (l_suppkey ASC, l_partkey ASC)
	)`
)
