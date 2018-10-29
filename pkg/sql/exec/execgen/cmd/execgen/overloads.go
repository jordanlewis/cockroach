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
// permissions and limitations under the License.

package main

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var binaryOpName = map[tree.BinaryOperator]string{
	tree.Plus:  "Plus",
	tree.Minus: "Minus",
	tree.Mult:  "Mult",
	tree.Div:   "Div",
}

var comparisonOpName = map[tree.ComparisonOperator]string{
	tree.EQ: "EQ",
	tree.NE: "NE",
	tree.LT: "LT",
	tree.LE: "LE",
	tree.GT: "GT",
	tree.GE: "GE",
}

var binaryOpInfix = map[tree.BinaryOperator]string{
	tree.Plus:  "+",
	tree.Minus: "-",
	tree.Mult:  "*",
	tree.Div:   "/",
}

var binaryOpDecMethod = map[string]string{
	"+": "Add",
	"-": "Sub",
	"*": "Mul",
	"/": "Quo",
}

var comparisonOpInfix = map[tree.ComparisonOperator]string{
	tree.EQ: "==",
	tree.NE: "!=",
	tree.LT: "<",
	tree.LE: "<=",
	tree.GT: ">",
	tree.GE: ">=",
}

type assignFunc func(op overload, target, l, r string) string

type overload struct {
	Name string
	// Only one of CmpOp and BinOp will be set.
	CmpOp tree.ComparisonOperator
	BinOp tree.BinaryOperator
	// OpStr is the string form of whichever of CmpOp and BinOp are set.
	OpStr   string
	LTyp    types.T
	RTyp    types.T
	RGoType string
	RetTyp  types.T

	AssignFunc assignFunc
}

var binaryOpOverloads []*overload
var comparisonOpOverloads []*overload
var comparisonOpToOverloads map[tree.ComparisonOperator][]*overload

func (o overload) Assign(target, l, r string) string {
	if o.AssignFunc != nil {
		if ret := o.AssignFunc(o, target, l, r); ret != "" {
			return ret
		}
	}
	// Default assign form assumes an infix operator.
	return fmt.Sprintf("%s = %s %s %s", target, l, o.OpStr, r)
}

func init() {
	registerTypeCustomizers()

	// Build overload definitions for basic types.
	inputTypes := types.AllTypes
	binOps := []tree.BinaryOperator{tree.Plus, tree.Minus, tree.Mult, tree.Div}
	cmpOps := []tree.ComparisonOperator{tree.EQ, tree.NE, tree.LT, tree.LE, tree.GT, tree.GE}
	comparisonOpToOverloads = make(map[tree.ComparisonOperator][]*overload, len(comparisonOpName))
	for _, t := range inputTypes {
		customizer := typeCustomizers[t]
		for _, op := range binOps {
			// Skip types that don't have associated binary ops.
			switch t {
			case types.Bytes, types.Bool:
				continue
			}
			ov := &overload{
				Name:    binaryOpName[op],
				BinOp:   op,
				OpStr:   binaryOpInfix[op],
				LTyp:    t,
				RTyp:    t,
				RGoType: t.GoTypeName(),
				RetTyp:  t,
			}
			if customizer != nil {
				if b, ok := customizer.(binOpTypeCustomizer); ok {
					ov.AssignFunc = b.getBinOpAssignFunc()
				}
			}
			binaryOpOverloads = append(binaryOpOverloads, ov)
		}
		for _, op := range cmpOps {
			opStr := comparisonOpInfix[op]
			ov := &overload{
				Name:    comparisonOpName[op],
				CmpOp:   op,
				OpStr:   opStr,
				LTyp:    t,
				RTyp:    t,
				RGoType: t.GoTypeName(),
				RetTyp:  types.Bool,
			}
			if customizer != nil {
				if b, ok := customizer.(cmpOpTypeCustomizer); ok {
					ov.AssignFunc = b.getCmpOpAssignFunc()
				}
			}
			comparisonOpOverloads = append(comparisonOpOverloads, ov)
			comparisonOpToOverloads[op] = append(comparisonOpToOverloads[op], ov)
		}
	}
}

var typeCustomizers map[types.T]interface{}

func registerTypeCustomizer(t types.T, customizer interface{}) {
	typeCustomizers[t] = customizer
}

type binOpTypeCustomizer interface {
	getBinOpAssignFunc() assignFunc
}

type cmpOpTypeCustomizer interface {
	getCmpOpAssignFunc() assignFunc
}

type boolCustomizer struct{}
type bytesCustomizer struct{}
type decimalCustomizer struct{}

func (boolCustomizer) getCmpOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.CmpOp {
		case tree.EQ, tree.NE:
			return ""
		}
		return fmt.Sprintf("%s = tree.CompareBools(%s, %s) %s 0",
			target, l, r, op.OpStr)
	}
}

func (bytesCustomizer) getCmpOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.CmpOp {
		case tree.EQ:
			return fmt.Sprintf("%s = bytes.Equal(%s, %s)", target, l, r)
		case tree.NE:
			return fmt.Sprintf("%s = !bytes.Equal(%s, %s)", target, l, r)
		}
		return fmt.Sprintf("%s = bytes.Compare(%s, %s) %s 0",
			target, l, r, op.OpStr)
	}
}

func (decimalCustomizer) getCmpOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		return fmt.Sprintf("%s = tree.CompareDecimals(&%s, &%s) %s 0",
			target, l, r, op.OpStr)
	}
}

func (decimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		return fmt.Sprintf("if _, err := tree.DecimalCtx.%s(&%s, &%s, &%s); err != nil { panic(err) }",
			binaryOpDecMethod[op.OpStr], target, l, r)
	}
}

func registerTypeCustomizers() {
	typeCustomizers = make(map[types.T]interface{})
	registerTypeCustomizer(types.Bool, boolCustomizer{})
	registerTypeCustomizer(types.Bytes, bytesCustomizer{})
	registerTypeCustomizer(types.Decimal, decimalCustomizer{})
}
