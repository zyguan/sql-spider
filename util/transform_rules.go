package util

import (
	"fmt"
	"math/rand"
)

type TransformContext struct {
	Cols []Expr
	ReplaceChildIdx int
}

type TransformRule interface {
	OneStep(node Expr, ctx TransformContext) []Expr
}

var rules []TransformRule

type ConstantToColumn struct {}

func (c *ConstantToColumn) OneStep(node Expr, ctx TransformContext) []Expr {
	var result []Expr
	switch node.(type) {
	case *Func:
	case *Column:
	case *Constant:
		for _, col := range ctx.Cols {
			result = append(result, col)
		}
	}
	return result
}

type ColumnToConstant struct {}

func (c *ColumnToConstant) OneStep(node Expr, ctx TransformContext) []Expr {
	var result []Expr
	switch node.(type) {
	case *Func:
	case *Constant:
	case *Column:
		result = append(result, GenConstant(TypeMask(node.RetType())))
	}
	return result
}

type ReplaceChildToConstant struct {}

func (r *ReplaceChildToConstant) OneStep(node Expr, ctx TransformContext) []Expr {
	var result []Expr
	switch e := node.(type) {
	case *Constant:
	case *Column:
	case *Func:
		fmt.Printf("ReplaceChildToConst child size:%d id:%d\n", len(e.children), ctx.ReplaceChildIdx)
		if len(e.children) > ctx.ReplaceChildIdx {
			newNode := e.Clone().(*Func)
			newNode.children[ctx.ReplaceChildIdx] = GenConstant(TypeMask(newNode.children[ctx.ReplaceChildIdx].RetType()))
			result = append(result, newNode)
		}
	}
	return result
}

type ReplaceChildToColumn struct {}

func (r *ReplaceChildToColumn) OneStep(node Expr, ctx TransformContext) []Expr {
	var result []Expr
	switch e := node.(type) {
	case *Constant:
	case *Column:
	case *Func:
		fmt.Printf("ReplaceChildToColumn child size:%d col size:%d id:%d\n", len(e.children), len(ctx.Cols), ctx.ReplaceChildIdx)
		if len(e.children) > ctx.ReplaceChildIdx {
			newNode := e.Clone().(*Func)
			newNode.children[ctx.ReplaceChildIdx] = ctx.Cols[rand.Intn(len(ctx.Cols))]
			fmt.Printf("old child:%s new child:%s\n", e.children[ctx.ReplaceChildIdx].ToSQL(), newNode.children[ctx.ReplaceChildIdx].ToSQL())
			result = append(result, newNode)
		}
	}
	return result
}


type ReplaceChildToFunc struct {}

func (r *ReplaceChildToFunc) OneStep(node Expr, ctx TransformContext) []Expr {
	var result []Expr
	switch e := node.(type) {
	case *Constant:
	case *Column:
	case *Func:
		if len(e.children) > ctx.ReplaceChildIdx {
			fmt.Printf("ReplaceChildToFunc child size:%d id:%d\n", len(e.children), ctx.ReplaceChildIdx)
			fmt.Printf("node:%s\n", node.ToSQL())
			newNode := node.Clone().(*Func)
			newNode.children[ctx.ReplaceChildIdx] = GenExpr(e.children, TypeMask(e.children[ctx.ReplaceChildIdx].RetType()), MustContainCols)
			result = append(result, newNode)
		}
	}
	return result
}

func init() {
	rules = []TransformRule{
		&ConstantToColumn{},
		&ColumnToConstant{},
		&ReplaceChildToConstant{},
		&ReplaceChildToColumn{},
		&ReplaceChildToFunc{},
	}
}
