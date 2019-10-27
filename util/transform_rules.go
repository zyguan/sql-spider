package util

import "fmt"

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
		fmt.Printf("ReplaceChild child size:%d id:%d", len(e.children), ctx.ReplaceChildIdx)
		if len(e.children) > ctx.ReplaceChildIdx {
			newNode := e.Clone().(*Func)
			newNode.children[ctx.ReplaceChildIdx] = GenConstant(TypeMask(newNode.children[ctx.ReplaceChildIdx].RetType()))
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
	}
}
