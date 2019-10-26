package main

import (
	"github.com/zyguan/sqlgen/util"
)

var bfsCases []util.Tree

func genCase1() util.Tree {
	/*
	SELECT * FROM ( SELECT CEIL(TAN(IF(c1, c1, c1))) AS c0 FROM (
	  SELECT col_string AS c0, col_datetime AS c1 FROM t
	 ) AS t) t ORDER BY c0;
	 */
	t := util.NewTable(getTableSchemas()[0], []int{3, 4})

	col := util.NewColumn("c1", util.ETDatetime)
	exprIf := util.NewFunc(util.FuncIf, util.ETDatetime)
	exprIf.AppendArg(col)
	exprIf.AppendArg(col)
	exprIf.AppendArg(col)
	exprTan := util.NewFunc(util.FuncTan, util.ETReal)
	exprTan.AppendArg(exprIf)
	exprCeil := util.NewFunc(util.FuncCeil, util.ETReal)
	exprCeil.AppendArg(exprTan)
	p := util.NewProjector([]util.Expr{exprCeil})
	p.AddChild(t)

	col0 := util.NewColumn("c0", util.ETReal)
	o := util.NewOrderBy([]util.Expr{col0})
	o.AddChild(p)

	return o
}
