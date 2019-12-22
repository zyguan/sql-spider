package main

import (
	"fmt"

	"github.com/zyguan/sql-spider/exprgen"
	"github.com/zyguan/sql-spider/nodegen"
	"github.com/zyguan/sql-spider/util"
)

func getTableSchemas() util.TableSchemas {
	return util.TableSchemas{
		{Name: "t",
			Columns: []util.Column{
				util.NewColumn("col_int", util.ETInt),
				util.NewColumn("col_double", util.ETReal),
				util.NewColumn("col_decimal", util.ETDecimal),
				util.NewColumn("col_string", util.ETString),
				util.NewColumn("col_datetime", util.ETDatetime),
			}},
	}
}

func main() {
	ts := getTableSchemas()
	emptyTrees := nodegen.GenerateNode(10)
	var trees []util.Tree
	for _, et := range emptyTrees {
		trees = append(trees, exprgen.GenExprTrees(et, ts, 3)...)
	}
	for _, t := range trees {
		safePrint(t)
	}
}

func safePrint(t util.Tree) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("============================")
			fmt.Println(t.String())
			fmt.Println("============================")
			panic("??")
		}
	}()
	//fmt.Println(t.ToSQL())
	fmt.Println(t.ToBeautySQL(0) + ";")
	fmt.Println()
}

func init() {
	//rand.Seed(time.Now().UnixNano())
}
