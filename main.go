package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/zyguan/sqlgen/exprgen"
	"github.com/zyguan/sqlgen/nodegen"
	"github.com/zyguan/sqlgen/util"
)

func getTableSchemas() util.TableSchemas {
	return util.TableSchemas{
		{Name: "t",
			Columns: []util.Column{
				util.NewColumn("col_int", util.ETInt),
				util.NewColumn("col_double", util.ETReal),
				util.NewColumn("col_decimal", util.ETDecimal),
				util.NewColumn("col_string", util.ETString),
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
			fmt.Println(t.ToString())
			fmt.Println("============================")
			panic("??")
		}
	}()
	//fmt.Println(t.ToSQL())
	fmt.Println(t.ToBeautySQL(0))
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
