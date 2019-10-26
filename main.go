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
		{Name: "t1",
			Columns: []util.Column{"a", "b"}},
		{Name: "t2",
			Columns: []util.Column{"a"}},
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
		fmt.Println(t.ToSQL())
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
