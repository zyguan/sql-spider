package main

import (
	"fmt"

	"github.com/zyguan/sqlgen/exprgen"
	"github.com/zyguan/sqlgen/nodegen"
	"github.com/zyguan/sqlgen/util"
)

func main() {
	var ts util.TableSchemas // TODO
	emptyTrees := nodegen.GenerateNode(3)
	var trees []util.Tree
	for _, et := range emptyTrees {
		trees = append(trees, exprgen.GenExprTrees(et, ts)...)
	}
	for _, t := range trees {
		fmt.Println(t.ToSQL())
	}
}
