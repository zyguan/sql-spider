package exprgen

import "github.com/zyguan/sqlgen/util"

func GenExprTrees(tree util.Tree, ts util.TableSchemas) []util.Tree {
	return nil
}

func genFilter(numCols []int) []util.Expr {
	return nil
}

func genProj(numCols []int) [][]util.Expr {
	return nil
}

func genJoinCond(numLCols, numRCols int) []util.Expr {
	return nil
}
