package exprgen

import (
	"math/rand"
	"strconv"

	"github.com/zyguan/sqlgen/util"
)

func GenExprTrees(tree util.Tree, ts util.TableSchemas, n int) []util.Tree {
	trees := make([]util.Tree, 0, n)
	for i := 0; i < n; i++ {
		t := tree.Clone()
		fillTree(t)
		trees = append(trees, t)
	}
	return trees
}

func fillTree(tree util.Tree) {
}

func fillNode(node util.Node, ts util.TableSchemas) {
	for _, child := range node.Children() {
		fillNode(child, ts)
	}
	switch x := node.(type) {
	case *util.Table:
		fillTable(x, ts)
	case *util.Projector:
		fillProj(x)
	case *util.Join:
		fillJoin(x)
	case *util.Filter:
		fillFilter(x)
	}
}

func fillTable(t *util.Table, ts util.TableSchemas) {
	t.SelectedColumns = make([]string, 0, len(t.Schema.Columns()))
	for _, col := range t.Schema.Columns() {
		if rand.Float64() < .5 {
			t.SelectedColumns = append(t.SelectedColumns, col.ToSQL())
		}
	}
}

func fillProj(p *util.Projector) {
	nCols := p.Children()[0].NumCols()
	nProjected := rand.Intn(nCols * 2)
	if nProjected == 0 {
		nProjected = 1
	}
	p.Projections = make([]util.Expr, nProjected)
	for i := 0; i < nProjected; i++ {
		p.Projections[i] = buildExpr(nCols)
	}
}

func fillJoin(j *util.Join) [][]util.Expr {
	nLCols, nRCols := j.Children()[0].NumCols(), j.Children()[1].NumCols()
	// TODO
	return nil
}

func fillFilter(f *util.Filter) []util.Expr {
	nCols := f.Children()[0].NumCols()
	// TODO
	return nil
}

func buildExpr(nCols int) util.Expr {
	var gen func(lv int) util.Expr
	gen = func(lv int) util.Expr {
		switch f := util.GenExprFromProbTable(lv); f {
		case util.Col:
			return util.Column("c" + strconv.Itoa(nCols))
		case util.Const:
			return util.Constant("'TODO'") // TODO
		default:
			argsSpec := util.NumArgs[f]
			n := rand.Intn(argsSpec[1]-argsSpec[0]) + argsSpec[0]
			expr := &util.Func{Name: f}
			for i := 0; i < n; i++ {
				expr.AppendArg(gen(lv + 1))
			}
			return expr
		}
	}
	return gen(0)
}
