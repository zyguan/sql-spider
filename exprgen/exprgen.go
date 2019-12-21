package exprgen

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/zyguan/sql-spider/util"
)

func GenExprTrees(tree util.Tree, ts util.TableSchemas, n int) []util.Tree {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("==================================")
			fmt.Println(tree.String())
			fmt.Println("==================================")
			panic("???")
		}
	}()

	trees := make([]util.Tree, 0, n)
	for i := 0; i < n; i++ {
		t := tree.Clone()
		fillNode(t, ts, true)
		trees = append(trees, t)
	}
	return trees
}

func fillNode(node util.Node, ts util.TableSchemas, isRoot bool) {
	for _, child := range node.Children() {
		fillNode(child, ts, false)
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
	case *util.Agg:
		fillAgg(x)
	case *util.OrderBy:
		fillOrderBy(x, ts)
	case *util.Limit:
		fillLimit(x)
	}
}

func fillOrderBy(o *util.OrderBy, ts util.TableSchemas) {
	for i, col := range o.Children()[0].Columns() {
		o.OrderByExprs = append(o.OrderByExprs, util.NewColumn("c"+strconv.Itoa(i), col.RetType()))
	}
}

func fillLimit(l *util.Limit) {
	l.Limit = 1 + rand.Intn(100)
}

func fillTable(t *util.Table, ts util.TableSchemas) {
	t.Schema = ts[rand.Intn(len(ts))]
	t.SelectedColumns = make([]int, 0, len(t.Schema.Columns))
	for i := range t.Schema.Columns {
		if rand.Float64() < .5 {
			t.SelectedColumns = append(t.SelectedColumns, i)
		}
	}
	if len(t.SelectedColumns) == 0 {
		t.SelectedColumns = append(t.SelectedColumns, rand.Intn(len(t.Schema.Columns)))
	}
}

func fillProj(p *util.Projector) {
	cols := p.Children()[0].Columns()
	nProjected := rand.Intn(len(cols) * 2)
	if nProjected == 0 {
		nProjected = 1
	}
	p.Projections = make([]util.Expr, nProjected)
	for i := 0; i < nProjected; i++ {
		p.Projections[i] = util.GenExpr(cols, util.TypeDefault, util.MustContainCols)
	}
}

func fillAgg(a *util.Agg) {
	cols := a.Children()[0].Columns()
	nCols := len(cols)
	aggCols := rand.Intn(nCols)
	if aggCols == 0 {
		aggCols = 1
	}
	for i := 0; i < nCols-aggCols; i++ {
		a.GroupByExprs = append(a.GroupByExprs, cols[i])
	}
	for i := nCols - aggCols; i < nCols; i++ {
		col := cols[i]
		expr := &util.Func{Name: util.GetAggExprFromPropTable()}
		//expr.AppendArg(col)
		expr.AppendArg(util.NewColumn("c"+strconv.Itoa(i), col.RetType()))
		expr.SetRetType(util.TypeMask(util.AggRetType(expr.Name, col)))
		a.AggExprs = append(a.AggExprs, expr)
	}
}

func fillJoin(j *util.Join) {
	j.JoinCond = buildJoinCond(j.Children()[0].Columns(), j.Children()[1].Columns())
}

func fillFilter(f *util.Filter) {
	cols := make([]util.Expr, 0, len(f.Children()[0].Columns()))
	for i, c := range f.Children()[0].Columns() {
		cols = append(cols, util.NewColumn(fmt.Sprintf("c%v", i), c.RetType()))
	}
	f.Where = util.GenExpr(cols, util.TypeNumber, util.MustContainCols)
}

func buildJoinCond(lCols []util.Expr, rCols []util.Expr) util.Expr {
	lIdx, rIdx := rand.Intn(len(lCols)), rand.Intn(len(rCols))
	expr := genJoinFunc()
	expr.AppendArg(util.NewColumn("t1.c"+strconv.Itoa(lIdx), lCols[lIdx].RetType()))
	expr.AppendArg(util.NewColumn("t2.c"+strconv.Itoa(rIdx), rCols[rIdx].RetType()))
	return expr
}

func genJoinFunc() *util.Func {
	allowFuncName := []string{
		util.FuncGE,
		util.FuncLE,
		util.FuncEQ,
		util.FuncNE,
		util.FuncLT,
		util.FuncGT,
	}
	return &util.Func{
		Name: allowFuncName[rand.Intn(len(allowFuncName))],
	}
}
