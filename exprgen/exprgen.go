package exprgen

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"

	"github.com/zyguan/sqlgen/util"
)

func GenExprTrees(tree util.Tree, ts util.TableSchemas, n int) []util.Tree {
	trees := make([]util.Tree, 0, n)
	for i := 0; i < n; i++ {
		t := tree.Clone()
		fillNode(t, ts)
		trees = append(trees, t)
	}
	return trees
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
	case *util.Agg:
		fillAgg(x)
	case *util.OrderBy:
		fillOrderBy(x, ts)
	case *util.Limit:
		fillLimit(x)
	}
}

func fillOrderBy(o *util.OrderBy, ts util.TableSchemas) {
	nCols := o.Children()[0].Columns()
	o.OrderByExprs = nCols[:1]
}

func fillLimit(l *util.Limit) {
	l.Limit = rand.Intn(100)
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
		p.Projections[i] = buildExpr(cols, util.TypeDefault)
	}
}

func fillAgg(a *util.Agg) {
	cols := a.Children()[0].Columns()
	chCols := len(a.Children()[0].Columns())
	aggCols := rand.Intn(chCols)
	if aggCols == 0 {
		aggCols = 1
	}
	for i := 0; i < aggCols-1; i++ {
		colName := fmt.Sprint("c%d", i)
		a.GroupByExprs = append(a.GroupByExprs, util.NewColumn(colName, cols[i].RetType()))
	}
	for i := aggCols; i < chCols; i++ {
		colName := fmt.Sprint("c%d", i)
		expr := &util.Func{Name: util.GetAggExprFromPropTable()}
		expr.AppendArg(util.NewColumn(colName, cols[i].RetType()))
		a.AggExprs = append(a.AggExprs, expr)
	}
}

func fillJoin(j *util.Join) {
	j.JoinCond = buildJoinCond(j.Children()[0].Columns(), j.Children()[1].Columns())
}

func fillFilter(f *util.Filter) {
	f.Where = buildExpr(f.Children()[0].Columns(), util.TypeNumber)
}

func buildJoinCond(lCols []util.Expr, rCols []util.Expr) util.Expr {
	lIdx, rIdx := rand.Intn(len(lCols)), rand.Intn(len(rCols))
	expr := &util.Func{Name: util.FuncEQ}
	expr.AppendArg(util.NewColumn("t1.c"+strconv.Itoa(lIdx), lCols[lIdx].RetType()))
	expr.AppendArg(util.NewColumn("t2.c"+strconv.Itoa(rIdx), rCols[rIdx].RetType()))
	return expr
}

func buildExpr(cols []util.Expr, tp util.TypeMask) util.Expr {
	var gen func(lv int, tp util.TypeMask) util.Expr
	gen = func(lv int, tp util.TypeMask) util.Expr {
		count := 10000
		for count > 0 {
			count--
			switch f := util.GenExprFromProbTable(lv); f {
			case util.Col:
				cc := make([]util.Expr, 0, len(cols))
				for _, col := range cols {
					if tp.Contain(col.RetType()) {
						cc = append(cc, col)
					}
				}
				if len(cc) == 0 {
					return nil
				}
				return cc[rand.Intn(len(cc))]
			case util.Const:
				return genConstant(tp)
			default:
				argsSpec := util.FuncInfos[f]
				n := argsSpec.MinArgs
				if argsSpec.MaxArgs > argsSpec.MinArgs {
					n = rand.Intn(argsSpec.MaxArgs-argsSpec.MinArgs) + argsSpec.MinArgs
				}
				expr := &util.Func{Name: f}
				for i := 0; i < n; i++ {
					subExpr := gen(lv+1, argsSpec.ArgTypeMask(i))
					if subExpr == nil {
						continue
					}
					expr.AppendArg(subExpr)
				}
				return expr
			}
		}
		panic("???")
	}
	return gen(0, tp)
}

func genConstant(tp util.TypeMask) util.Constant {
	t := rand.Intn(30)
	var ct util.Type
	var cv string
	if t < 10 && tp.Contain(util.ETInt) {
		ct = util.ETInt
		cv = genIntLiteral()
	} else if t < 20 && tp.Contain(util.ETReal) {
		ct = util.ETReal
		cv = genRealLiteral()
	} else {
		ct = util.ETString
		cv = genStringLiteral()
	}
	return util.NewConstant(cv, ct)
}

func genIntLiteral() string {
	return fmt.Sprintf("%v", int64(float64(math.MaxInt64)*rand.Float64()))
}

func genRealLiteral() string {
	return fmt.Sprintf("%v", math.MaxFloat64*rand.Float64())
}

func genStringLiteral() string {
	n := rand.Intn(10) + 1
	buf := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		x := rand.Intn(26)
		buf = append(buf, byte('a'+x))
	}
	return string(buf)
}
