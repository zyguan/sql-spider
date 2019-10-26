package exprgen

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/zyguan/sqlgen/util"
)

func GenExprTrees(tree util.Tree, ts util.TableSchemas, n int) []util.Tree {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("==================================")
			fmt.Println(tree.ToString())
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
		p.Projections[i] = buildExpr(cols, util.TypeDefault)
	}
}

func fillAgg(a *util.Agg) {
	cols := a.Children()[0].Columns()
	nCols := len(cols)
	aggCols := rand.Intn(nCols)
	if aggCols == 0 {
		aggCols = 1
	}
	for i := 0; i < aggCols; i++ {
		a.GroupByExprs = append(a.GroupByExprs, cols[i])
	}
	for i := aggCols; i < nCols; i++ {
		col := cols[i]
		expr := &util.Func{Name: util.GetAggExprFromPropTable()}
		expr.AppendArg(col)
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
					continue
				}
				return cc[rand.Intn(len(cc))]
			case util.Const:
				return genConstant(tp)
			default:
				fnSpec := util.FuncInfos[f]
				n := fnSpec.MinArgs
				if fnSpec.MaxArgs > fnSpec.MinArgs {
					n = rand.Intn(fnSpec.MaxArgs-fnSpec.MinArgs) + fnSpec.MinArgs
				}
				expr := &util.Func{Name: f}
				expr.SetRetType(fnSpec.ReturnType)
				ok := true
				for i := 0; i < n; i++ {
					subExpr := gen(lv+1, fnSpec.ArgTypeMask(i, expr.Children()))
					if subExpr == nil {
						ok = false
						break
					}
					expr.AppendArg(subExpr)
				}
				if !ok {
					continue
				}
				return expr
			}
		}
		panic("???")
	}
	return gen(0, tp)
}

func genConstant(tp util.TypeMask) util.Constant {
	if rand.Intn(100) <= 1 {
		return util.NewConstant("NULL", tp.Any())
	}

	var ct util.Type
	var cv string
	tps := tp.All()
	t := rand.Intn(len(tps))
	ct = tps[t]
	switch ct {
	case util.ETInt:
		cv = genIntLiteral()
	case util.ETReal, util.ETDecimal:
		cv = genRealLiteral()
	case util.ETString:
		cv = genStringLiteral()
	case util.ETDatetime:
		cv = genDateTimeLiteral()
	default:
		ct = tp.Any()
		cv = "NULL"
	}
	return util.NewConstant(cv, ct)
}

func genDateTimeLiteral() string {
	t := time.Unix(rand.Int63n(2000000000), rand.Int63n(30000000000))
	return t.Format("'2006-01-02 15:04:05'")
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
		x := rand.Intn(62)
		if x < 26 {
			buf = append(buf, byte('a'+x))
		} else if x < 52 {
			buf = append(buf, byte('A'+x-26))
		} else {
			buf = append(buf, byte('0'+x-52))
		}
	}
	return "'" + string(buf) + "'"
}
