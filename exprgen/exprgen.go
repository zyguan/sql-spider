package exprgen

import (
	"fmt"
	"github.com/zyguan/sqlgen/util"
	"math"
	"math/rand"
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
	}
}

func fillTable(t *util.Table, ts util.TableSchemas) {
	t.Schema = ts[rand.Intn(len(ts))]
	t.SelectedColumns = make([]string, 0, len(t.Schema.Columns))
	for _, col := range t.Schema.Columns {
		if rand.Float64() < .5 {
			t.SelectedColumns = append(t.SelectedColumns, col.ToSQL())
		}
	}
	if len(t.SelectedColumns) == 0 {
		t.SelectedColumns = append(t.SelectedColumns, t.Schema.Columns[rand.Intn(len(t.Schema.Columns))].ToSQL())
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
func fillAgg(a *util.Agg)  {
	chCols := a.Children()[0].NumCols()
	groupbyCols := rand.Intn(chCols)
	if groupbyCols == 0 {
		groupbyCols = 1
	}
	aggCols := chCols - groupbyCols
	if aggCols == 0 {
		aggCols = 1
	}
	children := a.Children()[0].Children()
	allExprs := make([]util.Expr, chCols)
	for i := 0;i < chCols;i ++ {
		allExprs[i] = buildExpr(children[i].NumCols(), util.TypeDefault)
	}
	a.GroupByExprs = allExprs[0: groupbyCols]
	a.AggExprs = allExprs[aggCols - 1: chCols]
}

func fillJoin(j *util.Join) {
	nLCols, nRCols := j.Children()[0].NumCols(), j.Children()[1].NumCols()
	j.JoinCond = buildJoinCond(nLCols, nRCols)
}

func fillFilter(f *util.Filter) {
	nCols := f.Children()[0].NumCols()
	f.Where = buildExpr(nCols)
}

func buildJoinCond(nLCols, nRCols int) util.Expr {
	lCol := fmt.Sprintf("t1.c%v", rand.Intn(nLCols))
	rCol := fmt.Sprintf("t2.c%v", rand.Intn(nLCols))
	expr := &util.Func{Name: util.FuncEQ}
	expr.AppendArg(util.Column(lCol))
	expr.AppendArg(util.Column(rCol))
	return expr
}

func buildExpr(nCols int, tp util.TypeMask) util.Expr {
	var gen func(lv int, tp util.TypeMask) util.Expr
	gen = func(lv int, tp util.TypeMask) util.Expr {
		count := 10000
		for count > 0 {
			count--
			switch f := util.GenExprFromProbTable(lv); f {
			case util.Col:
				return nil // TODO
				//return util.Column("c" + strconv.Itoa(rand.Intn(nCols)))
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
					subExpr := gen(lv+1, argsSpec.ArgType(i))
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
	var c util.Constant
	if t < 10 && tp.Contain(util.ETInt) {
		c.T = util.ETInt
		c.Val = genIntLiteral()
	} else if t < 20 && tp.Contain(util.ETReal) {
		c.T = util.ETReal
		c.Val = genRealLiteral()
	} else {
		c.T = util.ETString
		c.Val = genStringLiteral()
	}
	return c
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
	for i := 0; i < n; i ++ {
		x := rand.Intn(26)
		buf = append(buf, byte('a'+x))
	}
	return string(buf)
}
