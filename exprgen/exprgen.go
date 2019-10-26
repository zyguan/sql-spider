package exprgen

import "github.com/zyguan/sqlgen/util"

func GenExprTrees(tree util.Tree, ts util.TableSchemas, n int) []util.Tree {
	trees := make([]util.Tree, 0, n)
	for i := 0; i < n; i ++ {
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
	// TODO
}

func fillProj(p *util.Projector) {
	nCols := p.Children()[0].NumCols()
	// TODO
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
