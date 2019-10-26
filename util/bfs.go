package util

import (
	"container/list"
	"math/rand"
)

type BFSHelper struct {
	states       map[string]int
	results      []Tree
	fifo         *list.List
	nNneighbours int
}

func createBFSHelper(beginTree Tree, nNneighbours int) *BFSHelper {
	bh := &BFSHelper{
		states:       make(map[string]int, nNneighbours),
		results:      make([]Tree, 0, nNneighbours),
		fifo:         list.New(),
		nNneighbours: nNneighbours,
	}
	bh.fifo.PushBack(beginTree.Clone())
	bh.results = append(bh.results, beginTree.Clone())
	bh.states[beginTree.ToBeautySQL(0)] = 0
	return bh
}

func (bh *BFSHelper) doBFS() {
	for bh.fifo.Len() > 0 && len(bh.results) < bh.nNneighbours {
		e := bh.fifo.Front()
		bh.fifo.Remove(e)
		tree := e.Value.(Tree)
		bh.transform(tree, tree, nil)
	}
}

func (bh *BFSHelper) transform(root, node Node, path []int) {
	if len(bh.results) >= bh.nNneighbours {
		return
	}
	switch t := node.(type) {
	case *Filter:
		for _, r := range rules {
			ctx := TransformContext{}
			for _, child := range t.Children() {
				ctx.Cols = append(ctx.Cols, child.Columns()...)
			}
			ctx.ReplaceChildIdx = rand.Intn(len(t.children))

			exprs := r.OneStep(t.Where, ctx)
			for _, where := range exprs {
				tree := root.Clone()
				p := tree
				for i := range path {
					p = p.Children()[path[i]]
				}
				p.(*Filter).Where = where

				state := tree.ToBeautySQL(0)
				if _, ok := bh.states[state]; ok {
					continue
				}

				bh.fifo.PushBack(tree)
				bh.results = append(bh.results, tree)
				bh.states[tree.ToBeautySQL(0)] = len(path) + 1
				if len(bh.results) >= bh.nNneighbours {
					return
				}
			}
		}
	case *Projector:
		for _, r := range rules {
			for i, expr := range t.Projections {
				ctx := TransformContext{}
				for _, child := range t.Children() {
					ctx.Cols = append(ctx.Cols, child.Columns()...)
				}
				ctx.ReplaceChildIdx = rand.Intn(len(t.children))
				exprs := r.OneStep(expr, ctx)
				for _, proj := range exprs {
					tree := root.Clone()
					p := tree
					for i := range path {
						p = p.Children()[path[i]]
					}
					p.(*Projector).Projections[i] = proj

					state := tree.ToBeautySQL(0)
					if _, ok := bh.states[state]; ok {
						continue
					}

					bh.fifo.PushBack(tree)
					bh.results = append(bh.results, tree)
					bh.states[tree.ToBeautySQL(0)] = len(path) + 1
					if len(bh.results) >= bh.nNneighbours {
						return
					}
				}
			}
		}
	default:
		newPath := make([]int, len(path)+1)
		copy(newPath, path)
		for i, child := range node.Children() {
			newPath[len(path)] = i
			bh.transform(root, child, newPath)
		}
	}
}

func BFS(tree Tree, nNneighbours int) []Tree {
	bh := createBFSHelper(tree, nNneighbours)
	bh.doBFS()
	rets := bh.results
	bh.results = nil
	return rets
}
