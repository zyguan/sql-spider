package util

import (
	"strconv"
	"strings"
)

type Expr interface {
	ToSQL() string
	Children() []Expr
}

type Func struct {
	Name     string
	children []Expr
}

type Constant string

func (c Constant) Children() []Expr {
	return nil
}

func (c Constant) ToSQL() string {
	return string(c)
}

type Column string

func (c Column) Children() []Expr {
	return nil
}

func (c Column) ToSQL() string {
	return string(c)
}

type Node interface {
	NumCols() int
	ToSQL() string
	Children() []Node
}

type Tree Node

type baseNode struct {
	children []Node
}

func (b *baseNode) Children() []Node {
	return b.children
}

type Filter struct {
	baseNode
	Where Expr
}

func (f *Filter) NumCols() int {
	return f.children[0].NumCols()
}

func (f *Filter) ToSQL() string {
	return "SELECT * FROM (" + f.children[0].ToSQL() + ") WHERE " + f.Where.ToSQL()
}

type Projector struct {
	baseNode
	Projections []Expr
}

func (p *Projector) NumCols() int {
	return len(p.Projections)
}

func (p *Projector) ToSQL() string {
	cols := make([]string, len(p.Projections))
	for i, e := range p.Projections {
		cols[i] = e.ToSQL() + " AS c" + strconv.Itoa(i)
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM (" + p.children[0].ToSQL() + ")"
}

type Join struct {
	baseNode
	JoinCond Expr
}

func (j *Join) NumCols() int {
	return j.children[0].NumCols() + j.children[1].NumCols()
}

func (j *Join) ToSQL() string {
	l, r := j.children[0], j.children[1]
	cols := make([]string, l.NumCols()+r.NumCols())
	for i := 0; i < l.NumCols(); i++ {
		cols[i] = "t1.c" + strconv.Itoa(i) + " AS " + "c" + strconv.Itoa(i)
	}
	for i := 0; i < r.NumCols(); i++ {
		cols[i] = "t2.c" + strconv.Itoa(i) + " AS " + "c" + strconv.Itoa(i+l.NumCols())
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM (" + l.ToSQL() + ") AS t1, (" + r.ToSQL() + ") AS t2 ON " + j.JoinCond.ToSQL()
}

type Table struct {
	baseNode
	Schema TableSchema

	SelectedColumns []string
}

func (t *Table) NumCols() int {
	return len(t.SelectedColumns)
}

func (t *Table) ToSQL() string {
	cols := make([]string, len(t.SelectedColumns))
	for i, col := range t.SelectedColumns {
		cols[i] = col + " AS c" + strconv.Itoa(i)
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM " + t.Schema.Name()
}

type TableSchema interface {
	Name() string
	Columns() []Column
}

type TableSchemas []TableSchema
