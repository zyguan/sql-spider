package util

import (
	"strconv"
	"strings"
)

type Expr interface {
	ToSQL() string
	Children() []Expr
	Clone() Expr
}

type Func struct {
	Name     string
	children []Expr
}

func (f *Func) Children() []Expr {
	return f.children
}

func (f *Func) ToSQL() string {
	return "" // TODO
}

func (f *Func) AppendArg(expr Expr) {
	f.children = append(f.children, expr)
}

func (f *Func) Clone() Expr {
	xs := make([]Expr, 0, len(f.children))
	for _, c := range f.children {
		xs = append(xs, c.Clone())
	}
	return &Func{
		Name:     f.Name,
		children: xs,
	}
}

type Constant string

func (c Constant) Children() []Expr {
	return nil
}

func (c Constant) ToSQL() string {
	return string(c)
}

func (c Constant) Clone() Expr {
	return c
}

type Column string

func (c Column) Children() []Expr {
	return nil
}

func (c Column) ToSQL() string {
	return string(c)
}

func (c Column) Clone() Expr {
	return c
}

type Node interface {
	NumCols() int
	ToSQL() string
	ToString() string
	Children() []Node
	Clone() Node
	AddChild(node Node)
}

type Tree Node

type baseNode struct {
	children []Node
}

func (b *baseNode) Children() []Node {
	return b.children
}

func (b *baseNode) clone() *baseNode {
	xs := make([]Node, 0, len(b.children))
	for _, c := range b.children {
		xs = append(xs, c.Clone())
	}
	return &baseNode{xs}
}

func (b *baseNode) AddChild(node Node) {
	b.children = append(b.children, node)
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

func (f *Filter) Clone() Node {
	return &Filter{
		*f.baseNode.clone(),
		f.Where.Clone(),
	}
}

func (f *Filter) ToString() string {
	return "Filter(" + f.children[0].ToString() + ")"
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

func (p *Projector) Clone() Node {
	ps := make([]Expr, 0, len(p.Projections))
	for _, x := range p.Projections {
		ps = append(ps, x.Clone())
	}
	return &Projector{
		*p.baseNode.clone(),
		ps,
	}
}

func (p *Projector) ToString() string {
	return "Projector(" + p.children[0].ToString() + ")"
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

func (j *Join) Clone() Node {
	return &Join{
		*j.baseNode.clone(),
		j.JoinCond.Clone(),
	}
}
func (j *Join) ToString() string {
	return "Join(" + j.children[0].ToString() + "," + j.children[1].ToString() + ")"
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

func (t *Table) Clone() Node {
	t1 := &Table{
		*t.baseNode.clone(),
		t.Schema,
		nil,
	}
	for _, s := range t.SelectedColumns {
		t1.SelectedColumns = append(t1.SelectedColumns, s)
	}
	return t1
}

func (t *Table) ToString() string {
	return "Table"
}

type TableSchema interface {
	Name() string
	Columns() []Column
}

type TableSchemas []TableSchema
