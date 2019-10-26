package util

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
	return len(f.children)
}

func (f *Filter) ToSQL() string {
	//TBD
	return ""
}

type Projector struct {
	baseNode
	Projections []Expr
}

func (p *Projector) NumCols() int {
	return len(p.Projections)
}
func (f *Projector) ToSQL() string {
	//TBD
	return ""
}

type Join struct {
	baseNode
	JoinCond []Expr
}

func (j *Join) NumCols() int {
	return j.children[0].NumCols() + j.children[1].NumCols()
}

func (j *Join) ToSQL() string {
	//TBD
	return ""
}

type Table struct {
	baseNode
	Columns []string
}

func (t *Table) NumCols() int {
	return len(t.Columns)
}

func (t *Table) ToSQL() string {
	//TBD
	return ""
}

type TableSchema interface {
	Name() string
	Columns() []Column
}

type TableSchemas []TableSchema
