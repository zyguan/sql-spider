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
	Where []Expr
}

type Projector struct {
	baseNode
	Projections []Expr
}

type Join struct {
	baseNode
	JoinCond []Expr
}

type Table struct {
	baseNode
	Columns []string
}

type TableSchema interface {
	Name() string
	Columns() []Column
}

type TableSchemas []TableSchema
