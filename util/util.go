package util

type Expr interface {
	ToSQL() string
	Children() []Expr
}

type baseExpr struct {
	children []Expr
}

type Node interface {
	NumCols() int
	ToSQL() string
	Children() []Node
}

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
	Columns() []string
}

type TableSchemas []TableSchema
