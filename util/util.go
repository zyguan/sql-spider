package util

import (
	"fmt"
	"strconv"
	"strings"
)

type Type uint

type TypeMask uint

func (tm TypeMask) Contain(t Type) bool {
	return uint(tm)&uint(t) > 0
}

const (
	ETInt Type = 1 << iota
	ETReal
	ETDecimal
	ETString
	ETDatetime
	ETTimestamp
	ETDuration
	ETJson
)

type Expr interface {
	ToSQL() string
	Children() []Expr
	Clone() Expr
	RetType() Type
}

type Func struct {
	Name     string
	retType  Type
	children []Expr
}

func (f *Func) Children() []Expr {
	return f.children
}

func (f *Func) ToSQL() string {
	infixFn := func(op string) string {
		return fmt.Sprintf("(%s) %s (%s)", f.children[0].ToSQL(), op, f.children[1].ToSQL())
	}
	switch f.Name {
	case FuncEQ:
		return infixFn("=")
	case FuncNE:
		return infixFn("!=")
	case FuncGE:
		return infixFn(">=")
	case FuncGT:
		return infixFn(">")
	case FuncLE:
		return infixFn("<=")
	case FuncLT:
		return infixFn("<")
	case FuncLogicOr:
		return infixFn("OR")
	case FuncLogicAnd:
		return infixFn("AND")
	case FuncLogicXor:
		return infixFn("XOR")
	case FuncPlus:
		return infixFn("+")
	case FuncMinus:
		return infixFn("-")
	case FuncUnaryMinus:
		return "-(" + f.children[0].ToSQL() + ")"
	case FuncDiv:
		return infixFn("/")
	case FuncMul:
		return infixFn("*")
	case FuncMod:
		return infixFn("%")
	case FuncIntDiv:
		return infixFn("DIV")
	default:
		args := make([]string, len(f.children))
		for i, e := range f.children {
			args[i] = e.ToSQL()
		}
		return strings.ToUpper(f.Name) + "(" + strings.Join(args, ",") + ")"
	}
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
		retType:  f.retType,
		children: xs,
	}
}

func (f *Func) RetType() Type {
	return f.retType
}

type Constant struct {
	Val string
	T   Type
}

func (c Constant) Children() []Expr {
	return nil
}

func (c Constant) ToSQL() string {
	return c.Val
}

func (c Constant) Clone() Expr {
	return c
}

func (c Constant) RetType() Type {
	return c.T
}

type Column struct {
	col     string
	retType Type
}

func (c Column) Children() []Expr {
	return nil
}

func (c Column) ToSQL() string {
	return c.col
}

func (c Column) Clone() Expr {
	return c
}

func (c Column) RetType() Type {
	return c.retType
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
		nil,
		//f.Where.Clone(),
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

type Agg struct {
	baseNode
	AggExprs     []Expr
	GroupByExprs []Expr
}

func (a *Agg) NumCols() int {
	return len(a.AggExprs) + len(a.GroupByExprs)
}

func (a *Agg) ToSQL() string {
	//TBD
	return ""
}

func (a *Agg) Clone() Node {
	//TBD
	return nil
}
func (a *Agg) ToString() string {
	return "Agg(" + a.children[0].ToString() + ")"
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
		cols[i+l.NumCols()] = "t2.c" + strconv.Itoa(i) + " AS " + "c" + strconv.Itoa(i+l.NumCols())
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM (" + l.ToSQL() + ") AS t1, (" + r.ToSQL() + ") AS t2 ON " + j.JoinCond.ToSQL()
}

func (j *Join) Clone() Node {
	return &Join{
		*j.baseNode.clone(),
		nil,
		//j.JoinCond.Clone(),
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
	return "SELECT " + strings.Join(cols, ", ") + " FROM " + t.Schema.Name
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

type TableSchema struct {
	Name    string
	Columns []Column
}

type TableSchemas []TableSchema
