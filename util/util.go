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

func (tm TypeMask) Any() Type {
	for i := uint(0); i < 15; i++ {
		if tm.Contain(Type(1 << i)) {
			return 1 << i
		}
	}
	panic(fmt.Sprintf("DEBUG %v", tm))
}

func (tm TypeMask) Has(t TypeMask) bool {
	return tm&t > 0
}

func (tm TypeMask) All() []Type {
	ret := make([]Type, 0, 15)
	for i := uint(0); i < 15; i++ {
		if tm.Contain(Type(1 << i)) {
			ret = append(ret, Type(1<<i))
		}
	}
	if len(ret) == 0 {
		panic(fmt.Sprintf("DEBUG %v", tm))
	}
	return ret
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
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("============================")
			fmt.Println("====>>>", f.Name, len(f.children))
			fmt.Println("============================")
			panic("??")
		}
	}()

	infixFn := func(op string) string {
		return fmt.Sprintf("(%s %s %s)", f.children[0].ToSQL(), op, f.children[1].ToSQL())
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
		return strings.ToUpper(f.Name) + "(" + strings.Join(args, ", ") + ")"
	}
}

func (f *Func) AppendArg(expr Expr) {
	f.children = append(f.children, expr)
}

func (f *Func) SetRetType(tm TypeMask) {
	f.retType = Type(tm)
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
	val     string
	retType Type
}

func NewConstant(val string, retType Type) Constant {
	return Constant{val, retType}
}

func (c Constant) Children() []Expr {
	return nil
}

func (c Constant) ToSQL() string {
	return c.val
}

func (c Constant) Clone() Expr {
	return c
}

func (c Constant) RetType() Type {
	return c.retType
}

type Column struct {
	col     string
	retType Type
}

func NewColumn(col string, retType Type) Column {
	return Column{col, retType}
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
	Columns() []Expr
	ToSQL() string
	ToBeautySQL(level int) string
	ToString() string
	Children() []Node
	Clone() Node
	AddChild(node Node)
}

type Tree Node

type NodeType uint

const (
	NTJoin NodeType = 1 << iota
	NTAgg
	NTProjector
	NTFilter
	NTTable
	NTLimit
	NTOrderBy
)

type NodeTypeMask uint

func (m NodeTypeMask) Contain(tp NodeType) bool {
	return uint(m)&uint(tp) > 0
}

func (m NodeTypeMask) Add(tp NodeType) NodeTypeMask {
	m = NodeTypeMask(uint(m) | uint(tp))
	return m
}

func (m NodeTypeMask) Remove(tp NodeType) NodeTypeMask {
	m = NodeTypeMask(uint(m) ^ uint(tp))
	return m
}

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

func (f *Filter) Columns() []Expr {
	return f.children[0].Columns()
}

func (f *Filter) ToSQL() string {
	return f.children[0].ToSQL() + " WHERE " + f.Where.ToSQL()
	//return "SELECT * FROM (" + f.children[0].ToSQL() + ") WHERE " + f.Where.ToSQL()
}
func (f *Filter) ToBeautySQL(level int) string {
	return f.children[0].ToBeautySQL(level) + " WHERE " + f.Where.ToSQL()
	//	return strings.Repeat(" ", level) + "SELECT * FROM (\n" +
	//		f.children[0].ToBeautySQL(level+1) + "\n" +
	//		strings.Repeat(" ", level) + ") WHERE " + f.Where.ToSQL()
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

func (p *Projector) Columns() []Expr {
	cols := make([]Expr, len(p.Projections))
	for i, e := range p.Projections {
		cols[i] = NewColumn("c"+strconv.Itoa(i), e.RetType())
	}
	return cols
}

func (p *Projector) ToSQL() string {
	cols := make([]string, len(p.Projections))
	for i, e := range p.Projections {
		cols[i] = e.ToSQL() + " AS c" + strconv.Itoa(i)
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM (" + p.children[0].ToSQL() + ")"
}

func (p *Projector) ToBeautySQL(level int) string {
	cols := make([]string, len(p.Projections))
	for i, e := range p.Projections {
		cols[i] = e.ToSQL() + " AS c" + strconv.Itoa(i)
	}
	return strings.Repeat(" ", level) + "SELECT " + strings.Join(cols, ", ") + " FROM (\n" +
		p.children[0].ToBeautySQL(level+1) + "\n" +
		strings.Repeat(" ", level) + ")"
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

type OrderBy struct {
	baseNode
	OrderByExprs []Expr
}

func (o *OrderBy) Columns() []Expr {
	return o.children[0].Columns()
}

func (o *OrderBy) ToSQL() string {
	orderBy := make([]string, 0, len(o.OrderByExprs))
	for _, e := range o.OrderByExprs {
		orderBy = append(orderBy, e.ToSQL())
	}
	return o.children[0].ToSQL() + " ORDER BY " + strings.Join(orderBy, ", ")
	//return "SELECT * FROM (" + o.children[0].ToSQL() + ") ORDER BY " + strings.Join(orderBy, ", ")
}

func (o *OrderBy) ToBeautySQL(level int) string {
	orderBy := make([]string, 0, len(o.OrderByExprs))
	for _, e := range o.OrderByExprs {
		orderBy = append(orderBy, e.ToSQL())
	}
	return o.children[0].ToBeautySQL(level) + " ORDER BY " + strings.Join(orderBy, ", ")
	//return strings.Repeat(" ", level) + "SELECT * FROM (\n" +
	//	o.children[0].ToBeautySQL(level+1) + "\n" +
	//	strings.Repeat(" ", level) + ") ORDER BY " + strings.Join(orderBy, ", ")
}

func (o *OrderBy) Clone() Node {
	return &OrderBy{
		*o.baseNode.clone(),
		nil,
	}
}

func (o *OrderBy) ToString() string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("=======")
			fmt.Printf("child:%d", len(o.children))
			fmt.Println("=======")
			panic("wocao order")
		}
	}()
	return "Order(" + o.children[0].ToString() + ")"
}

type Limit struct {
	baseNode
	Limit int
}

func (l *Limit) Columns() []Expr {
	return l.children[0].Columns()
}

func (l *Limit) ToSQL() string {
	//	return "SELECT * FROM (" + l.children[0].ToSQL() + ") LIMIT " + strconv.Itoa(l.Limit)
	return l.children[0].ToSQL() + " LIMIT " + strconv.Itoa(l.Limit)
}

func (l *Limit) ToBeautySQL(level int) string {
	return l.children[0].ToBeautySQL(level) + " LIMIT " + strconv.Itoa(l.Limit)
	//	return strings.Repeat(" ", level) + "SELECT * FROM (\n" +
	//		l.children[0].ToBeautySQL(level + 1) + "\n" +
	//		strings.Repeat(" ", level) + ") LIMIT " + strconv.Itoa(l.Limit)
}

func (l *Limit) Clone() Node {
	return &Limit{
		baseNode: *l.baseNode.clone(),
		Limit:    0,
	}
}

func (l *Limit) ToString() string {
	return "Limit(" + l.children[0].ToString() + ")"
}

type Agg struct {
	baseNode
	AggExprs     []Expr
	GroupByExprs []Expr
}

func (a *Agg) Columns() []Expr {
	ret := make([]Expr, 0, len(a.AggExprs)+len(a.GroupByExprs))
	ret = append(ret, a.GroupByExprs...)
	ret = append(ret, a.AggExprs...)
	return ret
}

func (a *Agg) ToSQL() string {
	ret := a.Columns()
	aggs := make([]string, 0, len(ret))
	for _, e := range ret {
		aggs = append(aggs, e.ToSQL())
	}
	groupBy := make([]string, 0, len(a.GroupByExprs))
	for _, e := range a.GroupByExprs {
		groupBy = append(groupBy, e.ToSQL())
	}
	groupBySQL := "GROUP BY " + strings.Join(groupBy, ", ")
	if len(groupBy) == 0 {
		groupBySQL = ""
	}

	return "SELECT " + strings.Join(aggs, ", ") + " FROM (" + a.children[0].ToSQL() + ") " + groupBySQL
}

func (a *Agg) ToBeautySQL(level int) string {
	ret := a.Columns()
	aggs := make([]string, 0, len(ret))
	for _, e := range ret {
		aggs = append(aggs, e.ToSQL())
	}
	groupBy := make([]string, 0, len(a.GroupByExprs))
	for _, e := range a.GroupByExprs {
		groupBy = append(groupBy, e.ToSQL())
	}
	groupBySQL := "GROUP BY " + strings.Join(groupBy, ", ")
	if len(groupBy) == 0 {
		groupBySQL = ""
	}
	return strings.Repeat(" ", level) + "SELECT " + strings.Join(aggs, ", ") + " FROM (\n" +
		a.children[0].ToBeautySQL(level+1) + "\n" +
		strings.Repeat(" ", level) + ") " + groupBySQL
}

func (a *Agg) Clone() Node {
	return &Agg{
		*a.baseNode.clone(),
		nil, nil,
	}
}

func (a *Agg) ToString() string {
	return "Agg(" + a.children[0].ToString() + ")"
}

type Join struct {
	baseNode
	JoinCond Expr
}

func (j *Join) Columns() []Expr {
	exprs := make([]Expr, 0, len(j.children[0].Columns())+len(j.children[1].Columns()))
	for _, expr := range j.children[0].Columns() {
		exprs = append(exprs, expr)
	}
	for _, expr := range j.children[1].Columns() {
		exprs = append(exprs, expr)
	}
	return exprs
}

func (j *Join) ToSQL() string {
	l, r := j.children[0], j.children[1]
	lLen, rLen := len(l.Columns()), len(r.Columns())
	cols := make([]string, lLen+rLen)
	for i := 0; i < lLen; i++ {
		cols[i] = "t1.c" + strconv.Itoa(i) + " AS " + "c" + strconv.Itoa(i)
	}
	for i := 0; i < rLen; i++ {
		cols[i+lLen] = "t2.c" + strconv.Itoa(i) + " AS " + "c" + strconv.Itoa(i+lLen)
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM (" + l.ToSQL() + ") AS t1, (" + r.ToSQL() + ") AS t2 ON " + j.JoinCond.ToSQL()
}

func (j *Join) ToBeautySQL(level int) string {
	l, r := j.children[0], j.children[1]
	lLen, rLen := len(l.Columns()), len(r.Columns())
	cols := make([]string, lLen+rLen)
	for i := 0; i < lLen; i++ {
		cols[i] = "t1.c" + strconv.Itoa(i) + " AS " + "c" + strconv.Itoa(i)
	}
	for i := 0; i < rLen; i++ {
		cols[i+lLen] = "t2.c" + strconv.Itoa(i) + " AS " + "c" + strconv.Itoa(i+lLen)
	}
	return strings.Repeat(" ", level) + "SELECT " + strings.Join(cols, ",") + " FROM (\n" +
		l.ToBeautySQL(level+1) + ") AS t1, (\n" +
		r.ToBeautySQL(level+1) + ") AS t2\n" +
		strings.Repeat(" ", level) + " ON " + j.JoinCond.ToSQL()
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

	SelectedColumns []int
}

func (t *Table) Columns() []Expr {
	cols := make([]Expr, len(t.SelectedColumns))
	for i, idx := range t.SelectedColumns {
		cols[i] = NewColumn("c"+strconv.Itoa(i), t.Schema.Columns[idx].RetType())
	}
	return cols
}

func (t *Table) ToSQL() string {
	cols := make([]string, len(t.SelectedColumns))
	for i, idx := range t.SelectedColumns {
		cols[i] = t.Schema.Columns[idx].col + " AS c" + strconv.Itoa(i)
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM " + t.Schema.Name
}

func (t *Table) ToBeautySQL(level int) string {
	cols := make([]string, len(t.SelectedColumns))
	for i, idx := range t.SelectedColumns {
		cols[i] = t.Schema.Columns[idx].col + " AS c" + strconv.Itoa(i)
	}
	return strings.Repeat(" ", level) + "SELECT " + strings.Join(cols, ", ") + " FROM " + t.Schema.Name
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
