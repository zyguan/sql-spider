package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Type uint

func (t Type) String() string {
	switch t {
	case ETInt:
		return "INT"
	case ETReal:
		return "REAL"
	case ETDecimal:
		return "DECIMAL"
	case ETString:
		return "STRING"
	case ETDatetime:
		return "DATETIME"
	case ETTimestamp:
		return "TIMESTAMP"
	case ETDuration:
		return "DURATION"
	case ETJson:
		return "JSON"
	default:
		return "<UNKNOWN_TYPE>"
	}
}

func (t *Type) UnmarshalJSON(bs []byte) error {
	var s string
	if err := json.Unmarshal(bs, &s); err != nil {
		return err
	}
	var v Type
	switch strings.ToUpper(s) {
	case "INT":
		v = ETInt
	case "REAL":
		v = ETReal
	case "DECIMAL":
		v = ETDecimal
	case "STRING":
		v = ETString
	case "DATETIME":
		v = ETDatetime
	case "TIMESTAMP":
		v = ETTimestamp
	case "DURATION":
		v = ETDuration
	case "JSON":
		v = ETJson
	default:
		return errors.New("unknown type: " + s)
	}
	*t = v
	return nil
}

type TypeMask uint

func (tm TypeMask) Contain(t Type) bool {
	if t == 0 {
		panic("??")
	}
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

type Param interface {
	Value() string
	RetType() Type
}

type ParametricExpr interface {
	Expr
	Parameterize(choices []bool, params []Param) (Expr, []bool, []Param)
}

type Func struct {
	Name     string
	retType  Type
	children []Expr
}

func NewFunc(name string, retType Type) *Func {
	return &Func{name, retType, nil}
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
	case FuncAnd:
		return infixFn("&")
	case FuncOr:
		return infixFn("|")
	case FuncLogicXor, FuncXor:
		return infixFn("^")
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
	case FuncIsTrue:
		return "((" + f.children[0].ToSQL() + ") is true)"
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

func (f *Func) Parameterize(choices []bool, params []Param) (Expr, []bool, []Param) {
	e := f.Clone().(*Func)
	for i, c := range f.children {
		if len(choices) == 0 {
			break
		}
		if p, ok := c.(ParametricExpr); ok {
			e.children[i], choices, params = p.Parameterize(choices, params)
		}
	}
	return e, choices, params
}

type Constant struct {
	val     string
	retType Type
}

func NewConstant(val string, retType Type) Constant {
	return Constant{val, retType}
}

func (c Constant) Children() []Expr { return nil }

func (c Constant) ToSQL() string { return c.val }

func (c Constant) Clone() Expr { return Constant{c.val, c.retType} }

func (c Constant) RetType() Type { return c.retType }

func (c Constant) Value() string { return c.val }

func (c Constant) Parameterize(choices []bool, params []Param) (Expr, []bool, []Param) {
	if len(choices) == 0 {
		return c, choices, params
	}
	if !choices[0] {
		return c, choices[1:], params
	}
	return Constant{"?", c.retType}, choices[1:], append(params, c)
}

func (c Constant) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{"value": c.val, "type": c.retType.String()})
}

type Column struct {
	col     string
	retType Type
}

func NewColumn(col string, retType Type) Column { return Column{col, retType} }

func (c Column) Children() []Expr { return nil }

func (c Column) ToSQL() string { return c.col }

func (c Column) Clone() Expr { return Column{c.col, c.retType} }

func (c Column) RetType() Type { return c.retType }

func (c *Column) UnmarshalJSON(bs []byte) error {
	var tmp struct {
		Name string `json:"name"`
		Type Type   `json:"type"`
	}
	if err := json.Unmarshal(bs, &tmp); err != nil {
		return err
	}
	c.col = tmp.Name
	c.retType = tmp.Type
	return nil
}

type Node interface {
	Columns() []Expr
	ToSQL() string
	String() string
	Children() []Node
	Clone() Node
	AddChild(node Node)
	Parameterize(choices []bool, params []Param) (Node, []bool, []Param)
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

func (b *baseNode) inplaceParameterizeChildren(choices []bool, params []Param) ([]bool, []Param) {
	for i, c := range b.children {
		if len(choices) == 0 {
			break
		}
		b.children[i], choices, params = c.Parameterize(choices, params)
	}
	return choices, params
}

type Filter struct {
	baseNode
	Where Expr
}

func (f *Filter) Columns() []Expr {
	return f.children[0].Columns()
}

func (f *Filter) ToSQL() string {
	return "SELECT * FROM (" + f.children[0].ToSQL() + ") t WHERE " + f.Where.ToSQL()
}

func (f *Filter) Clone() Node {
	var where Expr
	if f.Where != nil {
		where = f.Where.Clone()
	}
	return &Filter{
		*f.baseNode.clone(),
		where,
	}
}

func (f *Filter) String() string {
	return "Filter(" + f.children[0].String() + ")"
}

func (f *Filter) Parameterize(choices []bool, params []Param) (Node, []bool, []Param) {
	n := f.Clone().(*Filter)
	choices, params = n.inplaceParameterizeChildren(choices, params)
	if len(choices) == 0 {
		return n, choices, params
	}
	if pe, ok := f.Where.(ParametricExpr); ok {
		n.Where, choices, params = pe.Parameterize(choices, params)
	}
	return n, choices, params
}

type Projector struct {
	baseNode
	Projections []Expr
}

func NewProjector(p []Expr) *Projector {
	return &Projector{Projections: p}
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
	return "SELECT " + strings.Join(cols, ", ") + " FROM (" + p.children[0].ToSQL() + ") t"
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

func (p *Projector) String() string {
	return "Projector(" + p.children[0].String() + ")"
}

func (p *Projector) Parameterize(choices []bool, params []Param) (Node, []bool, []Param) {
	if len(choices) == 0 {
		return p, choices, params
	}
	n := p.Clone().(*Projector)
	for i, e := range n.Projections {
		if len(choices) == 0 {
			break
		}
		if pe, ok := e.(ParametricExpr); ok {
			n.Projections[i], choices, params = pe.Parameterize(choices, params)
		}
	}
	choices, params = n.inplaceParameterizeChildren(choices, params)
	return n, choices, params
}

type OrderBy struct {
	baseNode
	OrderByExprs []Expr
}

func NewOrderBy(OrderByExprs []Expr) *OrderBy {
	return &OrderBy{OrderByExprs: OrderByExprs}
}

func (o *OrderBy) Columns() []Expr {
	return o.children[0].Columns()
}

func (o *OrderBy) ToSQL() string {
	orderBy := make([]string, 0, len(o.OrderByExprs))
	for _, e := range o.OrderByExprs {
		orderBy = append(orderBy, e.ToSQL())
	}
	return "SELECT * FROM (" + o.children[0].ToSQL() + ") t ORDER BY " + strings.Join(orderBy, ", ")
}

func (o *OrderBy) Clone() Node {
	orderBy := make([]Expr, 0, len(o.OrderByExprs))
	for _, or := range o.OrderByExprs {
		orderBy = append(orderBy, or.Clone())
	}
	return &OrderBy{
		*o.baseNode.clone(),
		orderBy,
	}
}

func (o *OrderBy) String() string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("=======")
			fmt.Printf("child:%d", len(o.children))
			fmt.Println("=======")
			panic("wocao order")
		}
	}()
	return "Order(" + o.children[0].String() + ")"
}

func (o *OrderBy) Parameterize(choices []bool, params []Param) (Node, []bool, []Param) {
	if len(choices) == 0 {
		return o, choices, params
	}
	n := o.Clone().(*OrderBy)
	choices, params = n.inplaceParameterizeChildren(choices, params)
	return n, choices, params
}

type Limit struct {
	baseNode
	parameterized bool
	Limit         int
}

func (l *Limit) Columns() []Expr {
	return l.children[0].Columns()
}

func (l *Limit) ToSQL() string {
	limit := "?"
	if !l.parameterized {
		limit = strconv.Itoa(l.Limit)
	}
	return "SELECT * FROM (" + l.children[0].ToSQL() + ") t LIMIT " + limit
}

func (l *Limit) Clone() Node {
	return &Limit{
		baseNode:      *l.baseNode.clone(),
		parameterized: l.parameterized,
		Limit:         l.Limit,
	}
}

func (l *Limit) String() string {
	return "Limit(" + l.children[0].String() + ")"
}

func (l *Limit) Parameterize(choices []bool, params []Param) (Node, []bool, []Param) {
	n := l.Clone().(*Limit)
	choices, params = n.inplaceParameterizeChildren(choices, params)
	if len(choices) == 0 {
		return n, choices, params
	}
	if choices[0] {
		n.parameterized = true
		params = append(params, NewConstant(strconv.Itoa(l.Limit), ETInt))
	}
	return n, choices[1:], params
}

type Agg struct {
	baseNode
	AggExprs     []Expr
	GroupByExprs []Expr
}

func (a *Agg) Columns() []Expr {
	ret := make([]Expr, 0, len(a.AggExprs)+len(a.GroupByExprs))
	for i, e := range a.GroupByExprs {
		ret = append(ret, NewColumn("c"+strconv.Itoa(i), e.RetType()))
	}
	for i, e := range a.AggExprs {
		ret = append(ret, NewColumn("c"+strconv.Itoa(len(a.GroupByExprs)+i), e.RetType()))
	}
	return ret
}

func (a *Agg) ToSQL() string {
	ret := make([]Expr, 0, len(a.AggExprs)+len(a.GroupByExprs))
	ret = append(ret, a.GroupByExprs...)
	ret = append(ret, a.AggExprs...)
	aggs := make([]string, 0, len(ret))
	for i, e := range ret {
		name := fmt.Sprintf("c%v", i)
		eSQL := e.ToSQL()
		if eSQL != name {
			eSQL += " AS " + name
		}
		aggs = append(aggs, eSQL)
	}
	groupBy := make([]string, 0, len(a.GroupByExprs))
	for _, e := range a.GroupByExprs {
		groupBy = append(groupBy, e.ToSQL())
	}
	groupBySQL := "SELECT " + strings.Join(aggs, ", ") + " FROM (" + a.children[0].ToSQL() + ") t"
	if len(groupBy) > 0 {
		groupBySQL += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	return groupBySQL
}

func (a *Agg) Clone() Node {
	aggExpr := make([]Expr, 0, len(a.AggExprs))
	for _, agg := range a.AggExprs {
		aggExpr = append(aggExpr, agg.Clone())
	}
	groupBy := make([]Expr, 0, len(a.GroupByExprs))
	for _, gb := range a.GroupByExprs {
		groupBy = append(groupBy, gb.Clone())
	}

	return &Agg{
		*a.baseNode.clone(),
		aggExpr, groupBy,
	}
}

func (a *Agg) String() string {
	return "Agg(" + a.children[0].String() + ")"
}

func (a *Agg) Parameterize(choices []bool, params []Param) (Node, []bool, []Param) {
	if len(choices) == 0 {
		return a, choices, params
	}
	n := a.Clone().(*Agg)
	choices, params = n.inplaceParameterizeChildren(choices, params)
	return n, choices, params
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
	return "SELECT " + strings.Join(cols, ",") + " FROM (" + l.ToSQL() + ") t1, (" + r.ToSQL() + ") t2 WHERE " + j.JoinCond.ToSQL()
}

func (j *Join) Clone() Node {
	var cond Expr
	if j.JoinCond != nil {
		cond = j.JoinCond.Clone()
	}
	return &Join{
		*j.baseNode.clone(),
		cond,
	}
}

func (j *Join) String() string {
	return "Join(" + j.children[0].String() + "," + j.children[1].String() + ")"
}

func (j *Join) Parameterize(choices []bool, params []Param) (Node, []bool, []Param) {
	if len(choices) == 0 {
		return j, choices, params
	}
	n := j.Clone().(*Join)
	choices, params = n.inplaceParameterizeChildren(choices, params)
	return n, choices, params
}

type Table struct {
	baseNode
	Schema TableSchema

	SelectedColumns []int
}

func NewTable(Schema TableSchema, SelectedColumns []int) *Table {
	return &Table{Schema: Schema, SelectedColumns: SelectedColumns}
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

func (t *Table) String() string {
	return "Table"
}

func (t *Table) AppendParams(params []Param) []Param { return params }

func (t *Table) Parameterize(choices []bool, params []Param) (Node, []bool, []Param) {
	return t, choices, params
}

type TableSchema struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

type TableSchemas []TableSchema

func GenConstant(tp TypeMask) Constant {
	if rand.Intn(100) <= 1 {
		return NewConstant("NULL", tp.Any())
	}

	var ct Type
	var cv string
	tps := tp.All()
	t := rand.Intn(len(tps))
	ct = tps[t]
	switch ct {
	case ETInt:
		cv = genIntLiteral()
	case ETReal, ETDecimal:
		cv = genRealLiteral()
	case ETString:
		cv = genStringLiteral()
	case ETDatetime:
		cv = genDateTimeLiteral()
	default:
		ct = tp.Any()
		cv = "NULL"
	}
	return NewConstant(cv, ct)
}

func genDateTimeLiteral() string {
	t := time.Unix(rand.Int63n(2000000000), rand.Int63n(30000000000))
	return t.Format("'2006-01-02 15:04:05'")
}

func genIntLiteral() string {
	return fmt.Sprintf("%v", int64(float64(math.MaxInt64)*rand.Float64()))
}

func genRealLiteral() string {
	//return fmt.Sprintf("%.6f", rand.Float64())
	base := math.Pow(10, float64(10-rand.Intn(20)))
	return fmt.Sprintf("%.3f", base*(rand.Float64()-0.5))
}

func genStringLiteral() string {
	n := rand.Intn(10) + 1
	buf := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		x := rand.Intn(62)
		if x < 26 {
			buf = append(buf, byte('a'+x))
		} else if x < 52 {
			buf = append(buf, byte('A'+x-26))
		} else {
			buf = append(buf, byte('0'+x-52))
		}
	}
	return "'" + string(buf) + "'"
}

func GenExpr(cols []Expr, tp TypeMask, validate ValidateExprFn) Expr {
	var gen func(lv int, tp TypeMask, validate ValidateExprFn) Expr
	gen = func(lv int, tp TypeMask, validate ValidateExprFn) Expr {
		count := 10000
		for count > 0 {
			count--
			switch f := GenExprFromProbTable(tp, lv); f {
			case Col:
				cc := make([]Expr, 0, len(cols))
				for _, col := range cols {
					if tp.Contain(col.RetType()) {
						cc = append(cc, col)
					}
				}
				if len(cc) == 0 {
					continue
				}
				expr := cc[rand.Intn(len(cc))]
				if !validate(expr) {
					continue
				}
				return expr
			case Const:
				expr := GenConstant(tp)
				if !validate(expr) {
					continue
				}
				return expr
			default:
				fnSpec := FuncInfos[f]
				n := fnSpec.MinArgs
				if fnSpec.MaxArgs > fnSpec.MinArgs {
					n = rand.Intn(fnSpec.MaxArgs-fnSpec.MinArgs) + fnSpec.MinArgs
				}
				expr := &Func{Name: f}
				expr.SetRetType(fnSpec.ReturnType)
				ok := true
				for i := 0; i < n; i++ {
					subExpr := gen(lv+1, fnSpec.ArgTypeMask(i, expr.Children()), RejectAllConstants)
					if subExpr == nil {
						ok = false
						break
					}
					expr.AppendArg(subExpr)
				}
				if !ok {
					continue
				}
				if lv == 0 && !validate(expr) {
					continue
				}
				if fnSpec.Validate != nil && !fnSpec.Validate(expr) {
					continue
				}
				return expr
			}
		}
		panic("???")
	}
	return gen(0, tp, validate)
}

func Parameterize(t Node, choices []bool) (Tree, []Param) {
	pt, _, params := t.Parameterize(choices, nil)
	return pt, params
}

func RandChoose(n int, prob float64) []bool {
	ps := make([]bool, n)
	for i := 0; i < n; i++ {
		ps[i] = rand.Float64() < prob
	}
	return ps
}
