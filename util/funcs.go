package util

const (
	FuncEQ     = "EQ"
	FuncIsTrue = "IsTrue"

	Col   = "Column"
	Const = "Constant"
)

var ProbabilityTable = map[string]float64{
	FuncEQ: 0.01,

	Col:   0.1,
	Const: 0.1,
}

var NumArgs = map[string][]int{
	FuncEQ:     {1, 1},
	FuncIsTrue: {1, 1},
}
