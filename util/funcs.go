package util

import (
	"math/rand"
	"sync"
)

const (
	FuncEQ     = "EQ"
	FuncIsTrue = "IsTrue"

	Col   = "Column"
	Const = "Constant"
)

var ProbabilityTable = []struct {
	Name string
	Prob float64
}{
	{FuncEQ, 0.01},

	{Col, 0.1},
	{Const, 0.1},
}

var (
	totalP     float64
	totalPOnce sync.Once
)

func GenExprFromProbTable(level int) string {
	totalPOnce.Do(func() {
		totalP = 0
		for _, p := range ProbabilityTable {
			totalP += p.Prob
		}
	})

	x := rand.Float64() * totalP
	for _, p := range ProbabilityTable {
		x -= p.Prob
		if x < 0 {
			return p.Name
		}
	}

	return ProbabilityTable[len(ProbabilityTable)-1].Name
}

var NumArgs = map[string][]int{
	FuncEQ:     {1, 1},
	FuncIsTrue: {1, 1},
}
