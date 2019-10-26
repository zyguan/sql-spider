package util

import "math/rand"

const (
	FuncAvg = "Avg"
	FuncSum = "Sum"
	FuncCount = "Count"
	FuncMax = "Max"
	FuncMin = "Min"
)

func GetAggExprFromPropTable() string {
	r := rand.Float64()
	if r < 0.2 {
		return FuncAvg
	}
	if r < 0.4 {
		return FuncSum
	}
	if r < 0.6 {
		return FuncCount
	}
	if r < 0.8 {
		return FuncMax
	}
	return FuncMin
}
