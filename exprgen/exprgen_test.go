package exprgen

import (
	"fmt"
	"testing"
)

func TestGenDateTime(t *testing.T) {
	for i := 0; i < 30; i++ {
		fmt.Println(genDateTimeLiteral())
	}
}
