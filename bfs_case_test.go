package main

import (
	"fmt"
	"testing"
)

func TestGenCase1(t *testing.T) {
	fmt.Println(genCase1().ToBeautySQL(0))
}
