package main

import (
	"fmt"
	"testing"
	"github.com/zyguan/sql-spider/util"
)

func TestGenCase1(t *testing.T) {
	input := genCase1()
	fmt.Println("====input====")
	fmt.Println(input.ToBeautySQL(0))
	nodes := util.BFS(genCase1(), 5)
	fmt.Println("====output====")
	for _, node := range nodes {
		fmt.Println(node.ToBeautySQL(0))
	}

}
