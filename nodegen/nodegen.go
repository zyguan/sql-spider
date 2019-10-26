package nodegen

import (
	"github.com/zyguan/sqlgen/util"
	"math/rand"
	"fmt"
)

type NodeGenerator interface {
	Generate(level int) util.Node
}


type RandomNodeGenerator struct {

}

func randomGenNode() util.Node {
	p := rand.Int31n(100)
	if p > 0 && p < 50 {
		return &util.Table{}
	}
	if p >= 50 && p < 70 {
		return &util.Filter{}
	}
	if p >= 70 && p < 90 {
		return &util.Projector{}
	}
	return &util.Join{}
}

func (rn *RandomNodeGenerator) Generate(level int) util.Node {
	// random pick node type
	node := randomGenNode()
	switch node.(type) {
	case *util.Table:
		break
	case *util.Filter:
		filter := node.(*util.Filter)
		filter.AddChild(rn.Generate(level))
		break
	case *util.Projector:
		proj := node.(*util.Projector)
		proj.AddChild(rn.Generate(level))
		break
	case *util.Join:
		join := node.(*util.Join)
		join.AddChild(rn.Generate(level))
		join.AddChild(rn.Generate(level))
		break
	}
	return node
}



func GenerateNode(number int) []util.Node {
	generator := RandomNodeGenerator{}
	var result []util.Node
	hashmap := map[string]bool{}
	for count := 0; count < number; {
		newNode := generator.Generate(0)
		nodeStr := newNode.ToString()
		if _, ok := hashmap[nodeStr]; !ok {
			fmt.Println(newNode.ToString())
			result = append(result, newNode)
			count++
			hashmap[nodeStr] = true
		}
	}
	return result
}