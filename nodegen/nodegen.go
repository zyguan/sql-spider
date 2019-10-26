package nodegen

import (
	"fmt"
	"math/rand"

	"github.com/zyguan/sqlgen/util"
)

type NodeGenerator interface {
	Generate(level int) util.Node
}

type RandomNodeGenerator struct {
}

func randomGenNode(level int) util.Node {
	propTable := []struct {
		Node util.Node
		Prop float64
	}{
		{&util.Join{}, 0.3 - 0.1*float64(level)},
		{&util.Agg{}, 0.3 - 0.1*float64(level)},
		{&util.Projector{}, 0.2 - 0.1*float64(level)},
		{&util.Filter{}, 0.2 + 0.15*float64(level)},
		{&util.Table{}, 0.1 + 0.15*float64(level)},
		{&util.OrderBy{}, 0.3 - 0.1*float64(level)},
	}

	var total float64
	for _, prop := range propTable {
		total += prop.Prop
	}

	p := rand.Float64() * total
	for _, prop := range propTable {
		p -= prop.Prop
		if p <= 0 {
			return prop.Node
		}
	}
	return nil
}

func (rn *RandomNodeGenerator) Generate(level int) util.Node {
	// random pick node type
	node := randomGenNode(level)
	switch node.(type) {
	case *util.Table:
		break
	case *util.Filter:
		filter := node.(*util.Filter)
		filter.AddChild(rn.Generate(level + 1))
		break
	case *util.Projector:
		proj := node.(*util.Projector)
		proj.AddChild(rn.Generate(level + 1))
		break
	case *util.Agg:
		agg := node.(*util.Agg)
		agg.AddChild(rn.Generate(level + 1))
		break
	case *util.Join:
		join := node.(*util.Join)
		join.AddChild(rn.Generate(level + 1))
		join.AddChild(rn.Generate(level + 1))
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
