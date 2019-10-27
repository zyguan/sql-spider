package nodegen

import (
	"fmt"
	"math/rand"

	"github.com/zyguan/sql-spider/util"
)




type NodeGenerator interface {
	Generate(level int) util.Node
}

type RandomNodeGenerator struct {
}

func randomGenNode(level int, mask util.NodeTypeMask) util.Node {
	type Entry struct {
		Node util.Node
		Prop float64
	}
	var propTable []Entry
	if level == 0 {
		propTable = []Entry {
			{&util.OrderBy{}, 0.5},
			{&util.Limit{}, 0.5},
		}
	} else {
		propTable = []Entry {
			{&util.Join{}, 0.4 - 0.1*float64(level)},
			{&util.Agg{}, 0.4 - 0.1*float64(level)},
			{&util.Table{}, 0.1 + 0.15*float64(level)},
		}
		if !mask.Contain(util.NTProjector) {
			propTable = append(propTable, Entry{&util.Projector{}, 0.2/* - 0.1*float64(level)*/})
		}
		if !mask.Contain(util.NTFilter) {
			propTable = append(propTable, Entry{&util.Filter{}, 0.2/* + 0.15 *float64(level)*/})
		}
		if !mask.Contain(util.NTOrderBy) {
			propTable = append(propTable, Entry{&util.OrderBy{}, 0.3/* - 0.1*float64(level)*/})
		}
		if !mask.Contain(util.NTLimit) && !mask.Contain(util.NTOrderBy) {
			propTable = append(propTable, Entry{&util.Limit{}, 0.3/* - 0.1 * float64(level)*/})
		}
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

func (rn *RandomNodeGenerator) Generate(level int, mask util.NodeTypeMask) util.Node {
	// random pick node type
	var node util.Node
	node = randomGenNode(level, mask)
	switch x := node.(type) {
	case *util.Table:
	case *util.Filter:
		x.AddChild(rn.Generate(level+1, mask.Add(util.NTFilter)))
	case *util.Projector:
		x.AddChild(rn.Generate(level+1, mask.Add(util.NTProjector)))
	case *util.OrderBy:
		x.AddChild(rn.Generate(level+1, mask.Add(util.NTOrderBy)))
	case *util.Limit:
		o := &util.OrderBy{}
		o.AddChild(rn.Generate(level+2, mask.Add(util.NTLimit | util.NTOrderBy)))
		x.AddChild(o)
		//x.AddChild(&util.OrderBy{})
		//mask.Add(util.NTLimit | util.NTOrderBy)
		//x.Children()[0].AddChild(rn.Generate(level+2, mask))
	case *util.Agg:
		mask.Remove(util.NTFilter | util.NTProjector | util.NTOrderBy | util.NTLimit)
		mask.Add(util.NTAgg)
		x.AddChild(rn.Generate(level+1, mask))
	case *util.Join:
		mask.Remove(util.NTFilter | util.NTProjector | util.NTOrderBy | util.NTLimit)
		mask.Add(util.NTJoin)
		x.AddChild(rn.Generate(level + 1, mask))
		x.AddChild(rn.Generate(level + 1, mask))
	}
	return node
}

func GenerateNode(number int) []util.Node {
	generator := RandomNodeGenerator{}
	var result []util.Node
	hashmap := map[string]bool{}
	for count := 0; count < number; {
		newNode := generator.Generate(0, 0)
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
