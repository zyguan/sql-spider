package nodegen

import "github.com/zyguan/sqlgen/util"

type NodeGenerator interface {
	Generate(level int) util.Node
}

type RandomNodeGenerator struct {

}

func (rn *RandomNodeGenerator) Generate(level int) util.Node {
	return nil
}


func GenerateNode(level int) []util.Node {
	return nil
}