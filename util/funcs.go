package util

import (
	"math"
	"math/rand"
)

const (
	FuncIsTrue          = "IsTrue"
	FuncIf              = "If"
	FuncIfnull          = "Ifnull"
	FuncASCII           = "ASCII"
	FuncBin             = "Bin"
	FuncConvert         = "Convert"
	FuncExportSet       = "ExportSet"
	FuncFormat          = "Format"
	FuncFromBase64      = "FromBase64"
	FuncInsertFunc      = "InsertFunc"
	FuncInstr           = "Instr"
	FuncLcase           = "Lcase"
	FuncLeft            = "Left"
	FuncRight           = "Right"
	FuncLength          = "Length"
	FuncLoadFile        = "LoadFile"
	FuncLocate          = "Locate"
	FuncLower           = "Lower"
	FuncLpad            = "Lpad"
	FuncLTrim           = "LTrim"
	FuncMid             = "Mid"
	FuncOct             = "Oct"
	FuncOctetLength     = "OctetLength"
	FuncOrd             = "Ord"
	FuncPosition        = "Position"
	FuncQuote           = "Quote"
	FuncRepeat          = "Repeat"
	FuncReplace         = "Replace"
	FuncReverse         = "Reverse"
	FuncRTrim           = "RTrim"
	FuncSpace           = "Space"
	FuncStrcmp          = "Strcmp"
	FuncSubstring       = "Substring"
	FuncSubstr          = "Substr"
	FuncSubstringIndex  = "SubstringIndex"
	FuncToBase64        = "ToBase64"
	FuncTrim            = "Trim"
	FuncUpper           = "Upper"
	FuncUcase           = "Ucase"
	FuncHex             = "Hex"
	FuncUnhex           = "Unhex"
	FuncRpad            = "Rpad"
	FuncBitLength       = "BitLength"
	FuncCharLength      = "CharLength"
	FuncCharacterLength = "CharacterLength"
	FuncFindInSet       = "FindInSet"
	FuncAbs             = "Abs"
	FuncAcos            = "Acos"
	FuncAsin            = "Asin"
	FuncAtan            = "Atan"
	FuncAtan2           = "Atan2"
	FuncCeil            = "Ceil"
	FuncCeiling         = "Ceiling"
	FuncConv            = "Conv"
	FuncCos             = "Cos"
	FuncCot             = "Cot"
	FuncCRC32           = "CRC32"
	FuncDegrees         = "Degrees"
	FuncExp             = "Exp"
	FuncFloor           = "Floor"
	FuncLn              = "Ln"
	FuncLog             = "Log"
	FuncLog2            = "Log2"
	FuncLog10           = "Log10"
	FuncPI              = "PI"
	FuncPow             = "Pow"
	FuncPower           = "Power"
	FuncRadians         = "Radians"
	FuncRand            = "Rand"
	FuncRound           = "Round"
	FuncSign            = "Sign"
	FuncSin             = "Sin"
	FuncSqrt            = "Sqrt"
	FuncTan             = "Tan"
	FuncTruncate        = "Truncate"
	FuncLogicAnd        = "LogicAnd"
	FuncLogicOr         = "LogicOr"
	FuncLogicXor        = "LogicXor"
	FuncGE              = "GE"
	FuncLE              = "LE"
	FuncEQ              = "EQ"
	FuncNE              = "NE"
	FuncLT              = "LT"
	FuncGT              = "GT"
	FuncPlus            = "Plus"
	FuncMinus           = "Minus"
	FuncMod             = "Mod"
	FuncDiv             = "Div"
	FuncMul             = "Mul"
	FuncIntDiv          = "IntDiv"
	FuncBitNeg          = "BitNeg"
	FuncAnd             = "And"
	FuncLeftShift       = "LeftShift"
	FuncRightShift      = "RightShift"
	FuncUnaryNot        = "UnaryNot"
	FuncOr              = "Or"
	FuncXor             = "Xor"
	FuncUnaryMinus      = "UnaryMinus"
	FuncLike            = "Like"
	FuncRegexp          = "Regexp"
	FuncSetVar          = "SetVar"
	FuncGetVar          = "GetVar"
	FuncBitCount        = "BitCount"
	FuncGetParam        = "GetParam"

	Col   = "Column"
	Const = "Constant"
)

func GenExprFromProbTable(tp TypeMask, level int) string {
	// Col: 0.1, Cons: 0.1, All Funcs: 0.8
	r := rand.Float64()
	r *= math.Pow(0.7, float64(level))
	if r < 0.1 {
		return Col
	} else if r < 0.2 {
		return Const
	}

	count := 0
	for count < 1000 {
		count++
		fname := funcList[rand.Intn(len(funcList))]
		fnSpec := FuncInfos[fname]
		if !tp.Has(fnSpec.ReturnType) {
			continue
		}
		return fname
	}

	panic("???")
}

type FuncInfo struct {
	Name       string
	MinArgs    int
	MaxArgs    int
	ArgsTypes  []TypeMask
	ReturnType TypeMask
}

const (
	TypeDefault = TypeMask(ETInt | ETReal | ETDecimal | ETString | ETDatetime)
	TypeNumber  = TypeMask(ETInt | ETReal | ETDecimal)
	TypeString  = TypeMask(ETString)
	TypeTime    = TypeMask(ETDatetime)
)

func (fi FuncInfo) ArgTypeMask(i int, prvArgs []Expr) TypeMask {
	switch fi.Name {
	case FuncEQ, FuncGE, FuncLE, FuncNE, FuncLT, FuncGT:
		if i == 0 || len(prvArgs) == 0 {
			return TypeDefault
		} else if TypeNumber.Contain(prvArgs[len(prvArgs)-1].RetType()) {
			return TypeNumber
		} else if TypeTime.Contain(prvArgs[len(prvArgs)-1].RetType()) {
			return TypeTime
		} else {
			return TypeMask(prvArgs[len(prvArgs)-1].RetType())
		}
	}
	if len(fi.ArgsTypes) <= i {
		return TypeDefault
	}
	return fi.ArgsTypes[i]
}

var FuncInfos = map[string]FuncInfo{
	FuncEQ:       {FuncEQ, 2, 2, nil, TypeNumber},
	FuncGE:       {FuncGE, 2, 2, nil, TypeNumber},
	FuncLE:       {FuncLE, 2, 2, nil, TypeNumber},
	FuncNE:       {FuncNE, 2, 2, nil, TypeNumber},
	FuncLT:       {FuncLT, 2, 2, nil, TypeNumber},
	FuncGT:       {FuncGT, 2, 2, nil, TypeNumber},
	FuncIsTrue:   {FuncIsTrue, 1, 1, nil, TypeDefault},
	FuncIf:       {FuncIf, 3, 3, nil, TypeDefault},
	FuncIfnull:   {FuncIfnull, 2, 2, nil, TypeDefault},
	FuncLcase:    {FuncLcase, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncLeft:     {FuncLeft, 2, 2, []TypeMask{TypeString, TypeNumber}, TypeString},
	FuncRight:    {FuncRight, 2, 2, []TypeMask{TypeString, TypeNumber}, TypeString},
	FuncLength:   {FuncLength, 1, 1, []TypeMask{TypeString}, TypeNumber},
	FuncLTrim:    {FuncLTrim, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncOct:      {FuncOct, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncRepeat:   {FuncRepeat, 2, 2, []TypeMask{TypeString, TypeNumber}, TypeString},
	FuncReplace:  {FuncReplace, 3, 3, []TypeMask{TypeString, TypeString, TypeNumber}, TypeString},
	FuncReverse:  {FuncReverse, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncRTrim:    {FuncRTrim, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncSubstr:   {FuncSubstr, 2, 3, []TypeMask{TypeString, TypeNumber, TypeNumber}, TypeString},
	FuncToBase64: {FuncToBase64, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncUcase:    {FuncUcase, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncHex:      {FuncHex, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncAbs:      {FuncAbs, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncAcos:     {FuncAcos, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncAtan:     {FuncAtan, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncAsin:     {FuncAsin, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncCeil:     {FuncCeil, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncSin:      {FuncSin, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncCos:      {FuncCos, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncTan:      {FuncTan, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncCot:      {FuncCot, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncExp:      {FuncExp, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncFloor:    {FuncFloor, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncLn:       {FuncLn, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncLog:      {FuncLog, 1, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber},
	FuncLog2:     {FuncLog2, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncPow:      {FuncPow, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber},
	FuncPower:    {FuncPower, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber},
	FuncLower:    {FuncLower, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncUpper:    {FuncUpper, 1, 1, []TypeMask{TypeString}, TypeString},
	FuncRound:    {FuncRound, 1, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber},
	FuncSign:     {FuncSign, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncSqrt:     {FuncSqrt, 1, 1, []TypeMask{TypeNumber}, TypeNumber},
	FuncLogicAnd: {FuncLogicAnd, 2, 2, []TypeMask{TypeDefault, TypeDefault}, TypeDefault},
	FuncLogicOr:  {FuncLogicOr, 2, 2, []TypeMask{TypeDefault, TypeDefault}, TypeDefault},
	FuncLogicXor: {FuncLogicXor, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber},
}

//var NumArgs = map[string][]int{
//	FuncPlus:            {2, 2},
//	FuncMinus:           {2, 2},
//	FuncMod:             {2, 2},
//	FuncDiv:             {2, 2},
//	FuncMul:             {2, 2},
//	FuncIntDiv:          {2, 2},
//	FuncBitNeg:          {1, 1},
//	FuncAnd:             {2, 2},
//	FuncLeftShift:       {2, 2},
//	FuncRightShift:      {2, 2},
//	FuncUnaryNot:        {1, 1},
//	FuncOr:              {2, 2},
//	FuncXor:             {2, 2},
//	FuncUnaryMinus:      {1, 1},
//	FuncLike:            {3, 3},
//	FuncRegexp:          {2, 2},
//	FuncSetVar:          {2, 2},
//	FuncGetVar:          {1, 1},
//	FuncBitCount:        {1, 1},
//	FuncGetParam:        {1, 1},
//}

var funcList []string

func init() {
	for f := range FuncInfos {
		funcList = append(funcList, f)
	}
}
