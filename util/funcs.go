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
	FuncFromBase64      = "from_base64"
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
	FuncToBase64        = "to_base64"
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
	Validate   ValidateExprFn
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
	FuncEQ:       {FuncEQ, 2, 2, nil, TypeNumber, nil},
	FuncGE:       {FuncGE, 2, 2, nil, TypeNumber, nil},
	FuncLE:       {FuncLE, 2, 2, nil, TypeNumber, nil},
	FuncNE:       {FuncNE, 2, 2, nil, TypeNumber, nil},
	FuncLT:       {FuncLT, 2, 2, nil, TypeNumber, nil},
	FuncGT:       {FuncGT, 2, 2, nil, TypeNumber, nil},
	FuncIsTrue:   {FuncIsTrue, 1, 1, nil, TypeDefault, nil},
	FuncIf:       {FuncIf, 3, 3, nil, TypeDefault, nil},
	FuncIfnull:   {FuncIfnull, 2, 2, nil, TypeDefault, nil},
	FuncLcase:    {FuncLcase, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncLeft:     {FuncLeft, 2, 2, []TypeMask{TypeString, TypeNumber}, TypeString, nil},
	FuncRight:    {FuncRight, 2, 2, []TypeMask{TypeString, TypeNumber}, TypeString, nil},
	FuncLength:   {FuncLength, 1, 1, []TypeMask{TypeString}, TypeNumber, nil},
	FuncLTrim:    {FuncLTrim, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncOct:      {FuncOct, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncRepeat:   {FuncRepeat, 2, 2, []TypeMask{TypeString, TypeNumber}, TypeString, nil},
	FuncReplace:  {FuncReplace, 3, 3, []TypeMask{TypeString, TypeString, TypeNumber}, TypeString, nil},
	FuncReverse:  {FuncReverse, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncRTrim:    {FuncRTrim, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncSubstr:   {FuncSubstr, 2, 3, []TypeMask{TypeString, TypeNumber, TypeNumber}, TypeString, nil},
	FuncToBase64: {FuncToBase64, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncUcase:    {FuncUcase, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncHex:      {FuncHex, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncAbs:      {FuncAbs, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncAcos:     {FuncAcos, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncAtan:     {FuncAtan, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncAsin:     {FuncAsin, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncCeil:     {FuncCeil, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncSin:      {FuncSin, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncCos:      {FuncCos, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncTan:      {FuncTan, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncCot:      {FuncCot, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncExp:      {FuncExp, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncFloor:    {FuncFloor, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncLn:       {FuncLn, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncLog:      {FuncLog, 1, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
	FuncLog2:     {FuncLog2, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncPow:      {FuncPow, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
	FuncPower:    {FuncPower, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
	FuncLower:    {FuncLower, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncUpper:    {FuncUpper, 1, 1, []TypeMask{TypeString}, TypeString, nil},
	FuncRound:    {FuncRound, 1, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
	FuncSign:     {FuncSign, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncSqrt:     {FuncSqrt, 1, 1, []TypeMask{TypeNumber}, TypeNumber, nil},
	FuncLogicAnd: {FuncLogicAnd, 2, 2, []TypeMask{TypeDefault, TypeDefault}, TypeDefault, nil},
	FuncLogicOr:  {FuncLogicOr, 2, 2, []TypeMask{TypeDefault, TypeDefault}, TypeDefault, nil},
	FuncLogicXor: {FuncLogicXor, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
	FuncPlus:     {FuncPlus, 2, 2, []TypeMask{TypeDefault, TypeDefault}, TypeDefault, nil},
	FuncMinus:    {FuncMinus, 2, 2, []TypeMask{TypeDefault, TypeDefault}, TypeDefault, nil},
	FuncMod:      {FuncMod, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
	FuncDiv:      {FuncDiv, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
	FuncMul:      {FuncMul, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
	FuncOr:       {FuncOr, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
	FuncXor:      {FuncXor, 2, 2, []TypeMask{TypeNumber, TypeNumber}, TypeNumber, nil},
}

//var NumArgs = map[string][]int{
//	FuncLeftShift:       {2, 2},
//	FuncRightShift:      {2, 2},
//	FuncUnaryNot:        {1, 1},
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
