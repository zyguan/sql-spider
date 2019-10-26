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

func GenExprFromProbTable(level int) string {
	// Col: 0.1, Cons: 0.1, All Funcs: 0.8
	r := rand.Float64()
	r *= math.Pow(0.7, float64(level))
	if r < 0.1 {
		return Col
	} else if r < 0.2 {
		return Const
	}
	return funcList[rand.Intn(len(funcList))]
}

type FuncInfo struct {
	MinArgs   int
	MaxArgs   int
	ArgsTypes []Type
}

const (
	typeDefault = ETInt | ETReal | ETDecimal | ETString | ETDatetime | ETTimestamp | ETTimestamp | ETJson
	typeNumber  = ETInt | ETReal | ETDecimal
)

func (fi FuncInfo) ArgType(i int) Type {
	if len(fi.ArgsTypes) < i {
		return typeDefault
	}
	return fi.ArgsTypes[i]
}

var funcInfos = map[string]FuncInfo{
	FuncEQ:     {2, 2, nil},
	FuncIsTrue: {1, 1, nil},
	FuncPow:    {2, 2, []Type{typeNumber, typeNumber}},
	FuncLower:  {1, 1, []Type{ETString}},
}

//var NumArgs = map[string][]int{
//	FuncEQ:              {2, 2},
//	FuncGE:              {2, 2},
//	FuncLE:              {2, 2},
//	FuncNE:              {2, 2},
//	FuncLT:              {2, 2},
//	FuncGT:              {2, 2},
//	FuncIsTrue:          {1, 1},
//	FuncIf:              {3, 3},
//	FuncIfnull:          {2, 2},
//	FuncASCII:           {1, 1},
//	FuncBin:             {1, 1},
//	FuncConvert:         {2, 2},
//	FuncExportSet:       {3, 5},
//	FuncFormat:          {2, 3},
//	FuncFromBase64:      {1, 1},
//	FuncInsertFunc:      {4, 4},
//	FuncInstr:           {2, 2},
//	FuncLcase:           {1, 1},
//	FuncLeft:            {2, 2},
//	FuncRight:           {2, 2},
//	FuncLength:          {1, 1},
//	FuncLocate:          {2, 3},
//	FuncLower:           {1, 1},
//	FuncLpad:            {3, 3},
//	FuncLTrim:           {1, 1},
//	FuncMid:             {3, 3},
//	FuncOct:             {1, 1},
//	FuncOctetLength:     {1, 1},
//	FuncOrd:             {1, 1},
//	FuncPosition:        {2, 2},
//	FuncQuote:           {1, 1},
//	FuncRepeat:          {2, 2},
//	FuncReplace:         {3, 3},
//	FuncReverse:         {1, 1},
//	FuncRTrim:           {1, 1},
//	FuncSpace:           {1, 1},
//	FuncStrcmp:          {2, 2},
//	FuncSubstring:       {2, 3},
//	FuncSubstr:          {2, 3},
//	FuncSubstringIndex:  {3, 3},
//	FuncToBase64:        {1, 1},
//	FuncTrim:            {1, 3},
//	FuncUpper:           {1, 1},
//	FuncUcase:           {1, 1},
//	FuncHex:             {1, 1},
//	FuncUnhex:           {1, 1},
//	FuncRpad:            {3, 3},
//	FuncBitLength:       {1, 1},
//	FuncCharLength:      {1, 1},
//	FuncCharacterLength: {1, 1},
//	FuncFindInSet:       {2, 2},
//	FuncAbs:             {1, 1},
//	FuncAcos:            {1, 1},
//	FuncAsin:            {1, 1},
//	FuncAtan:            {1, 2},
//	FuncAtan2:           {2, 2},
//	FuncCeil:            {1, 1},
//	FuncCeiling:         {1, 1},
//	FuncConv:            {3, 3},
//	FuncCos:             {1, 1},
//	FuncCot:             {1, 1},
//	FuncCRC32:           {1, 1},
//	FuncDegrees:         {1, 1},
//	FuncExp:             {1, 1},
//	FuncFloor:           {1, 1},
//	FuncLn:              {1, 1},
//	FuncLog:             {1, 2},
//	FuncLog2:            {1, 1},
//	FuncLog10:           {1, 1},
//	FuncPI:              {0, 0},
//	FuncPow:             {2, 2},
//	FuncPower:           {2, 2},
//	FuncRadians:         {1, 1},
//	FuncRand:            {0, 1},
//	FuncRound:           {1, 2},
//	FuncSign:            {1, 1},
//	FuncSin:             {1, 1},
//	FuncSqrt:            {1, 1},
//	FuncTan:             {1, 1},
//	FuncTruncate:        {2, 2},
//	FuncLogicAnd:        {2, 2},
//	FuncLogicOr:         {2, 2},
//	FuncLogicXor:        {2, 2},
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
	for f := range funcInfos {
		funcList = append(funcList, f)
	}
}
