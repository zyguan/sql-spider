package util

type ValidateExprFn func(expr Expr) bool

func Pass(expr Expr) bool { return true }

func Not(v ValidateExprFn) ValidateExprFn {
	return func(expr Expr) bool {
		return !v(expr)
	}
}

func And(v0 ValidateExprFn, vs ...ValidateExprFn) ValidateExprFn {
	return func(expr Expr) bool {
		ok := v0(expr)
		if !ok {
			return false
		}
		for _, v := range vs {
			if !v(expr) {
				return false
			}
		}
		return true
	}
}

func Or(v0 ValidateExprFn, vs ...ValidateExprFn) ValidateExprFn {
	return func(expr Expr) bool {
		ok := v0(expr)
		if ok {
			return true
		}
		for _, v := range vs {
			if v(expr) {
				return true
			}
		}
		return false
	}
}

func MustContainCols(expr Expr) bool {
	switch e := expr.(type) {
	case Column, *Column:
		return true
	case *Func:
		for _, arg := range e.children {
			if MustContainCols(arg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}
