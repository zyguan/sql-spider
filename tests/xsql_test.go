package tests

import (
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"github.com/zyguan/just"
	"github.com/zyguan/mytest/mycase"
	"github.com/zyguan/sql-spider/tests/xsql"
	"github.com/zyguan/sql-spider/util"
)

func TestAll(t *testing.T) {
	run(t, "xsql/all.json")
}

func TestRegression(t *testing.T) {
	run(t, "xsql/regression.json")
}

func run(t *testing.T, file string) {
	must := require.New(t)
	defer just.Assert(t, must.NoError)

	s := just.Try(mycase.NewSQLiteResultStore("xsql.db")).Nth(0).(mycase.ResultStore)
	x := just.Try(xsql.Load(file)).Nth(0).(*xsql.XSQLCase)
	x.DSNs = strings.Split(util.GetEnv("TEST_XSQL_DSN_LIST"), ";")

	err := mycase.Run(x, s, mycase.WithGlobalCheckers(
		mycase.CheckerMatchRegexp(".+", checker),
	))
	if err != nil {
		es := err.(*mycase.RunErrors)
		if es.ExecErr != nil {
			logger.Info("exec error", "err", es.ExecErr)
		}
		for i, e := range es.DiffErrs {
			logger.Info("diff error", "err", e, "key", es.DiffKeys[i])
		}
		must.NoError(err)
	}
}

func init() {
	util.LoadDotEnvOnce(func() {
		just.SetTraceFn(errors.Trace)
	})
}
