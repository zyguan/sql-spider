package tests

import (
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/zyguan/just"
	"github.com/zyguan/sql-spider/util"
)

func testArgs(t *testing.T) Args {
	return Args{
		DSNs:      strings.Split(util.GetEnv("TEST_DSN_LIST"), ";"),
		NExec:     240,
		NPrepared: 120,
		MaxParams: 5,
		ParamProb: .8,
		Schemas: util.TableSchemas{
			{Name: "t",
				Columns: []util.Column{
					util.NewColumn("col_int", util.ETInt),
					util.NewColumn("col_double", util.ETReal),
					util.NewColumn("col_decimal", util.ETDecimal),
					util.NewColumn("col_string", util.ETString),
					util.NewColumn("col_datetime", util.ETDatetime),
				},
			},
		},
	}
}

func TestQuickstart(t *testing.T) {
	args := testArgs(t)
	runners := newRunners(t, args.DSNs)
	defer func() {
		for _, r := range runners {
			if r.db != nil {
				r.db.Close()
			}
		}
	}()

	testExec(t, args.Gen()...)
}

func init() {
	util.LoadDotEnvOnce(func() {
		just.SetTraceFn(errors.Trace)
	})
}
