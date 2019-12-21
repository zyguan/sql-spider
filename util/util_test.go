package util_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zyguan/sql-spider/exprgen"
	"github.com/zyguan/sql-spider/nodegen"
	"github.com/zyguan/sql-spider/util"
	"github.com/zyguan/zapglog/log"
)

var logger = log.NewLogrLogger("test", "util")

func testSchemas(t *testing.T) util.TableSchemas {
	return util.TableSchemas{
		{Name: "t",
			Columns: []util.Column{
				util.NewColumn("col_int", util.ETInt),
				util.NewColumn("col_double", util.ETReal),
				util.NewColumn("col_decimal", util.ETDecimal),
				util.NewColumn("col_string", util.ETString),
				util.NewColumn("col_datetime", util.ETDatetime),
			}},
	}
}

func testDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", util.GetEnv("TEST_MYSQL_DSN", "root:@tcp(127.0.0.1:3306)/test"))
	require.NoError(t, err)
	db.Exec(`create table if not exists t (
		col_int int default null,
		col_double double default null,
		col_decimal decimal(40, 20) default null,
		col_string varchar(40) default null,
		col_datetime datetime default null,
		key(col_int),
		key(col_double),
		key(col_decimal),
		key(col_string),
		key(col_datetime),
		key(col_int, col_double),
		key(col_int, col_decimal),
		key(col_int, col_string),
		key(col_double, col_decimal)
	);`)
	return db
}

func TestRGenPreparedStmt(t *testing.T) {
	db := testDB(t)
	defer db.Close()

	for i := 0; i < 20; i++ {
		ngen := nodegen.RandomNodeGenerator{}
		node := ngen.Generate(0, 0)
		logger.V(2).Info("gen node: " + node.String())

		tree := exprgen.GenExprTrees(node, testSchemas(t), 1)[0]
		oSQL := tree.ToSQL()
		logger.Info("orig sql: " + oSQL)

		choices := util.RandChoose(5, .8)
		ptree, params := util.Parameterize(tree, choices)
		pSQL := ptree.ToSQL()
		logger.Info("prepared: "+pSQL, "params", params)

		stmt, err := db.Prepare(pSQL)
		if err != nil {
			if strings.Contains(err.Error(), "Error 1690: ") {
				// ignore numeric out of range errors
			} else {
				require.NoError(t, err)
			}
		} else {
			require.NoError(t, stmt.Close())
		}

		s := pSQL
		for _, arg := range params {
			s = strings.Replace(s, "?", arg.Value(), 1)
		}
		assert.Equal(t, oSQL, s)
	}

}

func init() {
	util.LoadDotEnvOnce()
}
