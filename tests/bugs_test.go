package tests

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/zyguan/just"
	"github.com/zyguan/sql-spider/util"
)

func TestPotentialBugs(t *testing.T) {
	t.Run("unexpected error 1690", applyExec(Execution{
		Query: `SELECT * FROM (SELECT col_double AS c0 FROM t) t WHERE (ABS((REPEAT(?, ?) OR 5617780767323292672)) < LN(EXP(c0)) + (? ^ ?));`,
		Params: []interface{}{
			"JuvkBX7ykVux20zQlkwDK2DFelgn7",
			815820.9671283874,
			-112990.35179796701,
			87997.92704840179,
		},
	}))
	t.Run("unexpected error 1105", applyExec(Execution{
		Query: `SELECT c0, COUNT(UCASE(?) < c0) AS c1 FROM (SELECT col_int AS c0 FROM t) t GROUP BY c0;`,
		Params: []interface{}{
			"xayh7vrWVNqZtzlJmdJQUwAHnkI8Ec",
		},
	}))
	t.Run("succeed without raising error 1690", applyExec(Execution{
		Query: `SELECT (? AND (REPEAT(?, POWER(?, 2)) + POWER(COS(col_int), ?))) FROM t;`,
		Params: []interface{}{
			-215337.21777715665,
			"OJSeK93lNAeLfLIw6bVjoiwjeNBltDm",
			219538,
			1223317.9047359922,
		},
	}))

	t.Run("query results mismatch #01", applyExec(Execution{
		Query: `SELECT HEX(REVERSE(HEX(((c1 + ?) AND (? ^ (? < c0)))))) AS c0 FROM (SELECT col_int AS c0, col_decimal AS c1 FROM t) t ORDER BY c0;`,
		Params: []interface{}{
			"uch8DWi0lkTWDa18J7HcvG14a13Hp",
			-16116,
			32620.130564556122,
		},
	}))
	t.Run("query results mismatch #02", applyExec(Execution{
		Query: `SELECT (? > c1) AND LOG2(LENGTH(c2)) AS c3, COUNT(*) FROM (SELECT col_decimal AS c1, col_string AS c2 FROM t) t GROUP BY c3 ORDER BY c3;`,
		Params: []interface{}{
			-82986,
		},
	}))

	query := `SELECT COUNT(*) FROM (SELECT col_int AS c0, col_string AS c1 FROM t) t WHERE (UCASE(?) OR (ROUND(c0) <= LOG2(c0 = ?)));`
	var exs []Execution
	for _, params := range [][]interface{}{
		{"qzniGtxTBNipXTJYxSaUnCjaU1x6n", -1086438.8178839851},
		{"2nHfD65y4hoRUr9x8sdi8YovWw8oP", -800139.8472226909},
	} {
		exs = append(exs, Execution{Query: query, Params: params})
	}
	t.Run("unexpected result on second execution", applyExec(exs...))
}

func init() {
	util.LoadDotEnvOnce(func() {
		just.SetTraceFn(errors.Trace)
	})
}
