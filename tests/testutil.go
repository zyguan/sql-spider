package tests

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zyguan/just"

	"github.com/zyguan/mytest/resultset"

	"github.com/zyguan/sql-spider/exprgen"
	"github.com/zyguan/sql-spider/nodegen"
	"github.com/zyguan/sql-spider/util"
	"github.com/zyguan/zapglog/log"
)

var logger = log.NewLogrLogger("tests", "prepared")

type Execution struct {
	Query  string
	Params []interface{}
}

func (ex *Execution) Key() string {
	hash := sha1.Sum([]byte(ex.String()))
	return hex.EncodeToString(hash[:])
}

func (ex *Execution) String() string {
	return fmt.Sprintf("%s %v", ex.Query, ex.Params)
}

type Args struct {
	DSNs      []string          `json:"dsns"`
	NExec     int               `json:"numExec"`
	NPrepared int               `json:"numPrepared"`
	MaxParams int               `json:"maxParams"`
	ParamProb float64           `json:"paramProb"`
	Schemas   util.TableSchemas `json:"schemas"`
}

func (a *Args) Gen() []Execution {

	type ex struct {
		sql string
		pts []util.Type
	}

	ns := nodegen.GenerateNode(a.NPrepared)
	exs := make([]ex, 0, a.NPrepared)

	for _, n := range ns {
		t := exprgen.GenExprTrees(n, a.Schemas, 1)[0]
		tree, params := util.Parameterize(t, util.RandChoose(a.MaxParams, a.ParamProb))
		ex := ex{sql: tree.ToSQL()}
		for _, p := range params {
			if p != nil {
				ex.pts = append(ex.pts, p.RetType())
			}
		}
		exs = append(exs, ex)
	}

	execs := make([]Execution, 0, a.NExec)
	for i := 0; i < a.NExec; i++ {
		ex := exs[rand.Intn(len(exs))]
		exec := Execution{Query: ex.sql}
		for _, pt := range ex.pts {
			switch pt {
			case util.ETInt:
				exec.Params = append(exec.Params, int64(float64(rand.Int63n(1e6))*rand.NormFloat64()))
			case util.ETDecimal, util.ETReal:
				exec.Params = append(exec.Params, float64(rand.Int63n(1e6))*rand.NormFloat64())
			case util.ETString:
				exec.Params = append(exec.Params, randstr(int(math.Abs(rand.NormFloat64()+30))))
			case util.ETDatetime:
				exec.Params = append(exec.Params, time.Unix(rand.Int63n(2000000000), rand.Int63n(30000000000)))
			default:
				exec.Params = append(exec.Params, nil)
			}
		}
		execs = append(execs, exec)
	}
	return execs
}

func randstr(size int, optCharset ...string) string {
	chars := charsDefault
	if len(optCharset) > 0 && len(optCharset[0]) > 0 {
		chars = optCharset[0]
	}
	bs := make([]byte, size)
	for i := range bs {
		bs[i] = chars[rand.Intn(len(chars))]
	}
	return string(bs)
}

const (
	charsNum     = "0123456789"
	charsLower   = "abcdefghijklmnopqrstuvwxyz"
	charsUpper   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	charsDefault = charsNum + charsLower + charsUpper
)

var checker = resultset.Checker{
	Assertions: []resultset.ValueAssertion{
		resultset.FloatAssertion{Delta: .5, TypeNames: []string{"DECIMAL", "FLOAT", "DOUBLE"}},
		resultset.RawBytesAssertion{},
	},
}

func newRunners(t *testing.T, dsns []string) []runner {
	runners := make([]runner, len(dsns))
	for i, dsn := range dsns {
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		runners[i] = runner{dsn: dsn, db: db, stmts: make(map[string]*sql.Stmt)}
	}
	return runners
}

func prun(ex Execution, runners []runner) ([]*resultset.ResultSet, []error) {
	ress := make([]*resultset.ResultSet, len(runners))
	errs := make([]error, len(runners))
	var wg sync.WaitGroup
	for k := range runners {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			ress[k], errs[k] = runners[k].run(ex)
		}(k)
	}
	wg.Wait()
	return ress, errs
}

func assertErrors(t *testing.T, errs []error) bool {
	e0, ok := errs[0], false
	for i := 1; i < len(errs); i++ {
		if e0 == nil {
			ok = assert.NoError(t, errs[i])
		} else {
			ok = assert.EqualError(t, errs[i], e0.Error())
		}
		if !ok {
			break
		}
	}
	return ok
}

func checkResults(checker resultset.Checker, ress []*resultset.ResultSet) (int, error) {
	r0 := ress[0]
	for i := 1; i < len(ress); i++ {
		if err := checker.Diff(r0, ress[i]); err != nil {
			return i, err
		}
	}
	return -1, nil
}

func applyExec(exs ...Execution) func(t *testing.T) { return func(t *testing.T) { testExec(t, exs...) } }

func testExec(t *testing.T, exs ...Execution) {
	runners := newRunners(t, strings.Split(util.GetEnv("TEST_DSN_LIST"), ";"))
	defer func() {
		for _, r := range runners {
			r.db.Close()
		}
	}()
	for i, ex := range exs {
		logger.Info("#"+strconv.Itoa(i), "query", ex.Query, "params", ex.Params)

		results, errs := prun(ex, runners)
		if !assertErrors(t, errs) {
			for i, err := range errs {
				logger.Info("error #"+strconv.Itoa(i), "err", err)
			}
			if len(util.GetEnv("TEST_FAIL_FAST")) > 0 {
				t.FailNow()
			}
			continue
		}
		if errs[0] != nil {
			continue
		}
		if k, err := checkResults(checker, results); err != nil {
			fmt.Fprintln(os.Stderr, "=========================")
			results[0].PrettyPrint(os.Stderr)
			fmt.Fprintln(os.Stderr, "=========================")
			results[k].PrettyPrint(os.Stderr)
			fmt.Fprintln(os.Stderr, "=========================")
			require.NoError(t, err)
		}
	}
}

type runner struct {
	dsn   string
	db    *sql.DB
	stmts map[string]*sql.Stmt
}

func (r *runner) run(ex Execution) (_ *resultset.ResultSet, err error) {
	defer just.Return(&err)
	if _, ok := r.stmts[ex.Query]; !ok {
		r.stmts[ex.Query] = just.Try(r.db.Prepare(ex.Query)).Nth(0).(*sql.Stmt)
	}

	stmt := r.stmts[ex.Query]
	startTime := time.Now()
	rows := just.Try(stmt.Query(ex.Params...)).Nth(0).(*sql.Rows)
	defer rows.Close()
	logger.V(1).Info("exec prepared stmt",
		"duration", time.Now().Sub(startTime),
		"sql", ex.Query, "params", ex.Params)

	return resultset.ReadFromRows(rows)
}
