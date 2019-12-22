package xsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path"
	"time"

	"github.com/go-logr/logr"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/zyguan/mytest/mycase"
	"github.com/zyguan/mytest/mystmt"
	"github.com/zyguan/mytest/resultset"
	"github.com/zyguan/zapglog/log"
)

var logger = log.NewLogrLogger("cases", "xsql")

type Assertion struct {
	Name    string   `json:"name"`
	Delta   *float64 `json:"delta"`
	Columns []int    `json:"columns"`
}

type Checker struct {
	CheckSchema    *bool       `json:"check_schema"`
	CheckPrecision *bool       `json:"check_precision"`
	FailFast       *bool       `json:"fail_fast"`
	Assertions     []Assertion `json:"assertions"`
}

var _ mycase.MyCase = &XSQLCase{}

type XSQLCase struct {
	Name   string          `json:"name"`
	Meta   json.RawMessage `json:"meta"`
	Stages struct {
		Setup    []string `json:"setup"`
		Test     []string `json:"test"`
		Teardown []string `json:"teardown"`
	} `json:"stages"`
	CheckerList map[string]Checker `json:"checkers"`

	DSNs []string

	home     string
	checkers map[string]resultset.Checker
	current  *mycase.TaskInfo
	log      logr.Logger
}

func (x *XSQLCase) NewTask() mycase.TaskInfo {
	x.current = &mycase.TaskInfo{
		Time: time.Now(),
		ID:   uuid.New().String(),
		Name: x.Name,
		Meta: x.Meta,
	}
	x.log = logger.WithName(x.Name).WithValues("id", x.current.ID)
	return *x.current
}

func (x *XSQLCase) Checkers() map[string]resultset.Checker { return x.checkers }

func (x *XSQLCase) Setup(args json.RawMessage) error {
	if x.current == nil {
		err := errors.New("task is uninitialized")
		x.log.Error(err, "check task info")
		return err
	}
	if len(x.DSNs) == 0 {
		x.log.Info("no dsn provided")
		return nil
	}
	if args != nil {
		var spec struct {
			File string `json:"file"`
		}

		if err := json.Unmarshal(args, &spec); err != nil {
			x.log.Error(err, "parse case spec")
			return err
		}
		if err := x.loadFromFile(spec.File); err != nil {
			x.log.Error(err, "load case spec", "file", spec.File)
			return err
		}
	}

	done := make(chan struct{})
	errs := make(chan error, len(x.DSNs)+1)

	tasks := make([]sqlTask, len(x.DSNs))
	for i, dsn := range x.DSNs {
		tasks[i].dsn = dsn
		tasks[i].log = x.log.WithValues("dsn", dsn)
		tasks[i].stmtCh = make(chan mystmt.Stmt, 64)
		go func(t *sqlTask) {
			errs <- t.Run()
		}(&tasks[i])
	}

	go func() {
		var (
			err error
			it  mystmt.Iterator
		)
		defer func() {
			for _, task := range tasks {
				close(task.stmtCh)
			}
			errs <- err
		}()
		for _, p := range x.Stages.Setup {
			f := path.Join(x.home, p)
			it, err = mystmt.SplitFile(f)
			if err != nil {
				x.log.Error(err, "load and split setup file", "file", f)
				return
			}
			for it.Scan() {
				stmt := it.Stmt()
				for _, task := range tasks {
					select {
					case <-done:
						return
					case task.stmtCh <- stmt:
					}
				}
			}
		}
	}()

	var fstErr error
	for i := 0; i < len(x.DSNs)+1; i++ {
		if err := <-errs; err != nil && fstErr == nil {
			fstErr = err
			close(done)
		}
	}
	return fstErr
}

func (x *XSQLCase) Test(rc mycase.ResultStore) error {
	if x.current == nil {
		err := errors.New("task is uninitialized")
		x.log.Error(err, "check task info")
		return err
	}
	if len(x.DSNs) == 0 {
		x.log.Info("no dsn provided")
		return nil
	}

	done := make(chan struct{})
	errs := make(chan error, len(x.DSNs)+1)

	tasks := make([]sqlTask, len(x.DSNs))
	for i, dsn := range x.DSNs {
		tasks[i].dsn = dsn
		tasks[i].log = x.log.WithValues("dsn", dsn)
		tasks[i].stmtCh = make(chan mystmt.Stmt, 64)
		tasks[i].rc = rc
		tasks[i].availCMDs = []string{cmdQuery, cmdExecute}
		go func(t *sqlTask) {
			errs <- t.Run()
		}(&tasks[i])
	}

	go func() {
		var (
			err error
			it  mystmt.Iterator
		)
		defer func() {
			for _, task := range tasks {
				close(task.stmtCh)
			}
			errs <- err
		}()
		for _, p := range x.Stages.Test {
			f := path.Join(x.home, p)
			it, err = mystmt.SplitFile(f)
			if err != nil {
				x.log.Error(err, "load and split test file", "file", f)
				return
			}
			for it.Scan() {
				stmt := it.Stmt()
				for _, task := range tasks {
					select {
					case <-done:
						return
					case task.stmtCh <- stmt:
					}
				}
			}
		}
	}()

	var fstErr error
	for i := 0; i < len(x.DSNs)+1; i++ {
		if err := <-errs; err != nil && fstErr == nil {
			fstErr = err
			close(done)
		}
	}
	return fstErr
}

func (x *XSQLCase) Teardown() error {
	if x.current == nil {
		return nil
	}
	defer func() { x.current, x.log = nil, nil }()

	done := make(chan struct{})
	errs := make(chan error, len(x.DSNs)+1)

	tasks := make([]sqlTask, len(x.DSNs))
	for i, dsn := range x.DSNs {
		tasks[i].dsn = dsn
		tasks[i].log = x.log.WithValues("dsn", dsn)
		tasks[i].stmtCh = make(chan mystmt.Stmt, 64)
		go func(t *sqlTask) {
			errs <- t.Run()
		}(&tasks[i])
	}

	go func() {
		var (
			err error
			it  mystmt.Iterator
		)
		defer func() {
			for _, task := range tasks {
				close(task.stmtCh)
			}
			errs <- err
		}()
		for _, p := range x.Stages.Teardown {
			f := path.Join(x.home, p)
			it, err = mystmt.SplitFile(f)
			if err != nil {
				x.log.Error(err, "load and split teardown file", "file", f)
				return
			}
			for it.Scan() {
				stmt := it.Stmt()
				for _, task := range tasks {
					select {
					case <-done:
						return
					case task.stmtCh <- stmt:
					}
				}
			}
		}
	}()

	var fstErr error
	for i := 0; i < len(x.DSNs)+1; i++ {
		if err := <-errs; err != nil && fstErr == nil {
			fstErr = err
			close(done)
		}
	}
	return fstErr
}

func (x *XSQLCase) loadFromFile(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return errors.Trace(err)
	}

	if err = json.NewDecoder(f).Decode(x); err != nil {
		return errors.Trace(err)
	}
	if err = validateAndSetDefault(x); err != nil {
		return err
	}
	return nil
}

const (
	AssertionRawBytes = "RawBytes"
	AssertionFloat    = "Float"
)

func Load(file string) (*XSQLCase, error) {
	x := &XSQLCase{home: path.Dir(file)}
	err := x.loadFromFile(file)
	if err != nil {
		return nil, err
	}
	return x, nil
}

func validateAndSetDefault(x *XSQLCase) error {
	if len(x.Stages.Test) == 0 {
		return errors.New("$.stages.test is required")
	}
	for k, c := range x.CheckerList {
		if err := validateAndSetChecker(x, k, c); err != nil {
			return err
		}
	}
	return nil
}

func validateAndSetChecker(x *XSQLCase, k string, c Checker) error {
	if x.checkers == nil {
		x.checkers = make(map[string]resultset.Checker)
	}
	ck := resultset.Checker{}
	if c.CheckSchema == nil {
		ck.CheckSchema = false
	} else {
		ck.CheckSchema = *c.CheckSchema
	}
	if c.CheckPrecision == nil {
		ck.CheckPrecision = false
	} else {
		ck.CheckPrecision = *c.CheckPrecision
	}
	if c.FailFast == nil {
		ck.FailFast = true
	} else {
		ck.FailFast = *c.FailFast
	}
	for _, a := range c.Assertions {
		switch a.Name {
		case AssertionFloat:
			aa := resultset.FloatAssertion{}
			if a.Delta == nil {
				aa.Delta = 1.0
			} else {
				aa.Delta = *a.Delta
			}
			if a.Columns == nil {
				aa.TypeNames = []string{"DECIMAL", "FLOAT", "DOUBLE"}
			} else {
				aa.Columns = a.Columns
			}
			ck.Assertions = append(ck.Assertions, aa)
		case AssertionRawBytes:
			ck.Assertions = append(ck.Assertions, resultset.RawBytesAssertion{})
		default:
			return errors.New("unknown assertion: " + a.Name)
		}
	}
	if c.Assertions == nil {
		ck.Assertions = []resultset.ValueAssertion{resultset.RawBytesAssertion{}}
	}
	x.checkers[k] = ck
	return nil
}

const (
	cmdIgnoreErrors = "ignore_errors"
	cmdExecute      = "execute"
	cmdQuery        = "query"
)

type sqlTask struct {
	dsn       string
	availCMDs []string
	stmtCh    chan mystmt.Stmt
	rc        mycase.ResultStore
	log       logr.Logger
}

func (t *sqlTask) Run() error {
	ctx := context.Background()
	db, err := sql.Open("mysql", t.dsn)
	if err != nil {
		return errors.Annotate(err, "open db")
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return errors.Annotate(err, "conn db")
	}
	defer conn.Close()

	var version string
	db.QueryRow("select version()").Scan(&version)

	for stmt := range t.stmtCh {
		ignoreErr := false
		lastRunCmd := mystmt.Command{}
		for _, cmd := range stmt.Commands {
			if !ignoreErr && cmd.Name == cmdIgnoreErrors {
				ignoreErr = true
			}
			for i := 0; i < len(t.availCMDs); i++ {
				if t.availCMDs[i] == cmd.Name {
					lastRunCmd = cmd
				}
			}
		}
		var (
			res  sql.Result
			rows *sql.Rows
			rs   *resultset.ResultSet
		)
		t0 := time.Now()
		switch lastRunCmd.Name {
		case cmdExecute:
			res, err = conn.ExecContext(ctx, stmt.Text)
			if err != nil {
				break
			}
			rs = resultset.NewFromResult(res)
			t1 := time.Now()
			key := ""
			if len(lastRunCmd.Args) > 0 {
				key = lastRunCmd.Args[0]
			}
			w := t.rc.Write(mycase.QueryResult{
				Time:      t0,
				Duration:  float64(t1.Sub(t0)) / float64(time.Second),
				Key:       key,
				SQL:       stmt.Text,
				Version:   version,
				ResultSet: rs,
			})
			if w != nil {
				t.log.Info("write execute result", "err", w.Error())
			}
		case cmdQuery:
			rows, err = conn.QueryContext(ctx, stmt.Text)
			if err != nil {
				break
			}
			rs, err = resultset.ReadFromRows(rows)
			rows.Close()
			if err != nil {
				break
			}
			t1 := time.Now()
			key := ""
			if len(lastRunCmd.Args) > 0 {
				key = lastRunCmd.Args[0]
			}
			w := t.rc.Write(mycase.QueryResult{
				Time:      t0,
				Duration:  float64(t1.Sub(t0)) / float64(time.Second),
				Key:       key,
				SQL:       stmt.Text,
				Version:   version,
				ResultSet: rs,
			})
			if w != nil {
				t.log.Info("write query result", "err", w.Error())
			}
		default:
			_, err = conn.ExecContext(ctx, stmt.Text)
		}
		if err != nil && !ignoreErr {
			t.log.Info("unexpected error", "sql", stmt.Text, "err", err.Error())
			return err
		}
	}
	return nil
}
