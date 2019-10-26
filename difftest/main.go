package main

import (
	"bytes"
	"database/sql"
	"flag"
	"io"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/zyguan/sqlgen/exprgen"
	"github.com/zyguan/sqlgen/nodegen"
	"github.com/zyguan/sqlgen/util"
)

func getTableSchemas() util.TableSchemas {
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

func main() {
	var opts struct {
		mysql   string
		tidb    string
		trees   int
		queries int
	}
	flag.StringVar(&opts.mysql, "mysql", "", "mysql dsn")
	flag.StringVar(&opts.tidb, "tidb", "", "tidb dsn")
	flag.IntVar(&opts.trees, "trees", 10, "number of tree")
	flag.IntVar(&opts.queries, "queries", 5, "queries per tree")
	flag.Parse()

	mydb, err := sql.Open("mysql", opts.mysql)
	perror(err)
	tidb, err := sql.Open("mysql", opts.tidb)
	perror(err)

	ts := getTableSchemas()
	emptyTrees := nodegen.GenerateNode(opts.trees)
	var trees []util.Tree
	for _, et := range emptyTrees {
		trees = append(trees, exprgen.GenExprTrees(et, ts, opts.queries)...)
	}

	r := Runner{mydb: mydb, tidb: tidb}
	r.outMySQLErr, err = os.OpenFile("mysql_err.out", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	perror(err)
	r.outInconsistencyErr, err = os.OpenFile("tidb_err.out", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	perror(err)

	for _, t := range trees {
		r.Run(t)
	}
}

func perror(err error) {
	if err != nil {
		panic(err)
	}
}

type Runner struct {
	outMySQLErr         io.Writer
	outInconsistencyErr io.Writer

	mydb *sql.DB
	tidb *sql.DB
}

func (r *Runner) Run(t util.Tree) {
	q := t.ToBeautySQL(0)
	rs, err := r.mydb.Query(q)
	if err != nil {
		r.outMySQLErr.Write([]byte("========================================\n> ERR\n"))
		r.outMySQLErr.Write([]byte(err.Error()))
		r.outMySQLErr.Write([]byte("\n> SQL\n"))
		r.outMySQLErr.Write([]byte(q))
		r.outMySQLErr.Write([]byte("\n"))
		return
	}
	defer rs.Close()

	expRows, err := dumpToByteRows(rs)
	if err != nil {
		log.Error("fail to dump mysql result")
		return
	}

	rs, err = r.tidb.Query(q)
	if err != nil {
		r.outInconsistencyErr.Write([]byte("========================================\n> ERR\n"))
		r.outInconsistencyErr.Write([]byte(err.Error()))
		r.outInconsistencyErr.Write([]byte("\n> SQL\n"))
		r.outInconsistencyErr.Write([]byte(q))
		r.outInconsistencyErr.Write([]byte("\n"))
		return
	}
	defer rs.Close()

	actRows, err := dumpToByteRows(rs)
	if err != nil {
		log.Error("failed to dump tidb result")
		return
	}

	exp, act := expRows.convertToString(), actRows.convertToString()

	if exp != act {
		r.outInconsistencyErr.Write([]byte("========================================\n> EXPEACT\n"))
		r.outInconsistencyErr.Write([]byte(exp))
		r.outInconsistencyErr.Write([]byte("\n> ACTUAL\n"))
		r.outInconsistencyErr.Write([]byte(act))
		r.outInconsistencyErr.Write([]byte("\n"))
		return
	}
}

type byteRow struct {
	data [][]byte
}

type byteRows struct {
	cols []string
	data []byteRow
}

func (rows *byteRows) Len() int {
	return len(rows.data)
}

func (rows *byteRows) Less(i, j int) bool {
	r1 := rows.data[i]
	r2 := rows.data[j]
	for i := 0; i < len(r1.data); i++ {
		res := bytes.Compare(r1.data[i], r2.data[i])
		switch res {
		case -1:
			return true
		case 1:
			return false
		}
	}
	return false
}

func (rows *byteRows) Swap(i, j int) {
	rows.data[i], rows.data[j] = rows.data[j], rows.data[i]
}

func (rows *byteRows) convertToString() string {
	res := strings.Join(rows.cols, "\t")
	for _, row := range rows.data {
		line := ""
		for _, data := range row.data {
			col := string(data)
			if data == nil {
				col = "NULL"
			}
			if len(line) > 0 {
				line = line + "\t"
			}
			line = line + col
		}
		res = res + "\n" + line
	}
	return res + "\n"
}

func dumpToByteRows(rows *sql.Rows) (*byteRows, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	data := make([]byteRow, 0, 8)
	args := make([]interface{}, len(cols))
	for rows.Next() {
		tmp := make([][]byte, len(cols))
		for i := 0; i < len(args); i++ {
			args[i] = &tmp[i]
		}
		err := rows.Scan(args...)
		if err != nil {
			return nil, err
		}

		data = append(data, byteRow{tmp})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return &byteRows{cols: cols, data: data}, nil
}
