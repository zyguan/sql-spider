package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

func main() {
	dropTable := `
DROP TABLE IF EXISTS t;`
	createTable := `
CREATE TABLE t (
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
);`
	fmt.Println(dropTable)
	fmt.Println(createTable)
	n := 10000
	for i := 0; i < n; i++ {
		insert := fmt.Sprintf(`INSERT IGNORE INTO t values (%v, %v, %v, %v, %v);`,
			optional(.9, genInt), optional(.9, genDouble), optional(.9, genDecimal),
			optional(.9, genString), optional(.9, genDatetime))
		fmt.Println(insert)
	}

}

func genInt() string {
	return fmt.Sprintf("%v", int64(math.MaxInt64*rand.Float64()))
}

func genDouble() string {
	return fmt.Sprintf("%v", rand.Float64())
}

func genDecimal() string {
	return fmt.Sprintf("%v", math.MaxFloat64*rand.Float64())
}

func genString() string {
	n := rand.Intn(10) + 1
	buf := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		x := rand.Intn(26)
		buf = append(buf, byte('a'+x))
	}
	return "'" + string(buf) + "'"
}

func genDatetime() string {
	t := time.Unix(rand.Int63n(2000000000), rand.Int63n(30000000000))
	return t.Format("'2006-01-02 15:04:05'")
}

func optional(p float64, f func() string) string {
	if rand.Float64() < p {
		return f()
	}
	return "NULL"
}
