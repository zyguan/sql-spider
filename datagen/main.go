package main

import (
	"fmt"
	"math"
	"math/rand"
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
);`
	fmt.Println(dropTable)
	fmt.Println(createTable)
	n := 10000
	for i := 0; i < n; i++ {
		insert := fmt.Sprintf(`INSERT INTO t values (%v, %v, %v, %v);`,
			genInt(), genDouble(), genDecimal(), genString())
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
	for i := 0; i < n; i ++ {
		x := rand.Intn(26)
		buf = append(buf, byte('a'+x))
	}
	return "'" + string(buf) + "'"
}
