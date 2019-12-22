drop table if exists t1;

create table t1 (data varchar(4) not null);

prepare stmt from "insert ignore t1 (data) values ('letter'), (1/0);";
--execute insert_1
execute stmt;
--query select_insert_1
select data from t1;

prepare stmt from "update ignore t1 set data='envelope' where 1/0 or 1;";
--execute update_1
execute stmt;
--query select_update_1
select data from t1;

prepare stmt from "insert ignore t1 (data) values (default), (1/0), ('dead beef');";
--execute insert_2
execute stmt;
--query select_insert_2
select data from t1;

drop table t1;

# TIDB does not support insertable and updatable view

# CREATE TABLE t1 ( pk INT, PRIMARY KEY (pk));
# CREATE TABLE t2 LIKE t1;
#
# INSERT INTO t1 VALUES (2);
# INSERT INTO t2 VALUES (2);
#
# CREATE VIEW v1 AS SELECT * FROM t2 AS a
#                   WHERE a.pk IN ( SELECT pk FROM t1 AS b WHERE b.pk = a.pk );
#
# CREATE VIEW v2 AS SELECT * FROM t1 AS a
#                   WHERE a.pk IN ( SELECT pk FROM v1 AS b WHERE b.pk = a.pk );
#
# PREPARE st1 FROM 'INSERT INTO v2 (pk) VALUES ( 1 )';
# --execute insert_view_1
# EXECUTE st1;
#
# --query select_insert_view_1
# SELECT * FROM t1;
# --query select_insert_view_1
# SELECT * FROM t2;
#
# DROP TABLE t1, t2;
# DROP VIEW v1, v2;
#
#
# CREATE TABLE t1 (pk INT, PRIMARY KEY (pk));
# INSERT INTO t1 VALUES (1);
#
# CREATE ALGORITHM = TEMPTABLE VIEW v2 AS
# SELECT * FROM t1 AS a NATURAL JOIN t1 b WHERE pk BETWEEN 1 AND 2;
#
# CREATE ALGORITHM = UNDEFINED VIEW v1 AS
# SELECT * FROM t1 AS a
# WHERE a.pk IN ( SELECT pk FROM v2 AS b WHERE b.pk = a.pk );
#
# PREPARE st1 FROM "INSERT INTO v1 (pk) VALUES (2)";
# EXECUTE st1;
#
# SELECT * FROM t1;
#
# DROP VIEW v1, v2;
# DROP TABLE t1;
