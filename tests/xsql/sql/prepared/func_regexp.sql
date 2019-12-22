drop table if exists t1;

create table t1 (a varchar(40));
insert into t1 values ('C1'),('C2'),('R1'),('C3'),('R2'),('R3');
prepare stmt1 from 'select a from t1 where a rlike ? order by a';
set @a="^C.*";
--query func_regexp_1
execute stmt1 using @a;
set @a="^R.*";
--query func_regexp_2
execute stmt1 using @a;
deallocate prepare stmt1;
drop table t1;

CREATE TABLE t1(a INT, b CHAR(4));
INSERT INTO t1 VALUES (1, '6.1'), (1, '7.0'), (1, '8.0');
PREPARE stmt1 FROM "SELECT a FROM t1 WHERE a=1 AND '7.0' REGEXP b LIMIT 1";
--query func_regexp_3
EXECUTE stmt1;
--query func_regexp_3
EXECUTE stmt1;
--query func_regexp_3
EXECUTE stmt1;
--query func_regexp_3
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
DROP TABLE t1;
