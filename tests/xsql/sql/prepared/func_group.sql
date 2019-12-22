drop table if exists t1;

create table t1 (a int);
insert into t1 values (1),(2);
prepare stmt1 from 'SELECT COUNT(*) FROM t1';
--query func_group_1
execute stmt1;
--query func_group_1
execute stmt1;
--query func_group_1
execute stmt1;
deallocate prepare stmt1;
drop table t1;

create table t1 (a int, primary key(a));
insert into t1 values (1),(2);
prepare stmt1 from 'SELECT max(a) FROM t1';
--query func_group_2
execute stmt1;
--query func_group_2
execute stmt1;
--query func_group_2
execute stmt1;
deallocate prepare stmt1;
drop table t1;
