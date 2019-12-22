set names utf8;
drop table if exists t1;
create table t1 (a char(10), b varchar(10));
insert into t1 values ('bar','kostja');
insert into t1 values ('kostja','bar');
prepare my_stmt from "select * from t1 where a=?";
set @a:='bar';
--query ctype_utf8_1
execute my_stmt using @a;
set @a:='kostja';
--query ctype_utf8_2
execute my_stmt using @a;
set @a:=null;
--query ctype_utf8_3
execute my_stmt using @a;
