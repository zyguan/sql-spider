DROP TABLE IF EXISTS t1;
CREATE TABLE t1(value DECIMAL(24,0) NOT NULL);
INSERT INTO t1(value)
VALUES('100000000000000000000001'),
('100000000000000000000002'),
('100000000000000000000003');

PREPARE stmt FROM 'SELECT * FROM t1 WHERE value = ?';
set @a="100000000000000000000002";
--query type_newdecimal_1
EXECUTE stmt using @a;
set @a=100000000000000000000002;
--query type_newdecimal_2
EXECUTE stmt using @a;
DEALLOCATE PREPARE stmt;
