DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a int, b int);
CREATE TABLE t2 (b int, c int);

INSERT INTO t1 VALUES (1, 1);
INSERT INTO t1 VALUES (1, 2);
INSERT INTO t1 VALUES (2, 3);

INSERT INTO t2 VALUES (2, 1);
INSERT INTO t2 VALUES (3, 2);
INSERT INTO t2 VALUES (4, 3);

SELECT * FROM t1;
SELECT * FROM t2;

-- SELECT * FROM t1 JOIN t2 ON t1.b=t2.b;
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.b=t2.b;
-- SELECT * FROM t1 JOIN t2 USING (b);
-- SELECT * FROM t1 SEMI-JOIN t2 ON t1.b=t2.b;
-- SELECT * FROM t1 JOIN t2;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;


