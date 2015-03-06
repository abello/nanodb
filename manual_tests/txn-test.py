# looked up most of this stuff from SO
from subprocess import Popen, PIPE, call

proc = Popen(["./nanodb", ""], stdout=PIPE, stdin=PIPE)

CREATE_TBL = "CREATE TABLE testwal (a INTEGER, b VARCHAR(30), c FLOAT);\n"


# BEGIN;
INS_1 = "INSERT INTO testwal VALUES (1, 'abc', 1.2);\n"
INS_2 = "INSERT INTO testwal VALUES (2, 'defghi', -3.6);\n"
INS_3 = "INSERT INTO testwal VALUES (3, 'jklmnopqrst', 5.5);\n"
INS_4 = "INSERT INTO testwal VALUES (4, 'hmm hmm', 261.32);\n"
INS_0 = "INSERT INTO testwal VALUES (-1, 'zxywvu', 78.2);\n"

SELECT = "SELECT * FROM testwal\n"

BEGIN = "BEGIN;\n"
COMMIT = "COMMIT;\n"
FLUSH = "FLUSH;\n"
CRASH = "CRASH;\n"

def delete_files():
    call(["rm", "../datafiles/*.log", "../datafiles/*.dat", "../datafiles/*.tbl"])


delete_files()
stdout = proc.stdin.write(CREATE_TBL)
stdout = proc.stdin.write(INS_1)
stdout = proc.stdin.write(INS_2)

stdout = proc.stdin.write(SELECT)

print proc.communicate()[0]


