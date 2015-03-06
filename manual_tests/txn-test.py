# looked up most of this stuff from SO
from subprocess import Popen, PIPE

proc = Popen(["./nanodb", ""], stdout=PIPE, stdin=PIPE)

CREATE_TBL = "CREATE TABLE testwal (a INTEGER, b VARCHAR(30), c FLOAT);\n"

stdout = proc.stdin.write(CREATE_TBL)
print proc.communicate()[0]

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


