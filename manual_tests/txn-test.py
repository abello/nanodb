# looked up most of this stuff from SO
from subprocess import Popen, PIPE, call
import shutil

proc = Popen(["./nanodb", ""], stdout=PIPE, stdin=PIPE)

CREATE_TBL = "CREATE TABLE testwal (a INTEGER, b VARCHAR(30), c FLOAT);\n"


# BEGIN;
INS_1 = "INSERT INTO testwal VALUES (1, 'abc', 1.2);\n"
INS_2 = "INSERT INTO testwal VALUES (2, 'defghi', -3.6);\n"
INS_3 = "INSERT INTO testwal VALUES (3, 'jklmnopqrst', 5.5);\n"
INS_4 = "INSERT INTO testwal VALUES (4, 'hmm hmm', 261.32);\n"
INS_0 = "INSERT INTO testwal VALUES (-1, 'zxywvu', 78.2);\n"

SELECT = "SELECT * FROM testwal;\n"

BEGIN = "BEGIN;\n"
COMMIT = "COMMIT;\n"
FLUSH = "FLUSH;\n"
CRASH = "CRASH;\n"

def delete_files():
    folder = '/home/ronnel/work/nanodb/datafiles/'
    shutil.rmtree(folder)

def sql_exec(proc, sql):
    ''' Executes a sql statement in the database instance '''
    print "Executing: " + sql
    return proc.stdin.write(sql)

def print_guide(sql):
    ''' Prints a testing guide, such as "should have returned X" '''
    print  '\033[92m' + sql + '\033[0m'


delete_files()
sql_exec(proc, CREATE_TBL)
sql_exec(proc, INS_1)
sql_exec(proc, INS_2)


sql_exec(proc, BEGIN)
sql_exec(proc, SELECT)

print_guide("Should list both records")
sql_exec(proc, COMMIT)
print_guide("Should list both records")
print proc.communicate()[0]



