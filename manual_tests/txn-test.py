# looked up most of this stuff from SO
from subprocess import Popen, PIPE, call
import shutil


# Basic SQL statements
CREATE_TBL = "CREATE TABLE testwal (a INTEGER, b VARCHAR(30), c FLOAT);\n"
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
    folder = './datafiles/'
    shutil.rmtree(folder)

ROLLBACK = "ROLLBACK;\n"

def sql_exec(proc, sql):
    ''' Executes a sql statement in the database instance '''
    print "Executing: " + sql
    return proc.stdin.write(sql)

def print_guide(sql):
    ''' Pretty-prints a testing guide, such as "should have returned X" '''
    print  '\033[92m' + sql + '\033[0m'


delete_files()
proc = Popen(["./nanodb", ""], stdout=PIPE, stdin=PIPE)

sql_exec(proc, CREATE_TBL)
sql_exec(proc, INS_1)
sql_exec(proc, INS_2)
sql_exec(proc, BEGIN)
sql_exec(proc, SELECT)
sql_exec(proc, COMMIT)
sql_exec(proc, CRASH)

print_guide("Both selects should list both records")
print proc.communicate()[0]

print_guide("Restarting nanodb...")
proc = Popen(["./nanodb", ""], stdout=PIPE, stdin=PIPE)
sql_exec(proc, SELECT)
sql_exec(proc, BEGIN)
sql_exec(proc, INS_3)
sql_exec(proc, SELECT)
sql_exec(proc, COMMIT)
sql_exec(proc, SELECT)
sql_exec(proc, CRASH)

print proc.communicate()[0]
print_guide("Should list all three records")



# delete_files

proc = Popen(["./nanodb", ""], stdout=PIPE, stdin=PIPE)
sql_exec(proc, CREATE_TBL)
sql_exec(proc, INS_1)
sql_exec(proc, INS_2)
sql_exec(proc, SELECT)
sql_exec(proc, COMMIT)
sql_exec(proc, SELECT)

sql_exec(proc, BEGIN)
sql_exec(proc, INS_0)
sql_exec(proc, SELECT)
sql_exec(proc, ROLLBACK)
sql_exec(proc, SELECT)
sql_exec(proc, CRASH)

print_guide("Restarting nanodb...")
print_guide("Should only list original two records")

proc = Popen(["./nanodb", ""], stdout=PIPE, stdin=PIPE)
sql_exec(proc, SELECT)
sql_exec(proc, INS_4)
sql_exec(proc, SELECT)
sql_exec(proc, CRASH)
print_guide("Should list appropriate three records")

proc = Popen(["./nanodb", ""], stdout=PIPE, stdin=PIPE)
sql_exec(proc, SELECT)









