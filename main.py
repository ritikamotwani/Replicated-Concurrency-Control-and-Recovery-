import re
import sys

from datamanager import database
from transaction_manager import TransactionManager


def parse_input(file_name):
    re_comments = re.compile("//")
    re_begin  = re.compile("begin\s*\(+(?P<arg>\w+)\s*\)")
    re_R = re.compile("R\(\s*(?P<transaction>\w+)\s*,\s*(?P<var>\w+)\s*\)")
    re_W = re.compile("W\(\s*(?P<transaction>\w+)\s*,\s*(?P<var>\w+)\s*,\s*(?P<arg>\w+)\s*\)")
    re_recover = re.compile("recover\s*\(+(?P<arg>\w+)\s*\)")
    re_fail = re.compile("fail\s*\(+(?P<arg>\w+)\s*\)")
    re_end = re.compile("end\s*\(+(?P<arg>\w+)\s*\)")
    re_dump = re.compile("dump\s*\(\s*\)\s*")
    try:
        with open(file_name, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                # line = line.replace(" ", "")
                if re_comments.match(line):
                    print("ignoring comment --", line)
                elif re_begin.match(line):
                    print("Begin transaction --", re_begin.match(line).group("arg"))
                    transaction_manager.handle_begin_transaction(re_begin.match(line).group("arg"))
                elif re_R.match(line):
                    transaction = re_R.match(line).group("transaction")
                    variable = re_R.match(line).group("var")
                    print("Transaction -- ", transaction, "Read value of --", variable)
                    transaction_manager.handle_read(transaction, variable)
                elif re_W.match(line):
                    transaction_manager.handle_write(re_W.match(line).group("transaction"), re_W.match(line).group("var"),  re_W.match(line).group("arg"))
                    print("Transaction -- ", re_W.match(line).group("transaction"), "Write value to --", re_W.match(line).group("var"), ": ", re_W.match(line).group("arg"))
                elif re_recover.match(line):
                    site = re_recover.match(line).group("arg")
                    print("Recover site --", site)
                    database.handle_recover_site(site)

                elif re_fail.match(line):
                    site = re_fail.match(line).group("arg")
                    print("Fail site --", site)
                    database.handle_fail_site(site)

                elif re_end.match(line):
                    print("End transaction --", re_end.match(line).group("arg"))
                    transaction_manager.handle_end_transaction(re_end.match(line).group("arg"))
                elif re_dump.match(line):
                    print("Dump")
                    database.dump()
                elif line is None or line == '':
                    print("Empty line, ignored")
                else:
                    print("Unexpected input", line)
                    # break
    except FileNotFoundError:
        print(f"The file {file_name} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if len(sys.argv) != 2:
    print("Usage: python3 main.py <file name>")
    sys.exit(1)
# get the input file 
file_name = sys.argv[1]


# call the class
transaction_manager = TransactionManager(database)

# parse the input file
parse_input(file_name)
