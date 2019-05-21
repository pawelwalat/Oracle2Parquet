# Oracle2Parquet

Simple tool to dump data from oracle DB to parquet file.

Usage: Oracle2Parquet.py [-h] [--use-sid] [--jdbc-dir JDBC_DIR]
                         output_file hostname<:port> SID username password
                         select_statement

positional arguments:
  output_file          Destination parquet file (existing file will be
                       overridden)
  hostname<:port>      Oracle DB hostname <and port>
  SID                  Oracle DB Service Name or SID (use --use-sid)
  username             Oracle DB username
  password             Oracle DB password
  select_statement     Select statement to be executed on the DB

optional arguments:
  -h, --help           show this help message and exit
  --use-sid            connect using SID instead of Service Name
  --jdbc-dir JDBC_DIR  specify where jdbc library is located


What you need to have:
-JVM installed
-JDBC lib placed in the same directory as oracle2parquet script or bin

Additionaly to run from source you need to install:
pip install numpy
pip install pandas
pip install argparse
pip install jpype1 (or use one from https://www.lfd.uci.edu/~gohlke/pythonlibs/)
pip install jaydebeapi
pip install fastparquet (or use one from https://www.lfd.uci.edu/~gohlke/pythonlibs/)
pip install python-snappy (or use one from https://www.lfd.uci.edu/~gohlke/pythonlibs/)
