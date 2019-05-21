#!/usr/bin/env python3

##########################################################################
### Oracle2Parquet
### Author: Pawel Walat
### Current Version: 1.1
###
### Change Log:
###     1.0: 18-May-2019 - Initial version (Pawel Walat)
###     1.1: 21-May-2019 - Adding instruction for installation
###
### Before you use
###     pip install numpy
###     pip install pandas
###     pip install jpype1 (or use one from https://www.lfd.uci.edu/~gohlke/pythonlibs/)
###     pip install jaydebeapi
###     pip install fastparquet (or use one from https://www.lfd.uci.edu/~gohlke/pythonlibs/)
###     pip install python-snappy (or use one from https://www.lfd.uci.edu/~gohlke/pythonlibs/)
##########################################################################

import sys
import os
import pandas as pd
import jpype
import jaydebeapi
import argparse

use_sid=1
jdbcfilename="ojdbc6.jar"

parser = argparse.ArgumentParser(description='Oracle2Parquet')
parser.add_argument('output_file', metavar='output_file', type=str, help='Destination parquet file (existing file will be overridden)')
parser.add_argument('hostname', metavar='hostname<:port>', type=str, help='Oracle DB hostname <and port>')
parser.add_argument('SID', metavar='SID', type=str, help='Oracle DB Service Name or SID (use --use-sid)')
parser.add_argument('username', metavar='username', type=str, help='Oracle DB username')
parser.add_argument('password', metavar='password', type=str, help='Oracle DB password')
parser.add_argument('select_statement', metavar='select_statement', type=str, help='Select statement to be executed on the DB')


parser.add_argument('--use-sid', dest='use_sid', action='store_const', const=use_sid, help='connect using SID instead of Service Name')
parser.add_argument('--jdbc-dir', dest='jdbc_dir', action='store', help='specify where jdbc library is located')

args = parser.parse_args()
path, filename = os.path.split(os.path.realpath(__file__))

if args.jdbc_dir != None and os.path.isfile(os.path.join(args.jdbc_dir,jdbcfilename)):
    jdbcfilename = os.path.join(os.path.dirname(os.path.realpath(__file__)), jdbcfilename)
elif os.path.isfile(os.path.join(os.getcwd(),jdbcfilename)):
    jdbcfilename = os.path.join(os.getcwd(),jdbcfilename)
elif os.path.isfile(os.path.join(os.path.dirname(os.path.realpath(__file__)),jdbcfilename)):
    jdbcfilename = os.path.join(os.path.dirname(os.path.realpath(__file__)), jdbcfilename)
elif os.path.isfile(os.path.join(path,jdbcfilename)):
    jdbcfilename = os.path.join(path, jdbcfilename)
else:
    sys.exit("Cannot find JDBC library. Provide location with --jdbc-dir parameter.")

print("Using JDBC library: "+jdbcfilename)
jHome = jpype.getDefaultJVMPath()
jpype.startJVM(jHome, '-Djava.class.path='+jdbcfilename)

try:
    conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
                              'jdbc:oracle:thin:'+args.username+'/'+args.password+'@'+args.hostname+'/'+args.SID)

    cur = conn.cursor()
    cur.execute(args.select_statement)
    result = cur.fetchall()
except Exception as e:
	print("Error: "+str(e))
	sys.exit(1)

try:
	field_names = [i[0] for i in cur.description]
	df = pd.DataFrame(list(result), columns=field_names)
	df.to_parquet(args.output_file)
	print("File \""+args.output_file+"\" saved successfully!")
except:
	print("Error: "+sys.exc_info()[0])



