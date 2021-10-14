#!/usr/bin/env python
""" Program: mapping_filename_content.py
	Description: Save text file to database table:[mapping_filename_content]
	Save textfile [FILE1] to [database]::[table]
	Usage of:
	To update an exist set
		ls *sql | mapping_filename_content.py --style=SQL
	To create a new set
		ls *sql | mapping_filename_content.py --wmode=replace --style=SQL
	
	Last Mod.,
	Thu Mar 22 16:42:17 EDT 2018
"""
import sys,os
import pandas as pd
from sqlalchemy import create_engine
from pandas.io import sql
from optparse import OptionParser

def get_script(xqr,pgDB=None,databaseURL=''):
	""" get dataframe df from pgDB
	"""
	if pgDB is None and len(dbURL)>10:
		pgDB= create_engine(dbURL)
	df=pd.read_sql(xqr,pgDB)
	return(df,pgDB)

def save_script(df,tablename='mapping_filename_content',pgDB=None,wmode='append',databaseURL=''):
	""" save dataframe df to table: [tablename]
	"""
	if pgDB is None and len(dbURL)>10:
		pgDB= create_engine(dbURL)
	df.to_sql(tablename,pgDB,schema='public',index=False,if_exists=wmode)
	return(df,pgDB)

def run_mapping_filename_content(fpx,style,tablename,wmode,pgDB,opts):
	tmpx = open(fpx).read()
	filename=os.path.basename(fpx)
	dx={"style":[style],"filename":[filename],"content":[tmpx]}
	df=pd.DataFrame.from_dict(dx,orient='columns')
	df=df[["style","filename","content"]]
	if wmode == 'fail':
		print  >> sys.stdout, df.to_dict()
		return (df,pgDB)
	if wmode == 'append':
		xqr="DELETE FROM {} WHERE filename='{}'".format(tablename,filename)
		sql.execute(xqr,pgDB)
	(df,pgDB)=save_script(df,tablename,pgDB,wmode)
	return (df,pgDB)

def batch_mapping_filename_content(fpLst,pgDB,opts):
	tablename=opts['tablename']
	wmode=opts['wmode']
	style=opts['style'].upper()
	for j,fpx in enumerate(fpLst):
		try:
			(df,pgDB) = run_mapping_filename_content(fpx,style,tablename,wmode,pgDB,opts)
			print  >> sys.stderr, "**SUCCESS {}. {} @ update mapping_filename_content".format(j,fpx)
		except Exception, e:
			print  >> sys.stderr, "**ERROR {}. {} @ run_mapping_filename_content():\n\t{}".format(j,fpx,str(e))
		if wmode != 'fail':
			wmode='append'

def opt_mapping_filename_content(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:   list arguments, usually passed from sys.argv
		retParser:      OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class
	"""
	parser = OptionParser(usage="usage: %prog [option] FILE1 ...", version="%prog 0.1",
		description="Save textfile [FILE1] to [database]::[table]")
	parser.add_option("-d","--database",action="store",dest="dbname",default="eSTAR_2",
		help="database (default: eSTAR_2)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="tablename",default="mapping_filename_content",
		help="db tablename (default: mapping_filename_content)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="append",
		help="db table write-mode [replace|append|fail] (default: append)")
	parser.add_option("","--style",action="store",dest="style",default="TXT",
		help="script style [SQL|PY|R|...] (default: TXT)")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no save to database (default: save to database)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	""" Save textfile [FILE1] to [database]::[table]
		Usage of:
		To update an exist set
			ls *sql | mapping_filename_content.py
		To create a new set
			ls *sql | mapping_filename_content.py --wmode=replace
	"""
	(options, args)=opt_mapping_filename_content(sys.argv)
	if len(args) == 0:
		fpLst = sys.stdin.read().strip().split("\n")
	else:
		fpLst = args
	if options['saveDB'] is False:
		options['wmode']="fail"
	dbURL='postgresql://sfdbo@{hostname}:5432/{dbname}'.format(**options)
	pgDB= create_engine(dbURL)
	batch_mapping_filename_content(fpLst,pgDB,options)
	exit(0)
