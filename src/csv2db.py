#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" save csv file into database table 
    Usage of:
	csv2db file -d ara -t abc
"""
import sys
from optparse import OptionParser
import pandas as pd
from sqlalchemy import create_engine
#from datetime import datetime

def run_csv2db(args,**kwargs):
	if len(args)<1:
		print >> sys.stderr, "**ERROR:","No file specified"
		return None
	filename=args[0]
	for (ky,va) in kwargs.iteritems():
		exec("{}=va".format(ky))
	if filename=='-':
		df=pd.read_csv(sys.stdin,sep=sep)
	else:
		df=pd.read_csv(filename,sep=sep)
	if tablename is not None:
		pgDB = create_engine('postgresql://sfdbo@{}:5432/{}'.format(hostname,dbname))
		df.to_sql(tablename,pgDB,index=False,schema='public',if_exists=wmode)
		print >> sys.stderr, "Data {} save to {}:{}".format(filename,dbname,tablename)
	else:
		print >> sys.stderr, df
	return df

def opt_csv2db(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:	list arguments, usually passed from sys.argv
		retParser:	OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class 
	"""
	parser = OptionParser(usage="usage: %prog [option] FILENAME", version="%prog 0.1",
		description="save FILENAME into DBNAME: TABLENAME")
	parser.add_option("-s","--sep",action="store",dest="sep",default="\t",
		help="field separator (default: \\t)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host (default: localhost)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara",
		help="database (default: ara)")
	parser.add_option("-t","--table",action="store",dest="tablename",
		help="db tablename (default: None)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table mode [replace|append] (default: replace)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	opt,args =opt_csv2db(sys.argv)
	try:
		run_csv2db(args,**opt)
	except Exception,e:
		print >> sys.stderr, "**ERROR:",str(e)
	
	
