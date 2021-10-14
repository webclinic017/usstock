#!/usr/bin/env python
""" Program: prc_temp_DN.py
    Description: Script to download Yahoo history
    Usage of:
	# from a list of SYMBOLS
		python prc_temp_DN.py SYMBOL1 SYMBOL2 ...
	OR
	# from stdin
		printf "SELECT ticker FROM mapping_ticker_cik where act_code=1"| psql.sh -d ara -At | python prc_temp_DN.py 
	to get [SYMBOL1,SYMBOL2,...] history
	and save them into [dbname]:prc_temp_yh
	then run prc_yh2hist.sql to insert additional to [prc_hist] (optional)
	OR
		printf "SELECT series FROM mapping_series_label"| psql.sh -d ara -At | python prc_temp_DN.py --table=macro_temp_fred
	then run fred2hist.sql to insert additional to [macro_hist_fred] (optional)
	OR
		cat currency.list | python prc_temp_DN.py -d ara --no_database_save
	to view history
    Note:
	To consolidate version for prc_temp_cn_DN.py & prc_temp_cn_DN.py.deprecated.
	This program will retire the following programs:
	prc_temp_av_DNLD.py
	prc_temp_batch.py
	prc_temp_cn.tradego.py
	prc_temp_DNLD.py
	prc_temp_yh_DNLD.py
	prc_temp_yh.py
    add '-' for SYMBOL1 as input from stdin
    direct use _alan_calc.data_from_web function 
    Last mod., Tue Apr 16 09:18:04 EDT 2019
"""
import sys
from optparse import OptionParser
import datetime
import pandas as pd
from sqlalchemy import create_engine
from _alan_calc import data_from_web

def batch_prc_temp(tkLst,opts):
	""" Get yahoo price history via [pandas_datareader.data] 
		from a list of tickers:[tkLst]
		and save them into table:[tablename] and database URL: [dbURL]
	"""
	for (ky,va) in opts.items():
		exec("{}=va".format(ky))
	try:
		dbURL='postgresql://sfdbo@{}:5432/{}'.format(hostname,dbname)
		engine = create_engine(dbURL) 
	except:
		print >> sys.stderr, "***DB ERROR:", sys.exc_info()[1]
		engine = None
	btime = datetime.datetime.now()
	print >> sys.stderr,"BEGIN----- @ {} -----BEGIN".format(btime)
	rmode= wmode
	for i,ticker in enumerate(tkLst):
		print >> sys.stderr,"=== pulling {}: {}".format(i,ticker)
		try:
			symbol=ticker.upper()
			df=data_from_web(symbol,start=start,end=end,days=days,src=src,debugTF=debugTF)
			if df is None or len(df)<1:
				continue
			if(engine is not None and saveDB is True) :
				df.to_sql(tablename,engine,schema='public',index=False,if_exists=rmode)
				rmode= "append"
			if debugTF is True:
				print  >> sys.stderr, df.tail(2)
			if output is not None and len(df)>0:
				sys.stdout.write("{}".format(df.to_csv(sep=sep))) 
		except Exception, e:
			print  >> sys.stderr, "***ERROR {}:{} @ prc_temp_DN.batch_prc_temp():{}".format(i,symbol,str(e))
			continue

	etime = datetime.datetime.now()
	print >> sys.stderr,"Total time: {}".format(etime-btime)
	print >> sys.stderr,"END----- @ {} -----END".format(etime)
	if(engine is not None) :
		engine.dispose()

def opt_prc_temp_DN(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:   list arguments, usually passed from sys.argv
		retParser:      OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class
	"""
	parser = OptionParser(usage="usage: %prog [option] SYMBOL1 ...", version="%prog 0.65",
		description="Script to download Yahoo/FRED history")
	parser.add_option("","--days",action="store",dest="days",default=1,type=int,
		help="number of days from endDate (default: 1)")
	parser.add_option("-s","--start",action="store",dest="start",
		help="start YYYY-MM-DD (default: 1-day-ago)")
	parser.add_option("-e","--end",action="store",dest="end",
		help="end YYYY-MM-DD (default: today)")
	parser.add_option("","--src",action="store",dest="src",default="yh",
		help="source [fred|iex|yahoo](default: yh)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara",
		help="database (default: ara)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="tablename",default="prc_temp_yh",
		help="db tablename (default: prc_temp_yh)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table write-mode [replace|append] (default: replace)")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no save to database (default: save to database)")
	parser.add_option("-o","--output",action="store",dest="output",
		help="OUTPUT type [csv|html|json] (default: no output)")
	parser.add_option("","--no_datetimeindex",action="store_false",dest="tsTF",default=True,
		help="no datetime index (default: use datetime)")
	parser.add_option("","--show_index",action="store_true",dest="indexTF",default=False,
		help="show index (default: False) Note, OUTPUT ONLY")
	parser.add_option("","--sep",action="store",dest="sep",default="|",
		help="output field separator (default: |) Note, OUTPUT ONLY")
	parser.add_option("","--debug",action="store_true",dest="debugTF",default=False,
		help="debugging (default: False)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	(options, args)=opt_prc_temp_DN(sys.argv)
	if len(args)==0 or (len(args)==1 and args[0]=='-'):
		print >> sys.stderr,"\nRead from pipe\n\n"
		tkLst = sys.stdin.read().strip().split()
	elif len(args)==1 and ',' in args[0]:
		tkLst = args[0].strip().split(',')
	else:
		tkLst = args
		options['saveDB']=False
	batch_prc_temp(tkLst,options)
