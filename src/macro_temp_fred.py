#!/usr/bin/env python
""" Program: macro_temp_fred.py 
    Description: Get global macro series history via [pandas_datareader.data] 
    Usage of:
	# from a list of SYMBOLS
	  macro_temp_fred.py [option] SYMBOL1 SYMBOL2 ...
	OR
	# from stdin
	  printf "SELECT series FROM mapping_series_label WHERE series<>label"| /apps/fafa/bin/psql.sh -d eSTAR_2 -At | macro_temp_fred.py -d ara -t macro_temp_fred --use_pipe
    Example:
	# To get [SYMBOL1,SYMBOL2,...] history and save them into [database]:[table]
	  Then use [fred2hist.sql] to insert additional to [macro_hist_fred]
	  Also use ara_globalmacro_hist.sql.create to [create ara_globalmacro_hist]
	1. printf "SELECT series,freq FROM mapping_series_label WHERE series<>label"| /apps/fafa/bin/psql.sh -d eSTAR_2 -At | macro_temp_fred.py -d ara -t macro_temp_fred
	2. /apps/fafa/bin/psql.sh -d eSTAR_2 < fred2hist.sql
	3. /apps/fafa/bin/psql.sh -d eSTAR_2 < ara_globalmacro_hist.sql.create
    Last mod.  Tue Apr  3 16:32:50 EDT 2018
"""
import sys,os
from optparse import OptionParser
import pandas_datareader.data as web
import datetime
import pandas as pd
from sqlalchemy import create_engine

def get_temp_fred(symbol,src="fred",start=None,end=None,days=365):
	df=None
	if end is None:
		end=datetime.datetime.now()
	else :
		end=datetime.datetime.strptime(end,'%Y-%m-%d')
	if start is None:
		start=end - datetime.timedelta(days=days) #- 365 days from end-date
	else :
		start=datetime.datetime.strptime(start,'%Y-%m-%d')
	try:
		print  >> sys.stderr, 'pulling {} from {} start {} to {}'.format(symbol,src,start,end)
		df = web.DataReader(symbol,src,start,end)
	except:
		print >> sys.stderr,"No data selected for ", symbol
		return df

	df.columns=['value']
	df=df.dropna(axis=0, how='any') #- drop if [any|all] is nan
	df["pbdate"]=[int(x.strftime("%Y%m%d")) for x in df.index ]
	df["series"]=symbol
	df=df[['series','value','pbdate']]
	return df

def macro_temp_fred(tkLst,args):
	""" Get fred history via [pandas_datareader.data] 
		from a list of tickers:[tkLst]
		and save them into table:[tablename] and database URL: [dbURL]
	"""
	for (ky,va) in args.iteritems():
		exec("{}=va".format(ky))
	print >> sys.stderr, "host={};dbname={};tablename={};start={}".format(hostname,dbname,tablename,start)
	engine = None
	if saveDB is True:
		try:
			dbURL='postgresql://sfdbo@{}:5432/{}'.format(hostname,dbname)
			print >> sys.stderr, "***DB Creating:",dbURL 
			engine = create_engine(dbURL) 
		except:
			print >> sys.stderr, "***DB ERROR:", sys.exc_info()[1]
	fqDct={'D':1,'W':5,'M':15,'Q':45,'Y':60}
	for i,ticker in enumerate(tkLst):
		print >> sys.stderr,"--------------------------------------------------"
		print >> sys.stderr,"Running [{}] at {}".format(ticker,"macro_temp_fred()")
		try:
			if '|' in ticker:
				(symbol,fq) = ticker.split('|')
				mx=fqDct.get(fq.upper())
				if mx>0:
					nday = days * mx
			else:
				symbol=ticker
				nday = days
			df=get_temp_fred(symbol,src="fred",start=start,end=end,days=nday)
			if len(df) < 1:
				continue 
			if(engine is not None) :
				df['value'] = df['value'].astype(float)
				df.to_sql(tablename, engine, schema='public', index=False, if_exists=wmode)
				wmode= 'append'
			else:
				print >> sys.stdout, df.to_csv(sep="\t")
			print >> sys.stderr,df.head(3)
			print >> sys.stderr,df.tail(3)
			print >> sys.stderr,"Total selections:{}".format(len(df))
			print >> sys.stderr,"--------------------------------------------------"
		except Exception,e:
			print >> sys.stderr, "**ERROR {}. {} @ {}:\n\t{}".format(i,symbol,"macro_temp_fred()",str(e))
			continue

	if(engine is not None) :
		engine.dispose()

def opt_macro_temp_fred():
	parser = OptionParser(usage="usage: %prog [option] SYMBOL1 ...", version="%prog 1.0")
	parser.add_option("-e","--end",action="store",dest="end",
		help="endDate YYYY-MM-DD (default: today)")
	parser.add_option("","--days",action="store",dest="days",default=730,type=int,
		help="number of days from endDate (default: 730)")
	parser.add_option("-s","--start",action="store",dest="start",
		help="startDate YYYY-MM-DD (default: ndays from endDate)")
	parser.add_option("-t","--table",action="store",dest="tablename",default="macro_temp_fred",
		help="tableName (default: macro_temp_fred)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="eSTAR_2",
		help="database (default: eSTAR_2)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host (default: localhost)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table write-mode [replace|append|fail] (default: replace)")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="save to database? [Y|N] (default: Y)")
	parser.add_option("-i","--use_pipe",action="store_true",dest="pipeYN",default=False,
		help="use stdin from pipe")
	(options, args) = parser.parse_args()
	return (vars(options), args)

if __name__ == '__main__':
	""" Get fred history via [pandas_datareader.data] 
		Usage of:
		printf "SELECT series,freq FROM mapping_series_label WHERE series<>label"| psql.sh -d eSTAR_2 -At | macro_temp_fred.py -d ara -t macro_temp_fred --use_pipe
	"""
	(options, args)=opt_macro_temp_fred()
	print >> sys.stderr, (options, args)
	if options['pipeYN'] is True or (len(args)>0 and args[0]=='-'):
		tkLst = sys.stdin.read().strip().split("\n")
	else:
		tkLst = args
	if len(tkLst)<1:
		print >> sys.stderr, "usage: %prog [option] SYMBOL1 ..."
		exit(1)
	macro_temp_fred(tkLst,options)
