#!/usr/bin/env python
''' DEPRECATED, use iex_types_batch.py
    Grab past 5d history from available stock symbols 
    and save them to [prc_temp_iex, iex_chart_temp]
    then update into [prc_hist_iex, iex_chart_hist] tables
    Note, cronjob every Saturday
    Last mod., Fri Aug 17 22:02:27 EDT 2018
'''
import sys
from optparse import OptionParser
import pandas as pd
from sqlalchemy import create_engine
import datetime
import requests
from pandas.io.json import json_normalize
import json

def sql_temp2hist(pgDB=None,temp="prc_temp_iex",hist="prc_hist_iex",col1="name",col2="pbdate"):
	if pgDB is None:
		return None
	xqTmp="""DELETE FROM {hist} B USING {temp} C
	WHERE B.{col1} = C.{col1} AND B.{col2} = C.{col2};
	INSERT INTO {hist} SELECT DISTINCT * FROM {temp}
	"""
	dux=locals()
	xqr = xqTmp.format(**dux)
	pgDB.execute(xqr,pgDB)
	return pgDB

def reshape_iex_data(ticker,jdTmp,debugTF):
	jdX=jdTmp[ticker]['chart']
	dx=json_normalize(jdX)
	dx["pbdate"]=map(lambda x:int(datetime.datetime.strptime(x,'%Y-%m-%d').strftime("%Y%m%d")),dx['date'])
	dx['src']='iex'
	dx['ticker']=ticker
	dx['name']=ticker.replace(".","-") if ticker!='AGM.A' else ticker
	dx['adjusted']=dx['close']
	dx['volume']=dx['volume'].astype("int")
	if debugTF is True:
		print >> sys.stderr,dx.iloc[:1]
		print >> sys.stderr,dx.iloc[-1:]
	return(dx[["open","high","low","close","volume","adjusted","pbdate","name"]], dx[["src","ticker","pbdate","name","open","high","low","close","change","changeOverTime","changePercent","date","label","unadjustedVolume","volume","vwap","adjusted"]])

def get_tkLst_iex(pgDB,activeON):
        df=pd.read_json("https://api.iextrading.com/1.0/ref-data/symbols")
        if activeON is True: # apply only to act_code=1 tickers
	       m=pd.read_sql('SELECT distinct ticker FROM mapping_ticker_cik WHERE act_code=1',pgDB)
	       mlist=map(lambda x: x.replace("-","."),m.ticker)
	       tkLst=list(df[['symbol']].query('symbol=={}'.format(mlist)).symbol)
        else:
	       tkLst=list(df['symbol'])
        return tkLst

def iex_hist_batch(opt,args):
	''' grab past 5d history from available stock symbols and save them to prc_temp_iex
		then update into price_hist_iex table
	'''
	for ky,va in opt.items():
	       exec("{}=va".format(ky))
	if debugTF is True:
		print >> sys.stderr, opt,args
		print >> sys.stderr, sorted(opt.keys())
		print >> sys.stderr, [dbname,hostname,ranged,saveDB,table1,table2,wmode]
	pgDB = create_engine('postgresql://sfdbo@{}:5432/{}'.format(hostname,dbname))
	dbname='ara';pgDB = create_engine('postgresql://sfdbo@localhost:5432/'+dbname)
	tkLst=get_tkLst_iex(pgDB,activeON) if len(args)<1 else sys.stdin.read().strip().split("\n") if args[0]=='-' else args
	n=100
	tkM = [tkLst[i * n:(i + 1) * n] for i in range((len(tkLst) + n - 1) // n )]
	urx="https://api.iextrading.com/1.0/stock/market/batch?symbols={}&types=chart&range={}"
	df=pd.DataFrame()
	dbscm='public';dbidx=False
	rmode=wmode
	for j,tkTmp  in enumerate(tkM):
		tkStr=','.join(tkTmp)
		url=urx.format(tkStr,ranged)
		jdTmp=requests.get(url).json()
		for jk, ticker  in enumerate(tkM[j]):
			print >> sys.stderr,"===RUNNING {}:{}".format(j*n+jk+1,ticker)
			try:
				(da,db)=reshape_iex_data(ticker,jdTmp,debugTF)
				print >> sys.stderr,db.iloc[:1]
				print >> sys.stderr,db.iloc[-1:]
				if saveDB is True:
					da.to_sql(table1,pgDB,schema=dbscm,index=dbidx,if_exists=rmode)
					db.to_sql(table2,pgDB,schema=dbscm,index=dbidx,if_exists=rmode)
					rmode="append"
			except Exception,e:
				print  >> sys.stderr, str(e), 'Fail at {}. {}'.format(j*n+jk,ticker)
				continue
	if saveDB is False:
		return (pgDB,da,db)
	temp=table1;hist=temp.replace('temp','hist')
	sql_temp2hist(pgDB,temp=temp,hist=hist)
	temp=table2;hist=temp.replace('temp','hist')
	sql_temp2hist(pgDB,temp=temp,hist=hist)
	return (pgDB,da,db)

def opt_iex_hist_batch(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:   list arguments, usually passed from sys.argv
		retParser:      OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class
	"""
	parser = OptionParser(usage="usage: %prog [option]", version="%prog 0.1",
		description="Pull up-to-date stock history from IEX")
	parser.add_option("-r","--range",action="store",dest="ranged",default="5d",
		help="range [5d,1m,5y] (default: 5d)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara",
		help="database (default: ara)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="table1",default="prc_temp_iex",
		help="db tablename (default: prc_temp_iex)")
	parser.add_option("","--table2",action="store",dest="table2",default="iex_chart_temp",
		help="db additional table (default: iex_chart_temp)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table write-mode [replace|append] (default: replace)")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no save to database (default: save to database)")
	parser.add_option("","--no_active",action="store_false",dest="activeON",default=True,
	       help="apply to all IEX symbols(default: active symbols ONLY)")
        parser.add_option("","--debug",action="store_true",dest="debugTF",default=False,
	       help="debugging (default: False)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	opt,args =opt_iex_hist_batch(sys.argv)
	pgDB,da,db=iex_hist_batch(opt,args)
	pgDB.dispose()
