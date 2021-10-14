#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Program: macro_pct_change.py 
    Description: conversion macro series index into annual percentage change rate in %
    Usage of:
	# from a list of SYMBOLS (|frequency)
	python macro_pct_change.py [option] SYMBOL1 SYMBOL2 ...
	OR
	# from stdin
	printf "SELECT series,freq FROM mapping_series_label WHERE source='deriv'"| /apps/fafa/bin/psql.sh -d ara -At | python macro_pct_change.py -d ara -t macro_pct_change --use_pipe
    Example:
	# To get [SYMBOL1,SYMBOL2,...] history and save them into [database]:[table]
	  Then use [fred2hist.sql] to insert additional to [macro_hist_fred]
	  Also use ara_globalmacro_hist.sql.create to [create ara_globalmacro_hist]
	1. printf "SELECT series,freq FROM mapping_series_label WHERE series<>label"| /apps/fafa/bin/psql.sh -d eSTAR_2 -At | macro_pct_change.py -d ara -t macro_pct_change
	2. /apps/fafa/bin/psql.sh -d ara < ara_globalmacro_hist.sql.create
    Last mod.,  Tue Apr 10 23:38:06 CST 2018
"""
import sys,os
if sys.version_info.major == 2:
	reload(sys)
	sys.setdefaultencoding('utf8')
from optparse import OptionParser
import pandas as pd, numpy as np
from sqlalchemy import create_engine
from rpy2.robjects import numpy2ri,pandas2ri, r
pandas2ri.activate()
numpy2ri.activate()

def most_sig_macro(tkLst=[],category='currency',dfTF=True,zlevel=1.645,debugTF=False,src='fred',days=366):
	from _alan_calc import find_mnmx_wmqy
	dsm,dsmHist = batch_sig_macro(tkLst=tkLst,category=category,zlevel=zlevel,debugTF=debugTF,src=src,days=days)
	d={}
	if dsm['sigZ'].iloc[0]!=0 :
		d = dsm.iloc[0].to_dict()
		d['data'] = dsmHist[d['series']]
		yqmw = find_mnmx_wmqy(d['data'],pcol='value',chgcol='value')
		d['yqmw'] = yqmw
	return d

def batch_sig_macro(tkLst=[],category='currency',dfTF=True,zlevel=1.645,debugTF=False,src='fred',days=366):
	'''
	determine significant 90% level change (2-tails) of currencies (or rates/commodity)
	'''
	if len(tkLst)<1:
		if category=='rates':
			tkLst = "DGS3MO DGS2 DGS5 DGS10 DGS30".split()
		elif category=='commodity':
			tkLst = "CL=F GC=F".split()
		else:
			tkLst = "DTWEXM TWD=X CNY=X JPY=X EURUSD=X AUDUSD=X GBPUSD=X".split()
	ret=[]
	dsmHist={}
	for ticker in tkLst:
		try:
			d, datax = find_sig_macro(ticker,zlevel=zlevel,debugTF=debugTF,src=src,days=days)
			ret.append(d)
			dsmHist.update({ticker:datax})
		except Exception as e:
			continue
	if dfTF is False:
		return ret
	dm = pd.DataFrame(ret)
	cols = list( set(dm.columns) &  {'series','value','freq','label','label_cn','max','mean','min','pbdate','pchg','sigZ','zscore','std'})
	dsm = dm[cols]
	dsm = dsm.loc[(dsm.zscore.abs().sort_values(ascending=False).index),:]
	dsm.reset_index(drop=True,inplace=True)
	return dsm, dsmHist

def find_sig_macro(ticker='EURUSD=X',zlevel=1.645,debugTF=False,src='fred',days=366):
	from _alan_calc import pull_stock_data as psd
	try:
		df = psd(ticker,src=src,days=days,debugTF=debugTF).sort_values(by='pbdate')
		df['pchg']=df['close'].copy().pct_change()
		df.rename(columns={'close':'value','name':'series'},inplace=True)
		dsr=df['pchg'].describe().to_dict()
		d=df.iloc[-1].to_dict()
	except Exception as e:
		print >> sys.stderr, "**ERROR: {}".format(str(e))
		return {}
	d.update(dsr)
	zscore = (d['pchg']-d['mean'])/d['std']
	sigZ = 1 if zscore > zlevel else -1 if zscore < -zlevel else 0
	d.update(zscore=zscore,sigZ=sigZ)
	return d, df

def sql_fred2hist(pgDB=None,tablename="macro_pct_change"):
	if pgDB is None:
		return None
	xqTmp="""DELETE FROM macro_hist_fred B USING {0} C
	WHERE B.series = C.series AND B.pbdate = C.pbdate;
	INSERT INTO macro_hist_fred SELECT DISTINCT * FROM {0}
	"""
	xqr = xqTmp.format(tablename)
	pgDB.execute(xqr,pgDB)
	#pd.read_sql_query(xqr,pgDB)
	return pgDB

def macro_pct_change(tkLst,args):
	for (ky,va) in args.iteritems():
		exec("{}=va".format(ky))
	print >> sys.stderr, "host={};dbname={};tablename={}".format(hostname,dbname,tablename)
	engine = None
	try:
		dbURL='postgresql://sfdbo@{}:5432/{}'.format(hostname,dbname)
		print >> sys.stderr, "***DB Creating:",dbURL 
		pgDB = create_engine(dbURL) 
	except:
		print >> sys.stderr, "***DB ERROR:", sys.exc_info()[1]
		return None
	fqDct={'D':251,'W':52,'M':12,'Q':4,'Y':1}
	xqTmp="SELECT DISTINCT * FROM macro_hist_fred WHERE series='{}' ORDER BY pbdate"
	for i,ticker in enumerate(tkLst):
		print >> sys.stderr,"--------------------------------------------------"
		print >> sys.stderr,"Running [{}] at {}".format(ticker,"macro_pct_change()")
		try:
			if '|' in ticker:
				(symbol,fq) = ticker.split('|')
				periods=fqDct.get(fq.upper())
			else:
				symbol=ticker
				periods = 1
			if '_PCTCHG' not in symbol:
				print >> sys.stderr, "**WARNING {}. {} {}".format(i,symbol,"is not a valid name for conversion")
				continue
			dd=pd.read_sql(xqTmp.format(symbol.split('_')[0]),pgDB)
			df=dd[['pbdate']].copy()
			df['series']=symbol
			df['value']=dd['value'].pct_change(periods=periods).apply(lambda x: x*100.)
			df=df.dropna(axis=0, how='any')
			df=df[["series","value","pbdate"]]
			print >> sys.stderr,df.head(2)
			print >> sys.stderr,df.tail(2)
			if saveDB is True:
				df.to_sql(tablename, pgDB, schema='public', index=False, if_exists=wmode)
				wmode='append'
		except Exception as e:
			print >> sys.stderr, "**ERROR {}. {} @ {}:\n\t{}".format(i,ticker,"macro_pct_change()",str(e))
			continue
	if saveDB is True:
		sql_fred2hist(pgDB=pgDB,tablename=tablename)
	if(pgDB is not None) :
		pgDB.dispose()

def opt_macro_pct_change():
	parser = OptionParser(usage="usage: %prog [option] SYMBOL1 ...", version="%prog 1.0")
	parser.add_option("-t","--table",action="store",dest="tablename",default="macro_pct_change",
		help="tableName (default: macro_pct_change)")
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
	(options, args)=opt_macro_pct_change()
	print >> sys.stderr, (options, args)
	if options['pipeYN'] is True:
		tkLst = sys.stdin.read().strip().split("\n")
	else:
		tkLst = args[0:]
	if len(tkLst)<1:
		print >> sys.stderr, "usage: %prog [option] SYMBOL1 ..."
		exit(1)
	macro_pct_change(tkLst,options)
