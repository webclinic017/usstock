#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" OHLC Analysis
	Usage of:
	printf "select ticker from mapping_ticker_cik order by ticker" | /apps/fafa/bin/psql.sh -d eSTAR_2 -At | ohlc_analysis.py
	Created on Fri Dec 29 14:14:32 2017
	List of Functions:
	def calc_ohlc_pnl(data,ret,prd=125,xfl=-1,xcap=1):
	def run_ohlc_analysis(ticker,pgDB,**kwargs):
	def save_ohlc_pattern(ret,dbM=None,tablename="ohlc_pattern"):
	def save2pgdb(df,engine,tablename='ohlc_pnl',wmode='replace'):
	def batch_ohlc_analysis(tkLst,**kwargs):
	* -def function is deprecated
	* code logic is written in forward counting, i.e.,
		 t is referred as j+2 for 3-day pattern, so t-2 is j 
		 t is referred as j+1 for 2-day pattern, so t-1 is j 
	Last mod., @ Fri Feb 23 15:08:20 EST 2018
"""
import sys,os
import datetime
import pandas as pd
import numpy as np
from pymongo import MongoClient
from sqlalchemy import create_engine
from _alan_pnl import calc_ohlc_pnl
from _alan_calc import pull_stock_data,run_tech,opt_alan_calc,conn2pgdb,conn2mgdb,save2pgdb
from _alan_pattern import calc_ohlc_pattern,add_MACD_pattern

def run_ohlc_analysis(ticker,pgDB=None,**kwargs):
	for ky,va in kwargs.items():
		exec("{}=va".format(ky))
		#globals()[ky]=va
	# Get OHLC data from DB or internet
	try:
		dx=pull_stock_data(ticker,start=start,end=end,src=src,dbname=dbname,hostname=hostname,pgDB=pgDB)
		dx.fillna(method='ffill',inplace=True)
		if dx is None:
			return (None,None,None)
		dx['ticker']=ticker
	except:
		print >> sys.stderr, "***ERROR @ run_ohlc_analysis():", sys.exc_info()[1]
		return (None,None,None)

	# Calculate OHLC, MACD, RSI and SMA
	data=run_tech(dx,pcol='close',winLst=[5,10,20])

	# Calculate morning star and evening star
	ret_ptn=calc_ohlc_pattern(data)

	# add MACD to pattern list
	ret_ptn=add_MACD_pattern(data,ret_ptn)

	# Calculate PnL 
	ret_pnl=calc_ohlc_pnl(data,ret_ptn,prd=251,xfl=-1,xcap=1)

	return (data,ret_ptn,ret_pnl)

def save_ohlc_pattern(ret,dbM=None,tablename="ohlc_pattern"):
	if dbM is None:
		return None
	for mobj in ret:
		print >>sys.stderr, mobj
		print >>sys.stderr, mobj["ticker"],mobj["name"]
		dbM[tablename].delete_one({"ticker":mobj["ticker"],"name":mobj["name"]})
		dbM[tablename].insert_one(mobj)
	return len(ret)

def save2pgdb(df,engine,tablename="temp",wmode='replace'):
	df.to_sql(tablename, engine, schema='public', index=False, if_exists=wmode)

def batch_ohlc_analysis(tkLst,**kwargs):
	for ky,va in kwargs.items():
		exec("{}=va".format(ky))
		#globals()[ky]=va
	if debugTF:
		print >> sys.stderr, kwargs
	pgDB=conn2pgdb(dbname=dbname,hostname=hostname)
	mgDB=conn2mgdb(dbname=dbname,hostname=hostname)
	wmode='replace'
	for ticker in tkLst:
		try:
			(data,ret_ptn,ret_pnl) = run_ohlc_analysis(ticker,pgDB,**kwargs)
			if data is None or saveDB == False:
				print ret_pnl.to_csv(sep="\t")
				continue
			save2pgdb(data,pgDB,tablename=tablename,wmode=wmode)
			save_ohlc_pattern(ret_ptn,mgDB,tablename=ohlc_pattern) 
			save2pgdb(ret_pnl,pgDB,tablename=ohlc_pnl,wmode=wmode)
			wmode='append'
		except:
			print >> sys.stderr, "***ERROR:",ticker,"@ batch_ohlc_analysis():", sys.exc_info()[1]
			continue
	return len(tkLst)

if __name__ == '__main__':
	(options, args)=opt_alan_calc(sys.argv)
	if len(args)==0 :
		print >> sys.stderr,"\nRead from pipe\n\n"
		tkLst = sys.stdin.read().strip().split("\n")
	else :
		tkLst = args
		options['saveDB']=False
	batch_ohlc_analysis(tkLst,**options)
	exit(0)
