#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import rpy2.robjects as robj
from rpy2.robjects import pandas2ri, r
import datetime
import pandas as pd
from _alan_calc import get_datax

def run_fcs(ticker,debugTF=False,funcname='rGARCH'):
	if isinstance(ticker,pd.DataFrame):
		datax=ticker
		ticker = ''
	else: # get data
		datax=get_datax(ticker)

	if debugTF is True:
		print datax.tail()

	if 'ticker' in datax:
		ticker=datax['ticker'].iloc[0]

	# get r-code
	pandas2ri.activate()
	rstring='source("./_alan_ohlc_fcs.r")'
	r(rstring)
	
	# convert to r-data 
	df=pandas2ri.py2ri(datax)

	# run r-function [rGARCH|rAR]
	if funcname in ['rGARCH','rAR']:
		ret=robj.globalenv[funcname](df,p=.70,debugTF=debugTF)
		dg=pandas2ri.ri2py(ret)
		dg['ticker']=ticker
	return (dg,datax)

from lsi_daily import run_comment_fcst
from _alan_calc import conn2pgdb
if __name__ == '__main__':
	global pgDB
	args=sys.argv[1:]
	ticker='AAPL' if len(args)<1 else args[0]
	funcname='rGARCH' if len(args)<2 else args[1]
	(df,datax)=run_fcs(ticker,funcname=funcname,debugTF=False)
	pgDB=conn2pgdb(dbname='ara',hostname='localhost')
	fp=(df.loc[df['freq']=="W"]).iloc[0]
	print run_comment_fcst(ticker=ticker,label="蘋果",fp=fp,pgDB=pgDB)
	print df
	#print datax.tail()
