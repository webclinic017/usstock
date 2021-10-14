#!/usr/bin/env python
'''
Description: backtest up/down sign for 1-step out-of-sample forecast for stock

Usage of:
python backtest_sign.py AAPL IBM
OR
printf "AAPL\nIBM\n" | python backtest_sign.py

Note: model is used auto.arima in R with log-diff return as benchmark

Last Mod: Fri May  3 16:44:28 EDT 2019
'''
from _alan_calc import pull_stock_data as psd,subDict,upd_temp2hist as ut2h
import sys
import rpy2.robjects as robj
from rpy2.robjects import pandas2ri, r
import pandas as pd
import warnings
from rpy2.rinterface import RRuntimeWarning
warnings.filterwarnings("ignore", category=RRuntimeWarning)
import math
pandas2ri.activate()


def get_tkLst(tkLst=[]):
        if len(tkLst)<1:
                tkLst=sys.argv[1:]
        if len(tkLst)<1 or (len(tkLst)==1 and tkLst[0]=='-'):
                tkLst = sys.stdin.read().split()
        return tkLst

rstring="""
library(xts)
library(lubridate)
library(forecast)
arima=function(dt,nfcs=3){
	tsDate=as.Date(as.character(dt$pbdate),"%Y%m%d")
	tsx=xts(dt[,'pchg'],order.by=ymd(dt$pbdate))
	afit=auto.arima(tsx)
	afcs=predict(afit,nfcs)
	#sd=afcs$se[1]
	#fcs1d=afcs$pred[1]
	return(afcs)
}
"""


def backtest_sign(ticker="AAPL"):
	datax=psd(ticker,days=365,src='yh')
	#datax['pchg']=datax['close'].pct_change()
	datax['pchg']=datax['close'].apply(math.log).diff()*100.
	datax=datax.dropna()
	nobs=67
	dd=[]
	for j in range(len(datax)-nobs-1):
		df=pandas2ri.py2ri(datax.iloc[-nobs-1-j:-1-j])
		#print(df)
		ret=robj.globalenv['arima'](df,nfcs=1)
		#dg=pandas2ri.ri2py(ret)
		afcs = dict(zip(ret.names, map(list,list(ret))))
		actual = datax['pchg'].iloc[-j-1]
		pbdate = datax['pbdate'].iloc[-j-1]
		fcs=dict(ticker=ticker,actual=actual,pbdate=pbdate)
		fcs.update(pred=afcs['pred'][0],sd=afcs['se'][0])
		fcs['accuracy'] = 1 if fcs['actual']*fcs['pred'] >=0 else -1
		dd.append(fcs)
		#print(fcs)
		#for x,y in dict(zip(ret.names, map(list,list(ret)))).items():
		#	print "===KEY:{}\n{}".format(x,y)
	dfcs = pd.DataFrame(dd)
	sys.stderr.write(dfcs.head().to_csv(index=False,header=True,sep="\t"))
	sys.stderr.write(dfcs.tail().to_csv(index=False,header=False,sep="\t"))
	sys.stderr.write("{}: GOOD:{}, BAD:{}".format(ticker,dfcs.loc[dfcs['accuracy']==1].shape[0],dfcs.loc[dfcs['accuracy']==-1].shape[0]))
	return dfcs

#-----------------------------------------------------
r(rstring)
tkLst = get_tkLst()
pgDB=None
for ticker in tkLst:
	try:
		df = backtest_sign(ticker)
		pgDB,xqr = ut2h(pgDB=pgDB,temp='backtest_sign_temp',hist='backtest_sign_hist',pcol=['ticker','pbdate'],dbname='ara',df=df)
	except Exception as e:	
		print("**ERROR: {} @ {}".format(str(e),ticker))
