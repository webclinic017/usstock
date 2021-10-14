#!/usr/bin/env python3
'''
Usage of: 
To create iex_spark_hist table (also see yh_spark_hist)
python3 -c "from iex_chart import iex_spark_hist as quos;d=quos(debugTF=True)"

OR DEPRECATED
OR to test
python3 -c "from iex_chart import iex_quote_short as quos;d=quos(['AAPL','MDB'],saveDB=False,debugTF=True);print(d)"

Note:
require yh_quote_curr before SoD run for quotes to spark hist and changes
Last mod., Thu Feb  6 17:41:06 EST 2020
'''
import sys, datetime
import requests
import pandas as pd
from pandas.io.json import json_normalize
from _alan_str import insert_mdb,find_mdb
from _alan_calc import getKeyVal,subDict,renameDict,conn2mgdb
from _alan_date import next_date

def iex_spark_hist(tkLst=None,dbname='ara',tablename='iex_spark_hist',zpk={'ticker','epochs'},chartLast=3,chartInterval=5,saveDB=True,**optx):
	''' Pull iex OHLC minute data
	and save to  MDB::ara:iex_spark_hist
	Note,
	iex_spark_batch() wrapper 
	run every 10 minute for past 10 records at 1,11,21,31,41,51
	also see yh_spark_hist 
	'''
	dd =  iex_spark_batch(tkLst,chartLast=chartLast,chartInterval=chartInterval,zpk=zpk,dbname=dbname,tablename=tablename,saveDB=saveDB,**optx)
	return dd

def epoch_parser(x,s=1000): return datetime.datetime.fromtimestamp(int(x/s))

def list2chunk(v,n=100):
	''' Utility function
	partition an array 'v' into arrays limit to 'n' elements
	'''
	import numpy as np
	return [v[i:i+max(1,n)] for i in np.arange(0, len(v), n)]

def getTkLst(tkLst=None,tgtk='AAPL',dbname='ara',tablename='yh_quote_curr',days=-7):
	''' Utility function
	Get a ticker list of 'yh_quote_curr' from lookback 'days'
	'''
	if tkLst is not None and len(tkLst)>0:
		mxdate=int(next_date(days=days,dformat=('%Y%m%d'),dtTF=False))
		return tkLst,mxdate
	mdb=conn2mgdb(dbname=dbname)
	mCur=mdb[tablename]
	try:
		mxdate=max(mCur.find({"ticker":tgtk}).distinct("pbdate"))
		mxdate=int(next_date(mxdate,days=days,dformat=('%Y%m%d'),dtTF=False))
	except:
		mxdate=int(next_date(days=days,dformat=('%Y%m%d'),dtTF=False))
	jo={'exchange':{'$ne':'PNK'},'ticker':{'$regex':'^((?![\^|=|0]).)*$'},'pbdate':{'$gt':mxdate}}
	r = mCur.find(jo,{"_id":0,"ticker":1}).distinct("ticker")
	tkLst = [x.replace('-','.') for x in r]
	return sorted(tkLst),mxdate

def iex_batchRaw(tkLst=[],filter='*',types='chart',range='1d',chartLast=-999,chartInterval=1,**optx):
	''' Utility function
	to pull data from https://cloud.iexapis.com/stable/stock/market/batch
	'''
	if not tkLst:
		return {}
	if 'apiTF' in optx and optx['apiTF']:
		utmp='https://api.iextrading.com/1.0/stock/market/batch?'
	else:
		if 'sandboxTF' in optx and optx['sandboxTF']:
			ut1='https://sandbox.iexapis.com/stable/stock/market/batch?token={}&chartIEXOnly=True&'
			token='Tsk_868cf1c000694de79c779a5437fc1d75'
		else:
			ut1='https://cloud.iexapis.com/stable/stock/market/batch?token={}&chartIEXOnly=True&'
			token='sk_c3846ce073c849f5838e5ae0be3d005d'
		utmp=ut1.format(token)
	urx=utmp+'symbols={}&types={}&range={}'
	url = urx.format(','.join(tkLst),types,range)
	if filter != '*':
		ftx="&filter={}".format(filter)
		url += ftx
	if chartLast>0:
		url += "&chartLast={}".format(chartLast)
	if chartInterval>1:
		url += "&chartInterval={}".format(chartInterval)
	res=requests.get(url,timeout=5)
	sys.stderr.write("====={}\n{}\n".format(res,url))
	jdTmp=res.json()
	return jdTmp

def iex_spark_process(dd,dq=[],**optx):
	''' Utility function
	to process data in iex_spark_batch()
	'''
	from _alan_calc import renameDict,subDict
	dx=[]
	for k,v in dd.items():
		mtmp = v['chart']
		m = [x for x in mtmp if x['close'] is not None]
		if len(m)<1:
			continue
		# grab xclose value from dq
		xq=[x for x in dq if x['ticker'] == k]
		if len(xq)>0:
			xclose = getKeyVal(xq[0],'xclose',None)
		else:
			xclose = None
		for j,x in enumerate(m):
			m[j].update(ticker=k.replace('.','-'))
			pbdt=pd.Timestamp("{} {}".format(x['date'],x['minute']))
			hhmm = x['minute'].replace(":","")
			pbdate = int(x['date'].replace("-",""))
			epochs = int(pbdt.strftime('%s000') )
			m[j].update(pbdt=pbdt,pbdate=pbdate,epochs=epochs,hhmm=hhmm)
			if xclose is not None and xclose>0:
				try:
					pchg=x['close']/xclose-1
					change=x['close']-xclose
					m[j].update(xclose=xclose,pchg=pchg,change=change)
				except Exception as e:
					sys.stderr.write("**ERROR:{},{}\n{}\n".format(k,j,m[j]))
		#if debugTF:
		#	sys.stderr.write("====={}\n{}\n".format(k,m))
		dx.extend(m)
	return dx

def iex_spark_batch(tkLst=[],filter='*',types='chart',range='1d',chartLast=-999,nchunk=100,**optx):
	''' Utility function
	to process data in iex_spark_hist()
	'''
	dbname = getKeyVal(optx,'dbname','ara')
	tablename = getKeyVal(optx,'tablename','iex_spark_hist')
	tabq = 'yh_quote_curr'
	zpk = getKeyVal(optx,'zpk',['ticker','epochs'])
	ordered = getKeyVal(optx,'ordered',False)
	optx.pop('tablename',None)
	optx.pop('zpk',None)
	optx.pop('ordered',None)
	saveDB = getKeyVal(optx,'saveDB',False)
	dfTF = getKeyVal(optx,'dfTF',False)
	tkLst,mxdate = getTkLst(tkLst)
	chunkLst = list2chunk(tkLst,nchunk)
	mdb=conn2mgdb(dbname=dbname)
	mCur=mdb[tabq]
	jdM=[]
	if len(tkLst)<1:
		#dQuotes=iex_quote_short(tkLst,isNew=True)
		sys.stderr.write("**ERROR: quotes date not update @ {},:{}\n".format(mxdate))
		return []
	for j,tkM in enumerate(chunkLst):
		try:
			jdTmp = iex_batchRaw(tkM,filter=filter,types=types,range=range,chartLast=chartLast,**optx)

			# pull quote info 
			jobj={'ticker':{'$in':tkM}}
			res=mCur.find(jobj)
			jqTmp=[x for x in res]

			if not isinstance(jdTmp,dict) or len(jdTmp)<1:
				continue
			dx = iex_spark_process(jdTmp,dq=jqTmp,**optx)
			if len(dx)<1:
				continue
			if saveDB is True:
				insert_mdb(dx,zpk=zpk,tablename=tablename,ordered=ordered,**optx)
			if not dfTF:
				jdM.extend(dx)
		except Exception as e:
			sys.stderr.write("**ERROR: {}\n{}\n".format(tkM,str(e)))
			continue
	sys.stderr.write("tkLst{}:\n{}\n".format(tkLst[:5],jdM[:5]))
	if dfTF is True:
		jdM = pd.DataFrame(jdM)
	return jdM

def iex_batchTypes(tkLst=[],filter='*',types='chart',range='1d',chartLast=-999,nchunk=100,**optx):
	''' Utility function
	Pull iex data in batch mode
	'''
	chunkLst = list2chunk(tkLst,nchunk)
	jdM={}
	for j,tkM in enumerate(chunkLst):
		try:
			jdTmp = iex_batchRaw(tkM,filter=filter,types=types,range=range,chartLast=chartLast,**optx)
			if not isinstance(jdTmp,dict) or len(jdTmp)<1:
				continue
			jdM.update(jdTmp)
		except Exception as e:
			sys.stderr.write("**ERROR: {}\n".format(str(e)))
			continue
	sys.stderr.write("tkLst{}:\n{}\n".format(tkLst,jdM))
	return jdM

# DEPRECATED (limited usage per month)
def iex_quote_short(tkLst=[],filter='',types='quote',dbname='ara',tablename='iex_quote_short',saveDB=True,debugTF=False,**optx):
	''' Pull in-the-fly IEX quotes
	and save to  MDB::ara:iex_quote_short
	mapping {
	"symbol":"ticker","changePercent":"pchg","latestPrice":"close",
	"latestUpdate":"epochs","previousClose":"xclose","avgTotalVolume":"volume",
	"previousVolume":"xvolume" }
	'''
	if not filter:
		filter='iexMarketPercent,previousClose,avgTotalVolume,previousVolume,week52High,peRatio,iexLastUpdated,companyName,calculationPrice,latestPrice,isUSMarketOpen,week52Low,lastTradeTime,primaryExchange,ytdChange,symbol,latestTime,change,marketCap,changePercent,latestSource,latestUpdate'
	isNew=getKeyVal(optx,'isNew',False)
	tkLst,mxdate = getTkLst(tkLst)
	jdTmp = iex_batchTypes(tkLst,filter=filter,types=types,**optx)
	dicX={"symbol":"ticker","changePercent":"pchg","latestPrice":"close","latestUpdate":"epochs","previousClose":"xclose","avgTotalVolume":"volume","previousVolume":"xvolume"}
	dm = []
	for ticker in tkLst:
		try:
			if ticker not in jdTmp:
				continue
			elif types not in jdTmp[ticker]:
				continue
			elif len(jdTmp[ticker][types])<1:
				coninue
			dx=jdTmp[ticker][types]
			renameDict(dx,dicX)
			pbdt = epoch_parser(dx['epochs'])
			pbdate = int(pbdt.strftime('%Y%m%d'))
			dx.update(ticker=ticker,pbdt=pbdt,pbdate=pbdate)
			dm.append(dx)
		except Exception as e:
			sys.stderr.write("**ERROR: {}:{}\n".format(ticker,str(e())))
			continue
	if len(dm)<1:
		return {}
	if saveDB is True:
		zpk = getKeyVal(optx,'zpk',['ticker','epochs'])
		ordered = getKeyVal(optx,'ordered',False)
		optx.pop('zpk',None)
		optx.pop('ordered',None)
		mdb=conn2mgdb(dbname=dbname)
		if isNew is True:
			mdb[tablename].drop()
		insert_mdb(dm,clientM=mdb,zpk=zpk,tablename=tablename,ordered=ordered,**optx)
	return dm

# DEPRECATED
def iex_minute_chart(tkLst=[],filter='',types='chart',range='1d',chartLast=-999,nchunk=100,dfTF=True,debugTF=False,tablename='iex_spark_hist',dbname=None,zpk={'ticker','epochs'},**optx):
	'''
	Pull minute ohlc pricing data from IEX but use marketVolume as volume
	since market data has 15-minute delay, latest 15 marketVolumes become 0 
	'''
	from _alan_str import write2mdb
	if not filter:
		filter='date,minute,open,high,low,close,changeOverTime,marketVolume'
	jdTmp = iex_batchTypes(tkLst,filter=filter,types=types,range=range,chartLast=chartLast,nchunk=nchunk,**optx)
	if len(jdTmp)<1:
		return {}
	colX = ["ticker","open","high","low","close","volume","change","changePercent","epochs","hhmm","pbdt","pbdate"]
	dLst=[]
	df=pd.DataFrame()
	clientM=None
	for ticker in tkLst:
		try:
			if ticker not in jdTmp:
				continue
			elif types not in jdTmp[ticker]:
				continue
			elif len(jdTmp[ticker][types])<1:
				continue
			jdX=jdTmp[ticker][types]
			dx=json_normalize(jdX)
			dx['ticker']=ticker
			if '-' in dx['date'].values[0]:
				dx['pbdate'] = [x.replace('-','') for x in dx['date']]
			else:
				dx['pbdate'] = dx['date']

			if 'minute' in dx:
				dformat= '%Y%m%d%H:%M'
				pbdt = [ datetime.datetime.strptime(x+y,dformat) for x,y in zip(dx['pbdate'],dx['minute']) ]
				dx['hhmm']=[x.strftime('%H%M') for x in pbdt]
				dx['epochs']=[int(x.strftime('%s000')) for x in pbdt]
			dx['pbdate']=dx['pbdate'].astype(int)
			dx = dx.dropna()
			if len(dx)<1:
				continue
			#if dx.shape[0]>1:
			#	dx['changePercent'] = dx['close'].pct_change()
			#	dx['change'] = dx['close'].diff()
			if "marketVolume" in dx:
				dx.rename(columns={"marketVolume":"volume"},inplace=True)
			if dfTF is False:
				dLst.extend(dx.to_dict(orient='records'))
				if tablename is not None and dbname is not None:
					sys.stderr.write("===Write to:{}:{}:{}\n".format(ticker,dbname,tablename))
					mobj,clientM,err_msg = write2mdb(jobj=dx,clientM=clientM,dbname=dbname,tablename=tablename,zpk=zpk,insertOnly=True)
				continue
			colX = [x for x in colX if x in dx.columns]
			dm = dx[colX]
			df= pd.concat([df,dm])
			if debugTF:
				sys.stderr.write("{}\n".format(df.tail(1)))
		except Exception as e:
			sys.stderr.write("**ERROR: {}:{}\n".format(ticker,str(e)))
			continue
	if dfTF:
		df.reset_index(drop=True,inplace=True)
		return df
	else:
		return dLst

# DEPRECATED
def func2mdb(tkLst,tablename='iex_spark_hist',dbname='ara',funcN='iex_minute_chart',zpk={'ticker','hhmm'},**optx):
	'''
	Run 'funcN'() and save the result to mongoDB
	Default to iex_quote_short()
	Note, for iex_minute_chart() use
	  tablename='iex_spark_hist',dbname='ara',funcN='iex_minute_chart',zpk={'ticker','hhmm'}	
	  also see: yh_spark_hist as supplement for data
	'''
	if funcN in globals():
		funcArg=globals()[funcN]
	else:
		return {}
	df = funcArg(tkLst,**optx)
	if len(df)<1:
		return {}

	# SAVE TO MDB
	clientM=None
	sys.stderr.write("===Write to:{}:{}:{}\n".format('MDB',dbname,tablename))
	mobj,clientM,err_msg = write2mdb(df,clientM,dbname=dbname,tablename=tablename,zpk=zpk,insertOnly=True)

	#tablename=tablename.replace('_hist','_temp')
	#zpk={'ticker'}
	#sys.stderr.write("===Write to {}:{}:{}\n".format(clientM,dbname,tablename))
	#mobj,clientM,err_msg = write2mdb(df,clientM,dbname=dbname,tablename=tablename,zpk=zpk)
	sys.stderr.write("=== finish {} ===\n".format(clientM))
	return mobj

if __name__ == '__main__':
	args = sys.argv[1:]
	if len(args)<1:
		exit(0)
	elif len(args)==1 and ',' in args[0]:
		args = args[0].split(',')
	elif len(args)==1 and '-' in args[0]:
		args = sys.stdin.read().strip().split()
	#df = iex_minute_chart(args,sandbodTF=True)
	#df = iex_quote_short(args)
	df = iex_spark_hist(args,debugTF=True)
	print(df)
