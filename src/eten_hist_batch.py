#!/usr/bin/env python
'''
Get daily/minute data via eten api
e.g.,
TZ=Asia/Taipei eten_hist_batch.py 2301 --output=csv
OR (To manually assign pubish date YYYYMMDD)
TZ=Asia/Taipei eten_hist_batch.py 2301 --output=csv --pbdate=20190322
'''
#'^GSPC','^TWII','000001.SS','^SOX','^DJI'
import sys
from optparse import OptionParser
import pandas as pd
import requests
import numpy as np
from datetime import datetime,timedelta
import pytz
from _alan_calc import upd_temp2hist,sqlQuery


def epoch_parser(x,s=1000): return datetime.fromtimestamp(int(x/s))
def ymd_parser(x,fmt='%Y%m%d'): return datetime.strptime(str(x),fmt)

def subDict(myDict,kyLst,reverseTF=False):
	"""
	return a sub-dict of myDict based on the matching [kyLst] keys
	OR
	sub-dict based on the non-matching [kyLst] keys
	"""
	if reverseTF is True: # invert-match, select non-matching [kyLst] keys
		return { ky:myDict[ky] for ky in myDict.keys() if ky not in kyLst }
	else:
		return { ky:myDict[ky] for ky in myDict.keys() if ky in kyLst }

def eten_hist(ticker=None,gap='1m',ranged='1d',pbdate=None,hdrTF=True,tsTF=True,debugTF=False):
	if gap == '1d':
	 	df = eten_daily(ticker=ticker,ranged=ranged,hdrTF=True,tsTF=True,debugTF=False)
		if len(df)<1:
			df = sqlQuery("SELECT DISTINCT * FROM prc_hist WHERE name='{}' ORDER BY pbdate".format(ticker),dbname='ara.tw')
		#from _alan_calc import sqlQuery
		#xqr="select * from prc_hist where name='{}' and pbdate>20171101".format(ticker)
		#df = sqlQuery(xqr,dbname='ara.tw')
		return df
	twtime = datetime.now(pytz.timezone('Asia/Taipei'))
	if pbdate is not None:
		pass
	elif  twtime.hour <= 8:
		pbdate = (twtime - timedelta(days=1)).strftime('%Y%m%d')
	else:
		pbdate = (twtime).strftime('%Y%m%d')
	return eten_minute(ticker=ticker,pbdate=pbdate,hdrTF=hdrTF,tsTF=tsTF,debugTF=debugTF)

def eten_daily(ticker=None,ranged='1y',hdrTF=True,tsTF=True,debugTF=False):
	"""
	Get daily/minute data via eten api
	"""
	if ticker is None:
		return ''
	#urx="http://mx68t.etencorp.com:8080/EtenDS/process.php?version=1&objtype=6&extcode={}"
	urx="http://192.168.243.5:8085/EtenDS/process.php?version=1&objtype=6&extcode={}"
	url=urx.format(ticker)
	if debugTF is True:
		sys.stderr.write(url+"\n")
	try:
		ret=requests.get(url,timeout=3)
                jTmp=ret.json()['objectData'][0]
		#jTmp = pd.read_json(url)['objectData'][0]
	except Exception as e:
		sys.stderr.write("**ERROR: {} of URL[{}]\n".format(str(e),url) )
		return {}
	
	#- ARRANGE input data	
	# build output data in datafame
	rng ={'5d':5,'10d':10,'1w':5,'1m':22,'3m':66,'1y':252,'2y':505,'3y':757}
	if ranged.lower() in rng:
		nobs = rng[ranged]
	else:
		nobs = None
	dx=pd.DataFrame(jTmp['data'])

	dx.loc[:,'ticker']=jTmp['extcode']
	if nobs is not None:
		dx=dx.iloc[-nobs:].reset_index(drop=True)

	if debugTF is True:
		sys.stderr.write("{}\n".format(dx))
	# build date/time as pbdate/epochs column into datafame
	pbdatetime = [datetime.strptime(x,'%Y-%m-%d') for x in dx['Date'].values]
	dx['pbdate'] = [int(x.strftime('%Y%m%d')) for x in pbdatetime]
	dx[['open', 'high', 'low', 'close', 'vol']]=dx[['open', 'high', 'low', 'close', 'vol']].astype('float')
	# add datetime index to datafame
	if tsTF is True:
		dx.set_index(pd.DatetimeIndex(pbdatetime),inplace=True)
		dx.index.rename('date',inplace=True)
	# remove NA rows related to [close] data
	dx.rename(columns={'vol':'volume'},inplace=True)
	dx.rename(columns={'ticker':'name'},inplace=True)
	dx.dropna(subset=['close'],inplace=True)
	dx['adjusted'] = dx['close']
	# change to prc_temp columns setup
	colLst = ['open','high','low','close','volume','adjusted','pbdate','name']
	datax =  dx[colLst]
	return datax

def eten_minute(ticker=None,pbdate=20181120,hdrTF=True,tsTF=True,debugTF=False):
	"""
	Get daily/minute data via eten api
	"""
	if ticker is None:
		return ''
	#urx="http://mx68t.etencorp.com:8080/EtenDS/process.php?version=1&objtype=5&extcode={}"
	urx="http://192.168.243.5:8085/EtenDS/process.php?version=1&objtype=5&extcode={}"
	url=urx.format(ticker)
	if debugTF is True:
		sys.stderr.write(url+"\n")
	try:
		jTmp = pd.read_json(url)['objectData'][0]
	except Exception as e:
		sys.stderr.write("**ERROR: {} of URL[{}]\n".format(str(e),url) )
		return {}
	if len(jTmp)<1:
		return {}
	
	#- ARRANGE input data	
	# build output data in datafame
	dx=pd.DataFrame(jTmp['data'])
	dx.loc[:,'ticker']=jTmp['extcode']
	# build date/time as pbdate/epochs column into datafame
	if 'epoch' in dx.columns:
		dx['epoch'] = dx['epoch'].astype(int)
		pbdatetime = [datetime.fromtimestamp(x) for x in dx['epoch'].values]
		dx['epochs'] = dx['epoch']*1000
	else:
		pbdatetime = [datetime.strptime(str(pbdate)+x,'%Y%m%d%H:%M') for x in dx['time'].values]
		dx['epochs'] = [int(x.strftime('%s000')) for x in pbdatetime]
	dx[['open', 'high', 'low', 'close', 'vol']]=dx[['open', 'high', 'low', 'close', 'vol']].astype('float')
	# add datetime index to datafame
	if tsTF is True:
		dx.set_index(pd.DatetimeIndex(pbdatetime),inplace=True)
		dx.index.rename('date',inplace=True)
	# remove NA rows related to [close] data
	dx.rename(columns={'vol':'volume'},inplace=True)
	dx.dropna(subset=['close'],inplace=True)
	# change to prc_temp columns setup
	colLst = ['open','high','low','close','volume','epochs','ticker']
	datax =  dx[colLst]
	return datax

def mainTst(tkLst=[],opts=None,optx=None):
	#- Set input parameters
	if opts is None or len(opts)<1:
		opts, _ = opt_csv2plot([])
	if optx is not None:
		opts.update(optx)
	kys=['gap','ranged','tsTF','pbdate','debugTF']

	for ky,va in opts.items():
		exec('{}=va'.format(ky))

	for j,ticker in enumerate(tkLst):
		hdrTF = True if j<1 else False
		df = eten_hist(ticker,hdrTF=hdrTF,**subDict(opts,kys))
		if len(df)<1:
			sys.stderr.write("**ERROR: No data for {}\n".format(ticker) )
			continue
		if output == 'csv':
			sep = sep.encode().decode('unicode_escape') if sys.version_info.major==3 else sep.decode("string_escape")
			sys.stdout.write(df.to_csv(sep=sep,index=indexTF) )
		elif output == 'html':
			sys.stdout.write(df.to_html(index=indexTF) )
		elif output == 'json':
			sys.stdout.write(df.to_json(orient='records') )
		if saveDB and gap=='1m':
			xqr,pgDB = upd_temp2hist(temp='minute_temp',hist='minute_hist',pcol=['ticker','epochs'],dbname='ara.tw',hostname='localhost',df=df)

def opt_eten_hist(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:   list arguments, usually passed from sys.argv
		retParser:      OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class
	"""
	parser = OptionParser(usage="usage: %prog [option] SYMBOL1 ...", version="%prog 0.01",
		description="Pull Price History from YAHOO")
	parser.add_option("","--range",action="store",dest="ranged",default='1d',
		help="range period from now (default: 1d)")
	parser.add_option("","--gap",action="store",dest="gap",default='1m',
		help="interval GAP of data frequency (default: 1m)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara",
		help="database (default: ara)")
	parser.add_option("","--pbdate",action="store",dest="pbdate",
		help="publish date in YYYYMMDD")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="tablename",default="prc_temp_yh",
		help="db tablename (default: prc_temp_yh)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table write-mode [replace|append] (default: replace)")
	parser.add_option("-o","--output",action="store",dest="output",
		help="OUTPUT type [csv|html|json] (default: no output)")
	parser.add_option("","--no_datetimeindex",action="store_false",dest="tsTF",default=True,
		help="no datetime index (default: use datetime)")
	parser.add_option("","--debug",action="store_true",dest="debugTF",default=False,
		help="verbose (default: False)")
	parser.add_option("","--show_index",action="store_true",dest="indexTF",default=False,
		help="show index (default: False) Note, OUTPUT ONLY")
	parser.add_option("","--database_save",action="store_true",dest="savdDB",default=False,
		help="Save to database, default: NO")
	parser.add_option("-s","--sep",action="store",dest="sep",default="|",
		help="output field separator (default: |) Note, OUTPUT ONLY")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no save to database (default: save to database)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	(opts, tkLst)=opt_eten_hist(sys.argv)
	if len(tkLst)==1 and tkLst[0]=='-':
		tkLst = sys.stdin.read().split()
	mainTst(tkLst,opts)
