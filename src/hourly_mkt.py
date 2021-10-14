#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Description: Hourly report
Usage of:
	python hourly_mkt.py 
	OR specfic date & hour and jinja2 tepmplates from 'dirname' and output to 'outdir'
	python hourly_mkt.py  --start=20190528 --extra_xs='archiveTest=True;target_hm=[1200];dirname="templates";outdir="US/mp3_hourly"' AAPL --title=蘋果 --src=iex
	OR specfic date & hour archive runs
	python hourly_mkt.py  --start=20181228 --extra_xs='archiveTest=True;target_hm=[1200,1600];dirname="templates"' SPY --title=SP500ETF --src=iex
	OR specfic source of data
	python hourly_mkt.py  --extra_xs='archiveTest=True;target_hm=[1600];dirname="templates"' ^GSPC --title=SP500指數 --src=yh
	OR
	python hourly_mkt.py  --extra_xs='archiveTest=True;target_hm=[1300];dirname="templates"' 1101 --title=台泥 --src=tw
Result: graph saved [ticker.svg] and info saved in mongoDB: ara::hourly_rpt
Note: run batch with hourly_mkt_batch.py
Last mod., Wed Mar 27 02:05:05 CST 2019
'''

import sys,os
import subprocess
import pandas as pd
import numpy as np
from _alan_date import ymd_delta,epoch_parser
from _alan_calc import subDict,find_mnmx_wmqy,chk_mnmx,pqint
from _alan_str import write2mdb, str_tofile
from csv2plotj2ts import opt_csv2plot, run_csv2plot
from datetime import datetime,timedelta
import re
import pytz
import random
import types
import pprint 
import json 
if sys.version_info.major == 2:
	reload(sys)
	sys.setdefaultencoding('utf8')
pd.set_option('display.max_colwidth', -1)
class myPP(pprint.PrettyPrinter):
	def format(self, object, context, maxlevels, level):
		if isinstance(object, unicode):
			return (object.encode('utf8'), True, False)
		return pprint.PrettyPrinter.format(self, object, context, maxlevels, level)
pp = myPP(indent=4,width=20,stream=sys.stderr)

def str2gtts(pfx,xstr,lang="cn",tempo=1.50,filetype="mp3",dirname="./"):
	glang="zh-tw" if lang=="cn" else "en"
	fname=pfx.replace("_mp3","").replace(".txt","")
	fname="{}/{}.{}".format(dirname,fname,filetype)
	if tempo==1.0:
		xcmd="/usr/local/bin/gtts-cli -l {} -f - -o {}".format(glang,fname)
	else:
		xcmd="/usr/local/bin/gtts-cli -l {} -f - | sox -G -t {} - {} tempo {}".format(glang,filetype,fname,tempo)

	pqint("===PIPE stdin:\n{}".format(xstr),file=sys.stderr)
	pqint("===PIPE SHELL:\n{}".format(xcmd),file=sys.stderr)
	p = subprocess.Popen(xcmd,shell=True,bufsize=1024,stdout=subprocess.PIPE,stdin=subprocess.PIPE)
	out, err = p.communicate(xstr.encode('utf-8'))
	#p.stdin.write(xstr)
	#p.communicate()[0]
	#p.stdin.close()
	return(out,err)

def qs_split(xstr,d1='&',d2='='):
	""" split query string into a dict object """
	d = {k:v for k,v in [lx.split(d2) for lx in xstr.split(d1)]}
	return d

def qs_exec(xstr):
	""" convert commend string into a dict object """
	exec(xstr)
	del xstr
	d = locals()
	return d

def extra_opts(opts={},xkey='extraJS',method='JS',updTF=True):
	""" 
	update values of extraKey: xkey to DICT: opts 
	based on method: [JS|QS|XS]
	where
		JS: DICT/JSON type object
		QS: k1=v1;k2=v2;... query-like string
	"""	
	import ast	
	if xkey not in opts or opts[xkey] is None: 
		d = {}
	elif method == 'JS': # additional key/val like POST arguments
		d = ast.literal_eval(opts[xkey])
	elif method == 'QS': # additional key/val like GET arguments
		d = qs_split(opts[xkey])
	else: # additional key/val like python executable string
		d = qs_exec(opts[xkey])
	if updTF is True:
		opts.update(d)
		opts[xkey] = None
	return d
	
def get_data_hist(ticker,gap='1m',ranged='1d',tsTF=True,debugTF=False,src='yh',plotTF=True):
	if any([x in ticker for x in ['^','=']]) is True or src=='yahoo' or src=='yh':
		src = 'yh'
		pqint( "Using src={},range:{},gap:{}".format(src,ranged,gap), file=sys.stderr)
		df = get_hist_yh(ticker,gap=gap,ranged=ranged,tsTF=tsTF,debugTF=debugTF,plotTF=plotTF)
	elif src=='tw':
		pqint( "Using src={}".format(src), file=sys.stderr)
		df = get_hist_tw(ticker,gap=gap,ranged=ranged,tsTF=tsTF,debugTF=debugTF)
	else: 
		pqint( "Using src={}".format(src), file=sys.stderr)
		df = get_hist_iex(ticker,gap=gap,ranged=ranged,tsTF=tsTF,debugTF=debugTF)
	return df

def get_hist_iex(ticker,gap='1m',ranged='1d',tsTF=True,debugTF=False):
	from _alan_calc import get_minute_iex,get_daily_iex
	#nytime = datetime.now(pytz.timezone('America/New_York'))
	if gap=='1d':
		ddf = get_daily_iex(ticker,ranged=ranged,tsTF=tsTF,debugTF=debugTF)
	else:
		if ',' in ranged:
			xu=ranged.split(',')
			date = xu[0] if len(xu[0])>0 else xu[1]
			ranged=None
		else:
			date = None
			ranged = '1d'
		ddf = get_minute_iex(ticker,ranged=ranged,date=date,tsTF=tsTF,debugTF=debugTF)
	return ddf

def get_spark_yh(ticker,gap='1m',ranged='1d',tsTF=True,debugTF=False):
	# TBD, missing volume and open
	from yh_chart import yh_hist_query as yhq
	ddf = yhq([ticker],types='spark',range=ranged,interval=gap)
	return ddf

def get_hist_yh(ticker,gap='1m',ranged='1d',tsTF=True,debugTF=False,plotTF=True):
	# TBD, missing volume and open
	#if gap=='1m' and plotTF is False:
	#	return get_spark_yh(ticker,gap=gap,ranged=ranged,tsTF=tsTF,debugTF=debugTF)
	from yh_hist_batch import yh_hist
	ddf = yh_hist(ticker,gap=gap,ranged=ranged,debugTF=debugTF)
	return ddf

def get_hist_tw(ticker,gap='1m',ranged='1d',tsTF=True,debugTF=False):
	from eten_hist_batch import eten_hist
	ddf = eten_hist(ticker,gap=gap,ranged=ranged)
	return ddf

def x_preopen(ticker,start=None,src='yh'):
	ddf = get_data_hist(ticker,gap='1d',ranged='5d',src=src)
	if start is None:
		start = datetime.now().strftime("%Y%m%d")
	if '-' in start:
		start=start.replace('-','') 
	ddf.drop(ddf[ddf['pbdate']>=int(start)].index,inplace=True)
	rpt_date = start
	x_date = "{}".format(ddf['pbdate'].iloc[-1])
	ranged = "{},".format(x_date)
	return (ranged, rpt_date)

def run_hourly(tkLst=[],end_hm=1600,ddfRaw=None,**optx):
	opts = optx.copy()
	debugTF = opts['debugTF'] if 'debugTF' in opts else True
	src = opts['src'] if 'src' in opts else 'iex'
	lang = opts['lang'] if 'lang' in opts else 'cn'
	curr_hm = int(datetime.now().strftime("%H%M"))
	archiveTest = opts['archiveTest'] if 'archiveTest' in opts else False
	if (curr_hm < end_hm) and archiveTest is False:
		pqint( "{0}<{1}, no {1} run".format(curr_hm,end_hm) , file=sys.stderr)
		return None, None, ddfRaw
	if src in ['tw']:
		fphm='mapping_hourly_hm_{}.dat'.format(src)
	else:
		fphm='mapping_hourly_hm.dat'
	hmMap = pd.read_csv(fphm,sep="\t")
	try:
		hmX = hmMap.query("cutoff_hm=={}".format(end_hm)).iloc[-1].to_dict()
		rpt_status = hmX['rpt_status']
	except Exception as e:
		return None
	pqint( "===Running:{} using {}".format(end_hm,hmX), file=sys.stderr)
	# get minute data
	if 'start' in opts and opts['start'] is not None:
		ranged = "{},".format(opts['start'])
	elif 'end' in opts and opts['end'] is not None:
		ranged = ",{}".format(opts['end'])
	else:
		ranged = '1d'

	if isinstance(tkLst,pd.DataFrame) is True:
		mndfRaw = tkLst
		if 'ticker' in mndfRaw:
			ticker = mndfRaw['ticker'].iloc[0]
		elif 'name' in mndfRaw:
			ticker = mndfRaw['name'].iloc[0]
		elif 'symbol' in mndfRaw:
			ticker = mndfRaw['symbol'].iloc[0]
		else:
			ticker = '^GSPC'
	else:
		ticker = tkLst[0] if len(tkLst)>0 else '^GSPC'
		start = opts['start'] if 'start' in opts else None
		if hmX['rpt_status']=='preopen':
			ranged,rpt_date = x_preopen(ticker,start=start,src=src)
			pqint( "ranged:[{}],rpt_date:{} in x_preopen()".format(ranged,rpt_date), file=sys.stderr)
			if 'rpt_date' in opts:
				rpt_date = opts['rpt_date']
			elif rpt_date is None:
				pqint( "**ERROR: preopen rpt_date undefined", file=sys.stderr)
				rpt_date = datetime.now().strftime('%Y%m%d')
		plotTF = opts['plotTF'] if 'plotTF' in opts else True
		mndfRaw = get_data_hist(ticker,gap='1m',ranged=ranged,src=src,debugTF=debugTF,plotTF=plotTF)
	latestRaw_time = epoch_parser(mndfRaw['epochs'].iloc[-1])
	mndf = mndfRaw.copy()
	if rpt_status in ['opening','trading']: # only using applicable subset-data corresponding to end_hm
		xdu = "{}{:04d}".format(latestRaw_time.strftime('%Y%m%d'),end_hm)
		cutoff_epoch = datetime.strptime(xdu,'%Y%m%d%H%M').strftime("%s000")
		mndf = mndf.query("epochs<={}".format(cutoff_epoch))
	if rpt_status in ['preopen','trading']: # create a 30-minute forecast
		from _alan_ohlc_fcs import run_ohlc_fcs
		fcsChg = 0.0
		try:
			(dd,dwm,_) = run_ohlc_fcs(mndf)
			#if len(dwm)<1:
			#	fcsChg = 0
			#	pass
			#fcsChg = float(dwm['prc_fcs'].values[-1])/float(dwm['prc_cur'].values[-1]) - 1
			#pqint( "Forecast Matrix (dwm):", file=sys.stderr)
			pqint( dwm, file=sys.stderr)
		except Exception as e:
			pqint( "**ERROR: {} @{}".format(str(e),"run_ohlc_fcs"), file=sys.stderr)
			pqint( "===mndf:\n{}".format(mndf.to_string()), file=sys.stderr)
			fcsChg = 0
	else:
		fcsChg = 0

	pqint( "30-minute Forecast Chg%: {}".format(fcsChg), file=sys.stderr)
	pqint( "Minute Data:", file=sys.stderr)
	pqint( mndf.head(2).to_csv(), file=sys.stderr)
	pqint( mndf.tail(2).to_csv(header=False), file=sys.stderr)
	latestUpd_time = epoch_parser(mndf['epochs'].iloc[-1]) # latest data received date/time
	latestUpd_hm = int(latestUpd_time.strftime('%H%M')) # latest data received time in HHMM
	if hmX['rpt_status']!='preopen':
		rpt_date = int(latestUpd_time.strftime('%Y%m%d')) # latest data received date in YYYYMMDD
	rpt_time = datetime.strptime("{}{:04d}".format(rpt_date,hmX['rpt_hm']), '%Y%m%d%H%M') # report datetime

	# get daily data
	end_date = int(latestUpd_time.strftime('%Y%m%d'))
	#start_date = ymd_delta(end_date,366)
	#ranged='{},{}'.format(start_date,end_date)
	ranged='1y'
	if ddfRaw is None:
		ddfRaw = get_data_hist(ticker,gap='1d',ranged=ranged,src=src)
	ddf = ddfRaw.copy()
	# sync daily data up to minute data date, delete additional
	# leave the last closing date 'end_date' for same-day closing price
	ddf.drop(ddf[ddf['pbdate']>end_date].index,inplace=True) 
	if debugTF is True:
		pqint( "Daily Data: <= end_date:{}".format(end_date), file=sys.stderr)
		pqint( ddf.head(2).to_csv(sep="|"), file=sys.stderr)
		pqint( ddf.tail(2).to_csv(sep="|",header=False), file=sys.stderr)

	# pick correspondent report template, 
	# hm: last HourMinute data required w.r.t template 
	if 'j2name' in opts and opts['j2name'] is not None: # for preopen case
		hmX['j2name'] = opts['j2name']
	fj2name = "{}_{}.j2".format(hmX['j2name'],lang)
	#rpt_hm = "{}".format(hmX['rpt_hm'])
	if debugTF is True:
		pqint( "hmX: {}".format(hmX), file=sys.stderr)
	npar = hmX['npar']
	j2ts = "{} include '{}' {}".format('{%',fj2name,'%}')
	pqint( "j2name:{}, j2ts:{}, hmX:{}".format(fj2name,j2ts,hmX), file=sys.stderr)
	pngname='{}_{}_{}.{}'.format(re.sub('[\^= ]','',ticker),rpt_date,end_hm,'svg')
	if opts['title'] is None:
		title="標普500"
	else:
		title=opts['title']

	#label=title.encode('utf8') if 'label' not in opts else opts['label']
	label=title if 'label' not in opts else opts['label']

	# add/update variables for jinjia2
	optx = hmX
	optx.update(npar=npar,pngname=pngname,j2ts=j2ts,title=title,label=label,ticker=ticker,
		fcsChg=fcsChg,rpt_time=rpt_time,rpt_date=rpt_date)
	if rpt_status in ['trading','closing']: #use different graphs
		optx.update({'ohlcComboTF':True})
	else:
		optx.update({'ohlcTF':True})
	opts.update(optx)
	mndf, opts, jobj =  process_hourly_comment(mndf, ddf, opts=opts)

	# save to mongodb
	#opts['mndf']=mndf.to_dict(orient='records')
	tpLst = (types.BuiltinFunctionType,types.FunctionType,pd.DataFrame)
	kobj = { k:v for (k,v) in opts.items() if isinstance(v,tpLst) is False }
	if len(jobj)>0:
		kobj['headline']=jobj['headline']
		kobj['comment']=jobj['comment']
		kobj['mp3comment']=jobj['mp3comment']
		kobj['fcsTrend']=jobj['fcsTrend']
	clientM=None
	if opts['src'] in ['tw','hk','cn']:
		mdbname='ara_{}'.format(opts['src'])
	else:
		mdbname='ara'
	#mobj,clientM,err_msg = write2mdb(kobj,clientM,dbname=mdbname,tablename='hourly_rpt',zpk={'ticker','rpt_time'})
	#pqint( "Status @ write2mdb:",err_msg, file=sys.stderr)
	#if debugTF is True:
	#	sys.stderr.write("===mobj:\n{}\n".format(mobj))
	if hmX['rpt_status']=='preopen':
		return jobj, tkLst, ddfRaw
	return jobj, mndfRaw, ddfRaw

def process_hourly_comment(mndf, ddf, opts={}):
	debugTF = opts['debugTF'] if 'debugTF' in opts else True
	end_date = epoch_parser(mndf['epochs'].iloc[-1]).strftime('%Y%m%d')
	mnmxStats = find_mnmx_wmqy(ddf)
	if debugTF is True:
		pqint( mnmxStats, file=sys.stderr)
	if int(end_date)==ddf['pbdate'].iloc[-1]:
		xprice = ddf['close'].iloc[-2]
		cprice = ddf['close'].iloc[-1]
		cvol = ddf['volume'].iloc[-1]
	else:
		xprice = ddf['close'].iloc[-1]
		cprice = mndf['close'].iloc[-1]
		cvol = 0
	if cvol<=0 and 'volume' in mndf:
		cvol = mndf['volume'].sum()
	if 'volume' in ddf:
		xvols = ddf['volume'].iloc[-20:]
		xmn, xsd = xvols.mean(), xvols.std()
	else:
		xmn, xsd = 0,0
	cpchg = cprice/xprice-1 if abs(xprice)>0 else -999999 
	if cpchg > -999999:
		largest_move,_,_,_ = chk_mnmx(cpchg,mnmxStats)
	else:
		largest_move=0
	sys.stderr.write( "Change%:{}, Largest Move: {}\n".format(cpchg*100,largest_move))
	sys.stderr.write( "mnmxStats:\n{}\n".format(mnmxStats))

	if debugTF is True:
		sys.stderr.write("===mndf:\n{}\n".format( mndf.head().to_csv()))
		sys.stderr.write("{}\n".format( mndf.tail().to_csv(header=False)))
		sys.stderr.write("xprice:{}|cprice:{}|xmn:{}|xsd:{}\n".format(xprice,cprice,xmn,xsd))
	volstats='{},{}'.format(xmn, xsd)
	# add/update variables for jinjia2
	optx = dict(cpchg=cpchg,xprice=xprice,cprice=cprice,cvol=cvol,volstats=volstats,largest_move=largest_move)
	optx.update({'trendTF':True,'xaxis':'epochs','columns':'ticker,close,open,high,low,volume,epochs'})
	opts.update(optx)
	opts['mnmxStats'] = mnmxStats.to_dict(orient='records')

	(mndf, opts, jobj) = run_csv2plot(mndf,opts=opts)

	# write txt and mp3 files
	ret = jobj['comment'] if 'comment' in jobj else ''
	outdir = opts['outdir'] if 'outdir' in opts else './'
	if not os.path.exists(outdir):
		outdir='./'

	if len(ret)>10:
		pfx="hourly_{}_{}_{}".format(opts['ticker'],opts['rpt_date'],opts['cutoff_hm'])
		fname="{}/{}.{}".format(outdir,pfx,'txt')
		str_tofile(fname,ret)

	# create mp3 file if text more than 10 characters and server not in China
	mp3ret = jobj['mp3comment'] if 'mp3comment' in jobj else ''
	if len(mp3ret)>10 and 'Shanghai' not in open('/etc/timezone').read() and ('mp3TF' not in opts or opts['mp3TF'] is True):
		str2gtts(pfx,mp3ret,dirname=outdir)
		pqint( "Create MP3 file:{}".format(pfx), file=sys.stderr)

	return (mndf, opts, jobj)


def wrap_hourly(tkLst=[],end_hm=1600,ddfRaw=None,**optx):
	opts, _ = opt_csv2plot([])
	opts.update(archiveTest=True,mp3TF=False,plotTF=False,outTF=True)
	if len(optx)>0:
		opts.update(optx)
	jobj, mndfRaw, ddfRaw = run_hourly(tkLst,end_hm,ddfRaw,**opts)
	return jobj

if __name__ == '__main__':
	opts, args = opt_csv2plot(sys.argv)
	random.seed(int(datetime.today().strftime("%m%d")))
	if opts['src'] in ['tw']:
		endLst=[800,1000,1100,1200,1300,1330]
	else:
		endLst=[900,1000,1100,1200,1300,1400,1500,1600]
	#try:
	#	extra_opts(opts,xkey='extraJS',method='JS',updTF=True)
	#	extra_opts(opts,xkey='extraXS',method='XS',updTF=True)
	#except Exception as e:
	#	pqint( str(e), file=sys.stderr)
	if 'target_hm' in opts:
		endLst = opts['target_hm']
	ddfRaw = None
	mndfRaw = args
	for j,end_hm in enumerate(endLst):
		jobj, mndfRaw, ddfRaw = run_hourly(mndfRaw,end_hm,ddfRaw,**opts)
		if jobj is None or len(jobj)<1:
			break
		if 'comment' in jobj and 'outTF' in opts and opts['outTF'] is True:
			pqint(jobj['comment'], file=sys.stderr)
