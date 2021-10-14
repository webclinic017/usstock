#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
for EoD batch of a list of stocks
python hourly_mkt_batch.py {region} {cutoff_hm} {start} {lang} {outdir} {dirname}
example:
1. TZ=America/New_York python hourly_mkt_batch.py US
OR for Intra-day market run
2. python hourly_mkt_batch.py intraday_briefing
OR for EoD market run
3. python hourly_mkt_batch.py daily_briefing --start=20190731 --lang=cn
e.g.,
TZ=America/New_York python hourly_mkt_batch.py US
OR
TZ=Asia/Taipei python hourly_mkt_batch.py TW

Last Mod., Fri Aug  2 14:35:34 EDT 2019
'''
import sys,os
import pandas as pd
from _alan_str import sysCall,jj_fmt
from _alan_calc import sqlQuery,conn2pgdb
import datetime
import numpy as np
if sys.version_info.major == 2:
	reload(sys)
	sys.setdefaultencoding('utf8')

def get_tkLst(tkLst=[],xqr="select etfname from spdr_sector",pgDB=None):
	xLst = [str(x) for x in sqlQuery(xqr,pgDB).iloc[:,0].values]
	if len(tkLst)<1:
		return xLst
	else:
		return np.append(tkLst,xLst)

def get_stocks_us(pgDB=None):
	tkLst=[]
	xqr = "select ticker from  ara_ranking_list where category='AI' and subtype='SP500' order by ranking limit 10"
	tkLst = get_tkLst(tkLst,xqr=xqr,pgDB=pgDB)
	xqr = "select ticker from  ara_ranking_list where category='VOLUME' and subtype='SP500' order by ranking limit 10"
	tkLst = get_tkLst(tkLst,xqr=xqr,pgDB=pgDB)
	xqr = "select etfname from spdr_sector"
	tkLst = get_tkLst(tkLst,xqr=xqr,pgDB=pgDB)
	tkLst = np.append(tkLst,['^GSPC','^DJI','^IXIC','^SOX','^TWII','000001.SS'])
	return tkLst

def get_stocks_tw(pgDB=None):
	tkLst = ['000','001','050','1101','1102','1216','1301','1303','1326','1402','2002','2105','2301','2303','2308','2317','2327','2330','2354','2357','2382','2395','2408','2409','2412','2454','2474','2492','2633','2801','2823','2880','2881','2882','2883','2884','2885','2886','2887','2890','2891','2892','2912','3008','3045','3481','3711','4904','4938','5871','5880','6505','9904']
	return tkLst

def run_hourly_mkt(ticker,title,dirname="templates",outdir="US/mp3_hourly/",start=None,archiveTest=False,target_hm='1600',pgDB=None,region='us',lang='cn',rpt_date=None):
	src='yh' if region=='us' else region
	extra_xs='archiveTest={};dirname=\"{}\";outdir=\"{}\";rpt_date={};plotTF=False'.format(archiveTest,dirname,outdir,rpt_date)
	if len(target_hm)>2:
		extra_xs='{};target_hm=[{}]'.format(extra_xs,target_hm)
	d = dict(start=start,title=title,ticker=ticker,lang=lang,src=src,extra_xs=extra_xs)
	xcmd="python hourly_mkt.py {ticker} --start={start} --extra_xs='{extra_xs}' --title='{title}' --lang={lang} --src={src}".format(**d)
	if ticker in ['^TWII','000001.SS']:
		xcmd = 'TZ=Asia/Taipei ' + xcmd
	sys.stderr.write(xcmd+"\n")
	sysCall(xcmd)

def get_cutoff_hm(hm=1600,region='us'):
	if hm is None or hm<=0:
		hm = datetime.datetime.now().hour*100
	if region.lower()=='tw':
		if hm<800:
			sys.stderr.write("{} too early!\n".format(currTime))
			return -1
		elif currTime.minute>=30 and hm==1300:
			hm=1330
		cutoff_hm=str(np.clip(hm,800,1330))
	else:
		if hm<900:
			return -1
		cutoff_hm=np.clip(hm,900,1600)
	return cutoff_hm

from _alan_str import write2mdb
def daily_briefing(start=None,region='US',dirname='templates/',outdir="US/mp3_hourly/",dbname='ara',saveDB=True,**optx):
	from headline_writer import generate_headline
	if 'cdt' not in optx:
		cdt=datetime.datetime.now()
	else:
		cdt = optx['cdt']
	if isinstance(cdt,str):
		cdt = pd.Timestamp(cdt)
	if start is None: 
		start = cdt.strftime('%Y%m%d')
	sys.stderr(" --cdt:{}, start:{}\n".format(cdt, start))
	opts={'lang': 'cn', 'dirname': 'templates', 'end': None, 'nlookback': 1, 'args': [], 'sep': '|', 'debugTF': False, 'hostname': 'localhost', 'tablename': None, 'days': 730, 'saveDB': True, 'extraJS': None, 'j2ts': '{% include "daily_briefing_cn.j2" %}', 'onTheFly': True, 'output': None, 'narg': 0, 'filename': None, 'extraQS': None, 'dbname': 'ara', 'mp3YN':False}
	del optx['args']
	hm=int(cdt.strftime("%H00")) # NOTE: sensative to crontab timing issue
	hm = get_cutoff_hm(hm=int(hm),region=region)
	if hm<1600:
		start = sqlQuery("select pbdate from prc_hist where name='AAPL' ORDER BY pbdate DESC limit 1").iloc[0].values[0]
		start = int(start)
		category='SoD'
	else:
		category='EoD'
	if not os.path.exists(outdir):
		outdir= './'
	#ret=generate_headline(opts,start=start,outdir=outdir,category=category,rpt_time=cdt,**optx)
	ret=jj_fmt(opts['j2ts'],dirname=dirname,start=start,outdir=outdir,rpt_time=cdt,category=category)
	
	if 'tablename' not in locals() or tablename is None:
		tablename='mkt_briefing'

	title='{}_briefing'.format(category)
	dd = dict(comment=ret,pbdt=cdt,title=title,hhmm=hm,category=category,rpt_time=cdt)
	if saveDB:
		clientM=None
		mobj, clientM, _ = write2mdb(dd,clientM,dbname=dbname,tablename=tablename,zpk=['hhmm','category'])
		tablename= tablename+'_hist'
		mobj, clientM, _ = write2mdb(dd,clientM,dbname=dbname,tablename=tablename,zpk=['pbdt','category'])
	return ret

from _alan_str import write2mdb
def intraday_briefing(args=[],region='US',lang='cn',dirname='templates/',outdir="US/mp3_hourly/",dbname='ara',start=None,mp3YN=False,archiveTest=False,saveDB=True,**optx):
	if 'tablename' not in locals() or tablename is None:
		tablename='mkt_briefing'
	from headline_calc import headline_calc
	if 'cdt' not in optx:
		cdt=datetime.datetime.now()
	else:
		cdt = optx['cdt']
	if isinstance(cdt,str):
		cdt = pd.Timestamp(cdt)
	hm=int(cdt.strftime("%H00"))
	end_hm=np.clip(hm,900,1600)
	end_hm = get_cutoff_hm(hm=int(hm),region=region)
	dd=headline_calc(eqtLst=None,np=3)
	ts = "{% include 'intraday_briefing.j2' %}"
	ret = jj_fmt(ts,dd=dd,dirname=dirname,outdir=outdir,end_hm=end_hm,**optx) 
	title='intraday_briefing'
	category='IntraDay'
	dd = dict(comment=ret,pbdt=cdt,title=title,hhmm=hm,category=category)
	if saveDB:
		clientM=None
		mobj, clientM, _ = write2mdb(dd,clientM,dbname=dbname,tablename=tablename,zpk=['hhmm','category'])
		#tablename= tablename+'_hist'
		#mobj, clientM, _ = write2mdb(dd,clientM,dbname=dbname,tablename=tablename,zpk=['pbdt','category'])
	return ret

def hourly_mkt_batch(args=[],tkLst=[],region='US',start=None,lang='cn',dirname='templates/', dbname='ara', archiveTest=False, **optx):
	''' setup a batch run
	'''
	if not args:
		args=sys.argv[1:]
	currTime=datetime.datetime.now()
	region = args[0] if len(args)>0 else region
	hm = args[1] if len(args)>1 else currTime.strftime('%H00')
	start = int(args[2]) if len(args)>2 else int(currTime.strftime('%Y%m%d'))
	lang = args[3] if len(args)>3 else lang
	outdir = args[4] if len(args)>4 else "{}/mp3_hourly".format(region.upper())
	dirname = args[5] if len(args)>5 else dirname
	rpt_date=currTime.strftime('%Y%m%d')

	if start<int(currTime.strftime('%Y%m%d')):
		archiveTest=True
	else:
		archiveTest=archiveTest
	cutoff_hm = str( get_cutoff_hm(hm=int(hm),region=region))
	ret = loop_hourly_mkt(tkLst=tkLst,start=start,dbname=dbname,cutoff_hm=cutoff_hm,region=region,dirname=dirname,outdir=outdir,archiveTest=archiveTest,rpt_date=rpt_date,**optx) 
	return ret

def loop_hourly_mkt(tkLst=[],lang='cn',dbname='ara',cutoff_hm='1600',rpt_date='20190731',region='us',start=20190731,archiveTest=False,dirname='templates/',outdir='US/mp3_hourly',**optx):
	''' run a list of tkLst
	'''
	region=region.lower()
	dbname='{}{}'.format(dbname,'.'+region if region=='tw' else '')
	pgDB=conn2pgdb(dbname=dbname)
	if not tkLst and region=='tw':
		tkLst = get_stocks_tw(pgDB)
	elif not tkLst:
		tkLst = get_stocks_us(pgDB)
	xqTmp = "select ticker,company{} as label from mapping_ticker_cik where ticker in ('{}')"
	xqr = xqTmp.format('_cn' if lang=='cn' else '', "','".join(tkLst))
	tkLbLst = sqlQuery(xqr,pgDB).to_dict(orient='records')
	for v in tkLbLst:
		try:
			ticker,title = v['ticker'],v['label']
			run_hourly_mkt(ticker,title,start=start,pgDB=pgDB,target_hm=cutoff_hm,region=region,dirname=dirname,outdir=outdir,archiveTest=archiveTest,rpt_date=rpt_date) 
		except Exception as e:
			sys.stderr.write("**ERROR:{ticker}:{label}:{err}\n".format(err=str(e),**v))
			continue
	#from  mongo2psql import mongo2psql
	#mongo2psql('hourly_report',dbname,engine=pgDB)
	fp=open(outdir+'/hourly_mp3_uptodate.txt','w')
	fp.write("{}_{}\n".format(start,cutoff_hm))
	fp.close()
	return 0

if __name__ == '__main__':
	from _alan_optparse import parse_opt
	description="RUN hourly_mkt_batch.py {region} {cutoff_hm} {start} {lang} {outdir} {dirname}"
	opts, args = parse_opt(sys.argv, description=description)
	funcArg = hourly_mkt_batch
	if len(args)>0 and len(args[0])>5:
		funcName = args[0]
		if funcName in globals():
			funcArg = globals()[funcName]
		if funcArg == hourly_mkt_batch:
			opts['args'][0] = 'US'
	ret = funcArg(**opts)
	print(ret)
