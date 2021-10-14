#!/usr/bin/python3
'''
To get company news based on ticker='AAPL'
Usage of:
python3 -c "ticker='AAPL';from rssCompany import run_rssCompany as rrc;ret=rrc(ticker);print(ret)"
'''

import feedparser
import sys
import pandas as pd
from _alan_str import find_mdb,upsert_mdb
from _alan_calc import saferun
from bson.objectid import ObjectId
import datetime
dtCvt = lambda x: str(x) if isinstance(x, (ObjectId,datetime.datetime)) else x

def adjObjectId(data):
	if not isinstance(data,list):
		return data
	for j in range(len(data)):
		if not isinstance(data[j],dict):
			continue
		for x,y in data[j].items():
			data[j][x]=dtCvt(y)
	return data


def load_rssCompany(ticker='',**optx):
	if len(ticker)<1:
		return []
	urx= 'https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US'
	url= urx.format(ticker=ticker)
	sys.stderr.write("==URL:{}\n".format(url))
	feed = feedparser.parse(url)
	rssLst=feed.entries
	if not isinstance(rssLst , list):
		return []
	for j in range(len(rssLst)):
		rssLst[j]['pubDate']=pd.Timestamp(rssLst[j]['published'])
		rssLst[j]['ticker']=ticker
		kyLst=list(rssLst[j])
		for ky in kyLst :
			if isinstance(rssLst[j][ky],dict):
				rssLst[j].pop(ky,None)

	sys.stderr.write(" --Save To DB:{}\n".format(optx))
	ret,clientM,err = upsert_mdb(rssLst,**optx)
	return ret

def renewChk(currDt,lastModDt,deltaTolerance=86400):
	deltaPassed=pd.Timedelta(currDt - lastModDt).total_seconds()
	sys.stderr.write(" --curr:{},last:{}:deltaPassed:{}\n".format(currDt,lastModDt,deltaPassed))
	return deltaPassed>deltaTolerance

@saferun
def run_rssCompany(ticker='',deltaTolerance=43200,clientM=None,dbname='ara',tablename='rssCompany',sortLst=['pubDate']):
	'''
	To get company news based on ticker='AAPL'
	where real-time data is only grab based on 'deltaTolerance' in seconds
	current setup is half-days
	'''
	clientM=None
	currDt=pd.datetime.now()
	jobj={"ticker":ticker}
	tableChk=tablename+'_chk'
	zsrt=[(k,-1) for k in sortLst] if len(sortLst)>0 else [('_id',1)]
	lastObj,clientM,_=find_mdb(jobj,clientM=clientM,dbname=dbname,tablename=tableChk,sortLst=sortLst,limit=1)
	if not lastObj:
		renewTF=True
	else:
		lastModDt=lastObj[0]['lastModDt']
		renewTF=renewChk(currDt,lastModDt,deltaTolerance)
	if renewTF:
		sys.stderr.write("==Data outdated, loading now\n")
		ret = load_rssCompany(ticker,clientM=clientM,dbname=dbname,tablename=tablename,zpk={"ticker","pubDate","link"})
		jobj.update(lastModDt=currDt)
		sys.stderr.write(" --{}\n".format(jobj))
		retChk,clientM,errChk = upsert_mdb(jobj,clientM=clientM,dbname=dbname,tablename=tableChk,zpk={"ticker"})
	else:
		sys.stderr.write("==Data exist, no loading\n")
		ret = clientM[dbname][tablename].find(jobj,sort=zsrt)
		ret = list(ret)
		#ret,clientM,err = clientM[dbname][tablename]find_mdb(jobj,clientM=clientM,dbname=dbname,tablename=tablename)
	
	return adjObjectId(ret)

def get_rss_company(ticker='AAPL'):
	ret=load_rssCompany(ticker,clientM=None,dbname='ara',tablename='rssCompany',zpk={"ticker","pubDate","link"},pkTF=True)
	return ret

if __name__ == '__main__':
	args=sys.argv[1:]
	ticker = 'MDB' if len(args)<1 else args[0]
	ret=run_rssCompany(ticker)
	print("{}".format(ret)[:300])
