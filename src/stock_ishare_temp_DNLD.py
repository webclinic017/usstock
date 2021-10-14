#!/usr/bin/env python
#- Download ishare etf fund [ivv|iwv|iwm|itot] and save data into [stock_ishare_temp]
#- then update to eSTAR_2:[stock_ishare_hist] respectively
#- Usage Of:
#-	./stock_ishare_temp_DNLD [etfName]
#-	OR to access local file: ./[etfName].[yyyymmdd].xls
#-	./stock_ishare_temp_DNLD [etfName] [yyyymmdd] 
#- Example:
#-	./stock_ishare_temp_DNLD ivv	
#- Note:
#-   - reference sites,
#-     https://www.ishares.com/us/239724/fund-download.dl' # SP core return: itot
#-     https://www.ishares.com/us/239726/fund-download.dl' # SP500: ivv
#-     https://www.ishares.com/us/239714/fund-download.dl' # Russell 3000: iwv
#-     https://www.ishares.com/us/239710/fund-download.dl' # Russell 2000: iwm
#-
#- fix xml bug via using lxml-xml and change ss: tags to be case sensitive
#- Last mod.  Sat Oct  7 10:09:18 EDT 2017
#----------------------------------------------------------------------------------------#

import sys
import urllib2
import re
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
#from subprocess import Popen,STDOUT,PIPE
#from glob import glob
#import os,time

#'Parse ishare xls format data [xldata] into a row-by-row of dataset [r]
def parse_ishare_xls(xldata,etfName):
	soup = BeautifulSoup(xldata,'lxml-xml')
	jRows=soup.find("ss:Worksheet").find_all('ss:Row')
	pbdt=str(jRows[3].find_all("ss:Data",{"ss:Type":"String"})[1].next)
	pbdate=(datetime.strptime(pbdt , '%b %d, %Y').strftime('%Y%m%d'))
	#pbdate=(datetime.strptime(pbdt , '%d-%b-%y').strftime('%Y%m%d'))
	
	hdr=[str(x.next) for j,x in enumerate(jRows[8].find_all("ss:Data",{"ss:Type":"String"}))]
	pLst=[0,1,3,4,8,10,11]
	jProd=[hdr[k] for k in pLst ]
	r=[]
	for j,jR in enumerate(jRows[9:] ) :
		jInfo=[str(x.next) for x in jR.find_all("ss:Data")]
		assetType=jInfo[2]
		if(assetType == 'Equity' and jInfo[0]!='--') :
			jInfo[-1]=re.sub('New York Stock Exchange Inc.','NYSE',jInfo[-1])
			jInfo[-1]=re.sub('Nyse Mkt Llc','NYSEMKT',jInfo[-1])
			jInfo[-1]=re.sub('Non-Nms Quotation Service \(Nnqs\)','NNQS',jInfo[-1])
			jProd=[jInfo[k] for k in pLst ]
			#sys.stderr.write("%s\t" % jInfo[k])
			jProd.append(etfName)
			jProd.append(pbdate)
			r.append(jProd)
		
	return (pbdate,r)

def updateTemp2Hist(currPg,tbName,etfName,pbDate):
        xqr="DELETE FROM stock_ishare_hist WHERE etfname='"+etfName+"' AND pbdate="+pbDate
	xqr=xqr+";INSERT INTO stock_ishare_hist SELECT * FROM stock_ishare_temp WHERE etfname='"+etfName+"' AND pbdate="+pbDate
        currPg.execute(xqr)
        connPg.commit()

#'Write row-by-row of dataset [df] to Postgresql ara::[tbName]
def writeXls2Pgdb(df,tbName,etfName,pbDate,dbname='ara'):
        global currPg
        global connPg
        connPg = psycopg2.connect("dbname='{}' user='sfdbo' host='localhost'".format(dbname))
        currPg = connPg.cursor()
        currPg.execute('DELETE FROM '+tbName+" WHERE etfname='"+etfName+"'")
	for j,xvals in enumerate(df):
		currPg.execute("INSERT INTO "+tbName+" VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", xvals)
        connPg.commit()
	updateTemp2Hist(currPg,tbName,etfName,pbDate)
        connPg.close()

"""
https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1521942788811.ajax?fileType=xls&fileName=iShares-Core-SP-500-ETF_fund&dataType=fund
https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1521942788811.ajax?fileType=xls&fileName=iShares-Russell-2000-ETF_fund&dataType=fund
https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/1521942788811.ajax?fileType=xls&fileName=iShares-Russell-3000-ETF_fund&dataType=fund
https://www.ishares.com/us/products/239724/ishares-core-sp-total-us-stock-market-etf/1521942788811.ajax?fileType=xls&fileName=iShares-Core-SP-Total-US-Stock-Market-ETF_fund&dataType=fund
"""
def xls_stock_ishare_temp_DNLD():
	etfX='ivv'
	dbname='ara'
	ldate='remote'
	if len(sys.argv) > 1:
		etfX = sys.argv[1].lower()
	if len(sys.argv) > 2:
		dbname = sys.argv[2]
	if len(sys.argv) > 3:
		ldate = sys.argv[3]
	isLocal = False if ldate=='remote' else True

	#urlx = 'https://www.ishares.com/us/{}/fund-download.dl'
	urlx = 'https://www.ishares.com/us/products/{0}/{1}/1521942788811.ajax?fileType=xls&fileName={2}&dataType=fund'
	etfLst={
	'ivv': ['239726','ishares-core-sp-500-etf','iShares-Core-SP-500-ETF_fund'],
	'iwv': ['239714','ishares-russell-3000-etf','iShares-Russell-3000-ETF_fund'],
	'iwm': ['239710','ishares-russell-2000-etf','iShares-Russell-2000-ETF_fund'],
	'itot':['239724','ishares-core-sp-total-us-stock-market-etf','iShares-Core-SP-Total-US-Stock-Market-ETF_fund']
	}
	try:
		urlName=urlx.format(*etfLst[etfX])
		sys.stderr("==URL:{}\n".format( urlName))
	except Exception as e:
		sys.stderr("**ERROR: {}, Usage of [ivv|itot|iwm|iwv]\n".format(str(e)))
		exit(-1)
	if(isLocal==True):
		urlName= "./{}.{}.xls".format(etfX,ldate)
		try:
			xldata = open(urlName).read()
		except:
			sys.stderr("**ERROR: {} not found!\n".format(urlName))
			exit(-2)
	else:
		xldata = urllib2.urlopen(urlName).read()
	sys.stderr("==etfName:{}\n".format( etfX))
	(pbdate,stockLst) = parse_ishare_xls(xldata,etfX)

	if(isLocal==False):
		fpX="./{}.{}.xls".format(etfX,pbdate)
		with open(fpX, 'w') as f: f.write(xldata)
		sys.stderr(" --etfName:{}:{}\n".format( etfX,fpX))

	tbX="stock_ishare_temp"
	writeXls2Pgdb(stockLst,tbX,etfX,pbdate,dbname=dbname)
	return stockLst

def csv_stock_ishare_temp_DNLD():
	import pandas as pd
	from io import StringIO
	from _alan_calc import save2pgdb
	etfX='ivv'
	dbname='ara'
	ldate='remote'
	if len(sys.argv) > 1:
		etfX = sys.argv[1].lower()
	if len(sys.argv) > 2:
		dbname = sys.argv[2]
	if len(sys.argv) > 3:
		ldate = sys.argv[3]
	isLocal = False if ldate=='remote' else True

	urlx = 'https://www.ishares.com/us/products/{0}/{1}/1467271812596.ajax?fileType=csv&fileName={2}&dataType=fund'
	etfLst={
	'ivv': ['239726','ishares-core-sp-500-etf','iShares-Core-SP-500-ETF_fund'],
	'iwv': ['239714','ishares-russell-3000-etf','iShares-Russell-3000-ETF_fund'],
	'iwm': ['239710','ishares-russell-2000-etf','iShares-Russell-2000-ETF_fund'],
	'itot':['239724','ishares-core-sp-total-us-stock-market-etf','iShares-Core-SP-Total-US-Stock-Market-ETF_fund']
	}
	try:
		urlName=urlx.format(*etfLst[etfX])
		sys.stderr.write("URL:{}\n".format(urlName))
	except Exception as e:
		print >> sys.stderr, "**ERROR: {}, Usage of [ivv|itot|iwm|iwv]".format(str(e))
		exit(-1)

	if(isLocal==True):
		urlName= "./{}.{}.csv".format(etfX,ldate)
		try:
			xldata = open(urlName).read().split("\n")
			stockLst = pd.read_csv(StringIO(xldata),sep=",",header=9)
		except:
			print >> sys.stderr, urlName, " Not Found!"
			exit(-2)
	else:
		stockLst = pd.read_csv(urlName,sep=",",header=9)
	print >> sys.stderr, "UPDATE [stock_ishare_temp] ", etfX

	tbX="stock_ishare_temp"
	colx=[re.sub("[().\s%]","",xs).lower() for xs in stockLst.columns]
	stockLst.columns=colx

	a2f = lambda x: float(re.sub(',','',x) if isinstance(x,str) else x)
	for xs in ['price', 'shares', 'marketvalue', 'notionalvalue']:
        	stockLst[xs]=stockLst[xs].apply(a2f)
	stockLst.dropna(subset=['ticker'],inplace=True)
	stockLst['etfname']=etfX

	sys.stderr.write( "Upload fund:{} to {}::{}\n".format(etfX,dbname,tbX))
	save2pgdb(stockLst,dbname,tablename=tbX)
	return stockLst

if __name__ == '__main__':
	#ret = xls_stock_ishare_temp_DNLD()
	ret = csv_stock_ishare_temp_DNLD()
