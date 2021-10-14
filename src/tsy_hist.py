#!/usr/bin/env python
import sys
from optparse import OptionParser
from sqlalchemy import create_engine, MetaData
from datetime import datetime
import pandas as pd
from pandas.io import sql


date2ymd = lambda x: int(datetime.strptime(str(x),"%m/%d/%y").strftime("%Y%m%d"))

def upd_temp2hist(pgDB=None,temp=None,hist=None,pcol=[],dbname=None,hostname='localhost'):
	"""
	Insert/update additional values from table: [temp] to [hist]
	base on primary keys pcol
	"""
	if any(x==None for x in (temp,hist)) is True:
		return None
	xqTmp='''CREATE TABLE IF NOT EXISTS "{hist}" AS SELECT * FROM "{temp}" WHERE 1=2; {delC} 
INSERT INTO "{hist}" SELECT DISTINCT * FROM "{temp}"'''
	if len(pcol)>0:
		whrC = 'WHERE '+' AND '.join(['B."{0}" = C."{0}"'.format(j) for j in pcol])
		delC = '\nDELETE FROM "{hist}" B USING "{temp}" C {whrC} ;'.format(hist=hist,temp=temp,whrC=whrC)
	else:
		delC = ''
	xqr = xqTmp.format(hist=hist,temp=temp,delC=delC)
	try:
		if all(x==None for x in (pgDB,dbname)) is True:
			print >> sys.stderr,"**ERROR: DB not defined!"
			return xqr
		elif pgDB is None and dbname is not None:
			pgDB=conn2pgdb(dbname=dbname,hostname=hostname)
		pgDB.execute(xqr,pgDB)
	except Exception as e:
		return str(e)
	return xqr


def treasury_hist(pgDB,yLst=[]):
	urx="https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yieldYear&year={}"
	classname="t-chart"
	if len(yLst)<1:
		yLst=range(1990,2019)
	rmode='replace'
	for y4 in yLst:
		uri=urx.format(y4)
		df=pd.read_html(uri,attrs={"class":classname},index_col=0,header=0)[0]
		df['pbdate']= map(date2ymd,df.index)
		df.columns = [x.lower() for x in df.columns]
		df=df[['1 mo','3 mo','6 mo','1 yr','2 yr','3 yr','5 yr','7 yr','10 yr','20 yr','30 yr','pbdate']] 
		df.to_sql('tsy_temp',pgDB,index=False,schema='public',if_exists=rmode)
		rmode='append'
	upd_temp2hist(pgDB,temp='tsy_temp',hist='tsy_hist',pcol=['pbdate'])
	return df

def treasury_upd(pgDB):
	uri="https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yield"
	classname="t-chart"
	rmode='replace'
	df=pd.read_html(uri,attrs={"class":classname},index_col=0,header=0)[0]
	df['pbdate']= map(date2ymd,df.index)
	dg=df[['1 mo','3 mo','6 mo','1 yr','2 yr','3 yr','5 yr','7 yr','10 yr','20 yr','30 yr','pbdate']] 
	dg.to_sql('tsy_temp',pgDB,index=False,schema='public',if_exists=rmode)
	sqx="INSERT INTO tsy_hist (SELECT b.* FROM tsy_hist a RIGHT JOIN tsy_temp b ON a.pbdate=b.pbdate WHERE a.pbdate IS NULL)"
	sql.execute(sqx, con=pgDB)

if __name__ == '__main__':
	pgDB = create_engine('postgresql://sfdbo@{}:5432/{}'.format('localhost','ara'))
	yLst = sys.argv[1:]
	if len(yLst)<1:
		treasury_upd(pgDB)
	else:
		treasury_hist(pgDB,yLst)

