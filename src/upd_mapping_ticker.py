#!/usr/bin/env python3
'''
Update mapping_ticker_cik table (weekly cron every Saturday)
Usage of:
upd_mapping_ticker.py upd
OR
upd_mapping_ticker.py deact

Required tables:
  PGDB::ara sp500_component, mapping_ticker_cik
  MDB::ara madmoney_hist

Last Mod., Mon Feb 17 16:24:38 EST 2020
'''
import sys
from _alan_date import next_date
from _alan_calc import conn2mgdb, sqlQuery, conn2pgdb
from _alan_str import insert_mdb
from yh_chart import yh_quote_comparison as yqc
from ticker_mapping import ticker_mapping as tkmp

def upd_mapping_ticker(lkbk=-7,mxcap=10**9,debugTF=False,saveDB=True,**optx):
	sys.stderr.write("===STARTING to update ticker list\n")
	#1. UPDATE sp500 / Dow30 list
	from theme_list import sp500_component
	sp500=sp500_component(saveDB=False)
	if len(sp500)<1:
		sp500=sqlQuery("SELECT * FROM sp500_component")

	sLst = sp500['ticker'].values

	#2. PULL MDB::ara:madmoney_hist
	dt6mo=int(next_date(months=-6,dformat='%Y%m01',dtTF=False))
	mdb=conn2mgdb(dbname='ara')
	mLst=mdb['madmoney_hist'].find({"pbdate":{"$gt":dt6mo}}).distinct("ticker")
	mLst = [x.replace('.','-') for x in mLst]

	#3. COMPILE a new list based on madmoney and SP500 new tickers
	tkCIK=sqlQuery("SELECT * FROM mapping_ticker_cik WHERE act_code=1 and ticker not similar to '%%(^|=)%%'")
	tLst = tkCIK['ticker'].values
	n1Lst = set(sLst)-set(tLst)
	sys.stderr.write("New SP500 List:{}\n".format(n1Lst))
	n2Lst = set(mLst)-set(tLst)
	sys.stderr.write("New madMoney List:{}\n".format(n2Lst))
	nLst = list( n1Lst.union(n2Lst) )

	#4. PULL only valid quotes within last 7 days and marketCap > 1B
	dt7d=int(next_date(days=lkbk,dformat='%Y%m%d',dtTF=False))
	df=yqc(nLst,tablename=None,screenerTF=False,dfTF=True)
	xqr='not(pbdate<{}|marketCap<{})'.format(dt7d,mxcap)
	dg = df.query(xqr)
	newLst=dg['ticker'].values
	if len(newLst)<1:
		sys.stderr.write("No Additions\n")
		return(0)
	else:
		sys.stderr.write("New Additional List:{}\n".format(newLst))

	#5. PULL new list 'newLst' with cik/sic/sector info
	if saveDB:
		newDf = tkmp(newLst)
		pgDB=conn2pgdb(dbname='ara')
		newDf.to_sql('ticker_mapping_temp',pgDB,schema='public',index=False,if_exists='replace')
		xqr="""
		DELETE FROM mapping_ticker_cik B USING ticker_mapping_temp C WHERE B.ticker = C.ticker;
		INSERT INTO mapping_ticker_cik
		SELECT cik,ticker,company,sic,sector,company_cn,sector_cn,1::int as act_code FROM (SELECT a.*,b.sector,b.sector_cn from ticker_mapping_temp a LEFT JOIN spdr_sector b ON a.sector_alias=b.sector_alias) as s
		"""
		pgDB.execute(xqr,pgDB)
	return newLst

def deact_mapping_ticker(lkbk=-14,mxcap=500000000,debugTF=False,saveDB=True,**optx):
	'''
	De-activate tickers with invalid quotes within last 14 days or marketCap < 500MM
	'''
	# de-active stale tickers
	sys.stderr.write("===STARTING de-active stale tickers\n")
	pgDB=conn2pgdb(dbname='ara')
	tkCIK=sqlQuery("SELECT * FROM mapping_ticker_cik WHERE act_code=1 and ticker not similar to '%%(^|=)%%'")
	tLst = tkCIK['ticker'].values
	dt7d=int(next_date(days=lkbk,dformat='%Y%m%d',dtTF=False))
	df=yqc(tLst,tablename=None,screenerTF=False,dfTF=True)
	if len(df)<1:
		sys.stderr.write("No deactivate list\n")
		return(0)
	yqr='pbdate<{}|marketCap<{}'.format(dt7d,mxcap)
	dg = df.query(yqr)
	newLst=dg['ticker'].values
	if len(newLst)<1:
		sys.stderr.write("No deactivate list\n")
		return(0)
	else:
		sys.stderr.write("New deactivate List:{}\n".format(newLst))
	if saveDB:
		xqTmp="UPDATE mapping_ticker_cik set act_code=0 where ticker in {}"
		xqr = xqTmp.format(tuple(newLst))
		pgDB.execute(xqr)
	return newLst

if __name__ == '__main__':
	args=sys.argv[1:]
	fx='deact' if len(args)<1 else args[0]
	fx = "{}_mapping_ticker".format(fx)
	fn = globals()[fx] if fx in globals() else deact_mapping_ticker
	fn()
