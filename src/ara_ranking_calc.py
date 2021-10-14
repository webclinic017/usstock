#!/usr/bin/env python 
import sys
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import pandas as pd

def ranking_by_ai(pgDB=None,sqr='',category='',subtype='',refname='',mtc=None,wmode='replace',tablename='ara_ranking_list',zsLst=[-1.00,-0.5,1.0,1.25],**optx):
	if not pgDB:
		pgDB = conn2psql(pgDB)
	sqr="""SELECT ticker,zscore,round((rrate_fcs/sigma)::numeric, 5) as zsunadj,rrate_fcs,sigma FROM ara_outlook_factor_temp WHERE sector NOT SIMILAR TO '%%(Index|ETF)' AND factor='overall'"""
	df = pd.read_sql(sqr,pgDB)
	# creating 5-start rating 
	d5 = df.query('zsunadj>={}'.format(*zsLst[-1:])).sort_values(['zsunadj'],ascending=False)
	d4 = df.query('zsunadj>={} and zsunadj<{}'.format(*zsLst[-2:])).sort_values(['zsunadj'],ascending=False)
	d3 = df.query('zsunadj>={} and zsunadj<{}'.format(*zsLst[-3:])).sort_values(['zsunadj'],ascending=False)
	d2 = df.query('zsunadj>={} and zsunadj<{}'.format(*zsLst[-4:])).sort_values(['zsunadj'],ascending=False)
	d1 = df.query('zsunadj<{}'.format(*zsLst[:1])).sort_values(['zsunadj'],ascending=False)
	df.loc[:,'rating']=0
	for j,dx in enumerate([d1,d2,d3,d4,d5]):
		df.loc[dx.index,'rating'] = j+1
	print df.sort_values(['zsunadj'],ascending=False).to_string()
	return df

def ranking_calc(pgDB,sqr,category,subtype,refname,mtc=None,wmode='replace',tablename='ara_ranking_list'):
	df=pd.read_sql(xqr,pgDB)
	ix=df.sort_values(by=[refname],ascending=False).index
	# df['ranking'].iloc[ix]=range(1,len(ix)+1) # Wrong allocation, warning msg appear
	df.loc[ix,'ranking']=range(1,len(ix)+1)
	df['category']=category
	df['subtype']=subtype
	df['ntotal']=len(ix)
	df['refval']=df[refname]
	d=df[['category','subtype','ticker','ranking','ntotal','refval']]
	if mtc is not None:
		d=d.merge(mtc,on='ticker')
	print >> sys.stderr, d.head(2)
	d.to_sql(tablename, pgDB, schema='public', index=False, if_exists=wmode)
	return d

def run_ranking(pgDB,sqr,category,subtype,refname,mtc=None,wmode='replace',tablename='ara_ranking_list'):
	d=None
	print  >> sys.stderr, "**RUNNING {}. {} @ {}:\n\t{}".format(category,subtype,"ranking_calc",sqr)
	try:
		d=ranking_calc(pgDB,xqr,category,subtype,refname,mtc=mtc,wmode=wmode)

	except Exception,e:
		print  >> sys.stderr, "**ERROR {}. {} @ {}: {}".format(category,subtype,"ranking_calc",str(e))
	return d

def conn2psql(pgDB=None,dbname='ara',hostname='localhost',port=5432,user='sfdbo'):
	if not pgDB:
		pgDB = create_engine('postgresql://{}@{}:{}/{}'.format(user,hostname,port,dbname))
	return pgDB

def main():
	pgDB = conn2psql()

	#- get company and sector in Chinese 
	m=pd.read_sql('SELECT distinct * FROM mapping_ticker_estar',pgDB)
	s=pd.read_sql('SELECT * FROM spdr_sector',pgDB)
	mtc=m.merge(s[['sector','sector_cn']],on='sector',how='left')
	sn=map(lambda x: x.split()[0],mtc[mtc['sector_cn'].isnull()]['company_cn'])
	mtc.loc[mtc['sector_cn'].isnull(),'sector_cn']=sn

	wmode='replace'
	#- for AI, Index
	category='AI';subtype='Index';refname='rating'
	xqr="SELECT 0::integer as ranking,zscore*2.5 AS rating, * FROM ara_outlook_factor_temp WHERE sector like '%%Index' AND factor='overall' AND zscore IS NOT NULL"
	d=run_ranking(pgDB,xqr,category,subtype,refname,mtc=mtc,wmode=wmode)

	wmode='append'
	#- for AI, SP500
	category='AI';subtype='SP500';refname='rating'
	xqr="SELECT 0::integer AS ranking,(rrate_fcs/sigma)*2.5 AS rating, * FROM ara_outlook_factor_temp WHERE sector NOT SIMILAR TO '%%(Index|ETF)' AND factor='overall'"
	d=run_ranking(pgDB,xqr,category,subtype,refname,mtc=mtc,wmode=wmode)

	cymd=pd.read_sql('SELECT * FROM ara_uptodate',pgDB).iloc[0][0]
	#- for VOLUME, SP500
	category='VOLUME';subtype='SP500';refname='volume'
	xqr="SELECT 0::integer AS ranking, p.* FROM price_volume_temp p,(SELECT ticker FROM ara_outlook_factor_temp WHERE sector NOT SIMILAR TO '%%(Index|ETF)' AND factor='overall') a WHERE p.ticker=a.ticker"
	d=run_ranking(pgDB,xqr,category,subtype,refname,mtc=mtc,wmode=wmode)

	#- for OHLC, STOCK
	#nday=1
	#cymd=pd.read_sql('SELECT max(pbdate) FROM prc_hist',pgDB).iloc[0][0]
	#dformat="%Y%m%d"
	#dt=datetime.strptime(str(cymd),dformat)-timedelta(days=nday)
	#x1dYmd=datetime.strftime(dt,dformat)
	x1dYmd=pd.read_sql("SELECT pbdate FROM ohlc_daily_comment_cn GROUP BY pbdate ORDER BY pbdate DESC limit 2",pgDB).iloc[1][0]
	xqTmp="""
	SELECT 0::integer AS ranking,o.* FROM
		(SELECT * FROM ohlc_daily_comment_cn WHERE pbdate>={x1dYmd} AND trr>0 ) AS o,
		(SELECT * FROM price_volume_temp) as p
		WHERE o.ticker=p.ticker ORDER BY o.trr DESC
	"""
	category='TRR';subtype='STOCK';refname='trr'
	xqr=xqTmp.format(x1dYmd=x1dYmd)
	d=run_ranking(pgDB,xqr,category,subtype,refname,mtc=mtc,wmode=wmode)

if __name__ == '__main__':
	main()
