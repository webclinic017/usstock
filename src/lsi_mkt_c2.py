#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Program: lsi_mkt.py
    Description: Create market commentary based on
	1. idxLst: SPY, XLK, and most-recommended ETF (based on highest score)
	2. highVolLst: top-3 most volatile globalmacro 
	3. rcntLst: recent updated globalmacro index (within one-week for non-daily)
    Input table required:
	[Chinese RMB] [appreciated] against the [US dollar] last week while the [Gold 3PM Price] [increased] and the [2-Y Treasury] rates [also] [rose].  In the stock markets, [SPY] [increased] while the [technology sector ETF XLK] [also] [gained].  Now, here is our exclusive AICaas market report and forecasts. 
    Example:
	python lsi_mkt.py --lang=cn --use_mp3 --database=ara
    Function:
	def generate_comment_header_cn(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="cn",mp3YN=False):
	def generate_comment_header_en(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="en",mp3YN=False):
	def generate_comment_footer_cn(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="cn",mp3YN=False):
	def generate_comment_footer_en(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="en",mp3YN=False):
	def generate_cmt(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="en",mp3YN=False,funcname=''):
	def get_macro_fcs(tkLst=[],pgDB=None,dbname='ara',lang="en"):
	def get_ohlc_fcs(tkLst=[],pgDB=None,dbname='ara',lang="en"):
	def get_highVolLst(tkLst=[],pgDB=None,dbname='ara'):
	def get_rcntLst(tkLst=[],pgDB=None,dbname='ara'):
	def macro_list_x(j,datax,currdate=None,pgDB=None):
	def get_macro_list(currdate=None,pgDB=None,dbname='ara',lang="en"):
	def create_macro_comment(f,xd,ts,flgAddi,pgDB=None,lang="cn",mp3YN=True):
	def label_masking(x,lang="en"):
	def create_stock_comment(f,j,xd,ts,pgDB=None,lang="cn",mp3YN=True,ctky="stock"):
	def str2gtts(pfx,xstr,lang="cn"):
	def prn_mkt_cmt(vcmt,region,lang,mp3YN,mp3Make=False,dirname=None):
	def run_lsi_mkt(tkLst,**kwargs):
	def opt_lsi_mkt(argv,retParser=False):
    Version: 0.655
    Last mod., Tue Aug 21 20:56:28 EDT 2018
"""
import datetime
import pandas as pd
from optparse import OptionParser
from _alan_calc import conn2pgdb
from _alan_date import ymd_delta
from _alan_str import ymd2md,udfStr,fq2unit_str,random
from _alan_ohlc_fcs import convert_data_comment_fcst
#from lsi_daily import *
import os,subprocess
import sys

def generate_comment_header_cn(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="cn",mp3YN=False):
	""" macro1 market comment
		required ts fields: currency1,indicator1,rate1,mkt1,mkt2
	"""
	if ts is None:
		return None
	for (ky,va) in f.iteritems(): 
		exec("{}=va".format(ky))
	if 'macro1XChg' not in f:
		macro1XChg=0 
		macro1Vntdate=20180101
	#xsign = 1 if currency1Ticker[-2:] == "US" else -1
	xsign = -1 # cn version always use EURO direction
	currency1XTrendWd=udfStr(xsign*currency1XChg,["貶值","升值","持平"],0.0001,lang=lang)
	indicator1XTrendWd=udfStr(indicator1XChg,udf=udfLst,zs=0.0001,lang=lang)
	rate1XTrendWd=udfStr(rate1XChg,udf=udfLst,zs=0.0001,lang=lang)
	commodity1XTrendWd=udfStr(commodity1XChg,udf=udfLst,zs=0.0001,lang=lang)
	currency2XTrendWd=udfStr(xsign*currency2XChg,["貶值","升值","持平"],0.0001,lang=lang)
	indicator2XTrendWd=udfStr(indicator2XChg,udf=udfLst,zs=0.0001,lang=lang)
	rate2XTrendWd=udfStr(rate2XChg,udf=udfLst,zs=0.0001,lang=lang)
	commodity2XTrendWd=udfStr(commodity2XChg,udf=udfLst,zs=0.0001,lang=lang)
	mkt1XTrendWd=udfStr(mkt1XChg,udf=udfLst,zs=0.0001,lang=lang)
	mkt2XTrendWd=udfStr(mkt2XChg,udf=udfLst,zs=0.0001,lang=lang)
	vntDateWd=ymd2md(str(macro1Vntdate),ym="%B %d",lang=lang) if 'macro1Vntdate' in f else ""
	unitStr=fq2unit_str(macro1Mfreq,lang) if 'macro1Mfreq' in f else "月"
	pastTrendWd = udfStr(macro1XChg,udfLst,0.0001,lang=lang)
	mkt0Label=mkt0Label[:mkt0Label.find('指數基金')]

	(currency1Adv,currency2Adv)=('同時','也在') if currency1XChg*currency2XChg>=0 else ('然而','卻在')
	(indicator1Adv,indicator2Adv,indicator12Adv)=('並且','也在','正相關') if indicator1XChg*indicator2XChg>0 else ('然而','卻在','負相關')
	(rate1Adv,rate2Adv)=('','也在') if rate1XChg*rate2XChg>=0  else ('可是','卻在')
	(commodity1Adv,commodity2Adv,commodity12Adv)=('並且','也在','正相關') if commodity1XChg*commodity2XChg>=0 else ('然而','卻在','負相關')
	(mkt1Adv,mkt2Adv)=('','也正') if mkt1XChg*mkt2XChg>=0 else ('但是','卻正')
	(macro1Adv,macro11Adv)=('','也正') if mkt1XChg*mkt2XChg>=0 else ('但是','卻正')
	dux = locals()
	ret=ts.format(**dux)
	return(ret)

def generate_comment_header_en(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="en",mp3YN=False):
	""" macro1 market comment
		required ts fields: currency1,indicator1,rate1,mkt1,mkt2
	"""
	if ts is None:
		return None
	for (ky,va) in f.iteritems(): 
		exec("{}=va".format(ky))
	if 'macro1XChg' not in f:
		macro1XChg=0 
		macro1Vntdate=20180101
	#xsign = 1 if currency1Ticker[-2:] == "US" else -1
	xsign = -1 # cn version always use EURO direction
	currency1XTrendWd=udfStr(xsign*currency1XChg,["depreciated","appreciated","stayed flat"],0.0001,lang=lang)
	indicator1XTrendWd=udfStr(indicator1XChg,udf=udfLst,zs=0.0001,lang=lang)
	rate1XTrendWd=udfStr(rate1XChg,udf=udfLst,zs=0.0001,lang=lang)
	commodity1XTrendWd=udfStr(commodity1XChg,udf=udfLst,zs=0.0001,lang=lang)
	currency2XTrendWd=udfStr(xsign*currency2XChg,["depreciated","appreciated","stayed flat"],0.0001,lang=lang)
	indicator2XTrendWd=udfStr(indicator2XChg,udf=udfLst,zs=0.0001,lang=lang)
	rate2XTrendWd=udfStr(rate2XChg,udf=udfLst,zs=0.0001,lang=lang)
	commodity2XTrendWd=udfStr(commodity2XChg,udf=udfLst,zs=0.0001,lang=lang)
	mkt1XTrendWd=udfStr(mkt1XChg,udf=udfLst,zs=0.0001,lang=lang)
	mkt2XTrendWd=udfStr(mkt2XChg,udf=udfLst,zs=0.0001,lang=lang)
	vntDateWd=ymd2md(str(macro1Vntdate),ym="%B %d",lang=lang) if 'macro1Vntdate' in f else ""
	unitStr=fq2unit_str(macro1Mfreq,lang) if 'macro1Mfreq' in f else "month"
	pastTrendWd = udfStr(macro1XChg,udfLst,0.0001,lang=lang)
	mkt0Label=mkt0Label[:mkt0Label.find('Index')]

	(currency1Adv,currency2Adv)=('and','also') if currency1XChg*currency2XChg>=0 else ('','yet')
	(indicator1Adv,indicator2Adv,indicator12Adv)=('and','also','positively correlated') if indicator1XChg*indicator2XChg>0 else ('','however','negatively correlated')
	(rate1Adv,rate2Adv)=('','also') if rate1XChg*rate2XChg>=0  else ('','yet')
	(commodity1Adv,commodity2Adv,commodity12Adv)=('and','also','positively correlated') if commodity1XChg*commodity2XChg>=0 else ('','however','negatively correlated')
	(mkt1Adv,mkt2Adv)=('','also') if mkt1XChg*mkt2XChg>=0 else ('','yet')
	(macro1Adv,macro11Adv)=('','also') if mkt1XChg*mkt2XChg>=0 else ('but','')
	dux = locals()
	ret=ts.format(**dux)
	return(ret)

def generate_comment_footer_cn(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="cn",mp3YN=False):
	""" macro market comment
		required 
		required ts fields: currency,indicator,rate,mkt1,mkt2
	"""
	if ts is None:
		return None
	for (ky,va) in f.iteritems(): 
		exec("{}=va".format(ky))
	
	mkt1NTrendWd=udfStr(mkt1NChg,udf=udfLst,zs=0.0001,lang=lang)
	rateNTrendWd=udfStr(rate1NChg,udf=udfLst,zs=0.0001,lang=lang)
	rhoWd=udfStr(rho,["正","負","獨立不存在"],0.05,lang=lang)
	rateIfWd=udfStr(rate1NChg,udf=udfLst,zs=0.0001,lang=lang)
	mkt1IfWd=udfStr(rho*rate1NChg,udf=udfLst,zs=0.000001,lang=lang)
	if rho*rate1NChg*mkt1NChg<0:
		macro1Adv="，可能不如原始預期，必須持續觀察利率走向。"
	else:
		macro1Adv="，更勝於原有預期。"
	if abs(rho*rate1NChg) <= 0.000001:
		macro1Adv=''
	dux = locals()
	ret=ts.format(**dux)
	return(ret)

def generate_comment_footer_en(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="en",mp3YN=False):
	""" macro market comment
		required 
		required ts fields: currency,indicator,rate,mkt1,mkt2
	"""
	if ts is None:
		return None
	for (ky,va) in f.iteritems(): 
		exec("{}=va".format(ky))
	mkt1NTrendWd=udfStr(mkt1NChg,["an increase","a decrease","no change in"],0.0001)
	rateNTrendWd=udfStr(rate1NChg,["a rise","a fall","no change in"],0.0001)
	rhoWd=udfStr(rho,["positively","negatively","almost not"],0.05)
	rateIfWd=udfStr(rate1NChg,["goes up","goes down","stays flat"],0.0001)
	mkt1IfWd=udfStr(rho*rate1NChg,["rise","fall","stay flat"],0.000001)
	if rho*rate1NChg*mkt1NChg<0:
		macro1Adv='against our original forecast'
	else:
		macro1Adv='even more'
	if mkt1IfWd == "stay flat":
		macro1Adv=''
	dux = locals()
	ret=ts.format(**dux)
	return(ret)

def generate_cmt(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="en",mp3YN=False,funcname=''):
	try:
		funcN="{}_{}".format(funcname,lang)
		funcArg=globals()[funcN]
		xcmt=funcArg(f,ts=ts,dotSign=dotSign,prcn=prcn,usdSign=usdSign,udfLst=udfLst,mp3YN=mp3YN)
	except Exception,e:
		print  >> sys.stderr, "**ERROR @ {}():{}".format(funcN,str(e))
		xcmt=''
	return xcmt

def get_macro_fcs(tkLst=None,pgDB=None,dbname='ara',lang="en"):
	""" 1a, get selected market indices from [macro_hist_fred] table index section
	"""
	if pgDB is None:
		pgDB=conn2pgdb(dbname=dbname)
	if tkLst is None:
		tkLst=["^DJI","^SOX","^IXIC","^GSPC"]
	ext= "" if lang=="en" else "_"+lang
	tkStr="('{}')".format("','".join(tkLst))
	sqr="""SELECT p.*,m.freq as mfreq,m.label{} as label, 'Market Index' as sector, m.category,m.category_seq FROM 
		(SELECT * FROM macro_fcs where ticker in {} ) as p
		LEFT JOIN mapping_series_label m ON p.ticker=m.series ORDER BY m.category_seq""".format(ext,tkStr)
	tmpx = pd.read_sql(sqr,con=pgDB)
	datax = tmpx.query("(freq=='W' & mfreq=='D') | (freq=='D' & mfreq!='D')").copy()
	datax['zx']=(datax.prc_chg/datax.rrate_sigma)
	ds=datax.query("freq=='W'")
	dm=[ds.query("ticker=={!r}".format(x)).iloc[0].to_dict() for x in tkLst]
	return (dm,datax,pgDB)

def get_ohlc_fcs(tkLst=[],pgDB=None,dbname='ara',lang="en"):
	""" 1b, get most recommended AI market sector ETF
	"""
	if pgDB is None:
		pgDB=conn2pgdb(dbname=dbname)
	xqr="SELECT ticker FROM ara_ranking_list WHERE category='AI' AND subtype='Index' ORDER BY ranking limit 1"
	tkLst=pd.read_sql(xqr,pgDB).iloc[0].values
	ext= "" if lang=="en" else "_"+lang
	tkStr="('{}')".format("','".join(tkLst))
	sqr="""SELECT p.*,'D' as mfreq, m.company{} as label ,m.sector ,'stock' as category, 5::int as category_seq FROM
		(SELECT * FROM ohlc_fcs where ticker in {} ) as p
		LEFT JOIN mapping_ticker_cik m ON p.ticker=m.ticker""".format(ext,tkStr)
	datax = pd.read_sql(sqr,con=pgDB)
	print >> sys.stderr, sqr,dbname
	datax['zx']=(datax.prc_chg/datax.rrate_sigma)[:]
	ds=datax.query("freq=='W'")
	dm=ds.to_dict(orient='record')
	return (dm,datax,pgDB)

def get_index_list(idxLst=None,pgDB=None,dbname='ara',lang="en"):
	""" 1. get recent stock market indices and relevant ETF
	"""
	(dm,datam,pgDB)=get_macro_fcs(idxLst,pgDB=pgDB,dbname=dbname,lang=lang)
	(dmo,datax,pgDB)=get_ohlc_fcs(None,pgDB=pgDB,dbname=dbname,lang=lang)
	dm = dmo+dm
	return(dm,datax,pgDB)

def macro_list_x(j,datax,currdate=None,pgDB=None):
	xqr="category_seq=={}".format(j)
	df=datax.query(xqr)
	if j==1: # indicator
		df=df[~df['ticker'].isin(["^DJI","^SOX","^IXIC","^GSPC"])]
	elif j==2: # currency
		df=df.query("ticker!='DEXUSAL'")
	elif j==3: # rate
		df=df[df['ticker'].str.contains("DGS")]
	elif j==4: # macro
		try:
			vntLst=pd.read_sql("select freq,series,vntdate::int from macro_vintage_date where series in ('UNRATE','SPCS20RSA','A939RX0Q048SBEA','CPIAUCNS','PPIACO','HPIPONM226S','UMCSENT') ORDER BY vntdate DESC",con=pgDB)
		except Exception,e:
			print  >> sys.stderr, "**ERROR @ {}():{},DB:{}".format("macro_list_x",str(e),pgDB)
			return None
		x7d=int(ymd_delta(currdate,days=10))
		vntx=vntLst.query("vntdate>={}".format(x7d))
		if vntx.shape[0]>0:
			df=df[df['ticker'].str.contains("|".join(vntx['series'].values))]
			df['series']=df['ticker'].apply(lambda x:x.replace('_PCTCHG',''))			
			df=df.merge(vntx[['series','vntdate']],on='series')
		else:
			return None
	if(len(df)<1):
		return None
	dy=df.loc[df.zx.abs().sort_values(ascending=False).index[:2]]
	return dy

def get_macro_list(currdate=None,pgDB=None,dbname='ara',lang="en"):
	""" 2. get globalmacro index 
	"""
	if pgDB is None:
		pgDB=conn2pgdb(dbname=dbname)
	ext= "" if lang=="en" else "_"+lang
	sqr="SELECT p.*,m.freq as mfreq,m.label{0} as label, m.category,m.category_seq FROM macro_fcs p LEFT JOIN mapping_series_label m ON p.ticker=m.series ORDER BY m.category_seq".format(ext)
	tmpx = pd.read_sql(sqr,con=pgDB)
	datax = tmpx.query("(freq=='W' & mfreq=='D') | (freq=='D' & mfreq!='D')").copy()
	datax['zx']=(datax.prc_chg/datax.rrate_sigma)
	print >> sys.stderr, datax.head(2)
	print >> sys.stderr, datax.tail(3)
	dx=[]
	for j in (datax.category_seq.unique()) :
		dy=macro_list_x(j,datax,currdate=currdate,pgDB=pgDB)
		if dy is None or len(dy)<1:
			continue
		dx.append(dy.to_dict(orient='records'))
	return (dx,datax,pgDB)

def create_macro_comment(f,j,xd,ts,flgAddi,pgDB=None,lang="cn",mp3YN=True):
	tkLst=[]
	ticker=xd["ticker"]
	ky="{}{}".format(xd['category'].split()[-1],j)
	f[ky+"Ticker"]=ticker
	f[ky+"Label"]=xd['label'].replace('-Y',' Year')
	f[ky+"XChg"]=xd['prc_chg']
	f[ky+"NChg"]=xd['prc_fcs']/xd['prc_cur']-1
	if 'vntdate' in xd:
		f[ky+"Vntdate"]=xd['vntdate']
		f[ky+"Mfreq"]=xd['mfreq']
		flgAddi += 1
	dx=pd.DataFrame([xd])
	xcmt=convert_data_comment_fcst(ticker,ky,dx,pgDB=pgDB,lang=lang,mp3YN=mp3YN,ts=ts)
	xcmt=xcmt.replace('-Y',' Year')
	print >> sys.stderr, str(xcmt)
	tkLst.append(ticker)
	return (xcmt,flgAddi,tkLst)

def label_masking(x,lang="en"):
	x=x.replace('SPDR ','').replace('ETF:','')
	if lang=="en":
		x=x.replace('SPY','S P Y ').replace('SOX','S O X ')
	return x
	
def create_stock_comment(f,j,xd,ts,pgDB=None,lang="cn",mp3YN=True,ctky="stock"):
	""" ctky: category key among [macro|rate|currency|indicator|stock]
	"""
	tkLst=[]
	ticker=xd["ticker"]
	ky="mkt{}".format(j)
	f[ky+"Ticker"]=ticker
	f[ky+"Label"]= label_masking(xd["label"],lang)
	f[ky+"XChg"]=xd["prc_chg"]
	f[ky+"NChg"]=xd["prc_fcs"]/xd["prc_cur"]-1
	dx=pd.DataFrame([xd])
	xcmt=convert_data_comment_fcst(ticker,ctky,dx,pgDB=pgDB,lang=lang,mp3YN=mp3YN,ts=ts)
	xcmt = label_masking(xcmt,lang)
	tkLst.append(ticker)
	print >> sys.stderr, str(xcmt)
	return (xcmt,tkLst)

def str2gtts(pfx,xstr,lang="cn"):
	glang="zh-tw" if lang=="cn" else "en"
	if pfx is None or len(pfx)<1:
		print >> sys.stderr, "**ERROR: Filename for making mp3 not exist for content:[{}]!".format(xstr)
		return 
	fname=pfx.replace("_mp3","").replace(".txt","")+".mp3"
	xcmd="python /usr/local/bin/gtts-cli.py -l {} - | sox -G -t mp3 - {} tempo 1.25 speed .95".format(glang,fname)
	print >> sys.stderr, xcmd
	p = subprocess.Popen(xcmd,shell=True,bufsize=1024,stdout=subprocess.PIPE,stdin=subprocess.PIPE)
	p.stdin.write(xstr)
	p.communicate()[0]
	p.stdin.close()

def prn_mkt_cmt(vcmt,region,lang,mp3YN,mp3Make=False,dirname='./'):
	mtype="mp3" if mp3YN is True else ""
	pfx="_".join(filter(None,["mktCmt",region,lang,mtype]))
	dirname = './' if dirname is None or len(dirname)<2 else dirname
	fn=xcmt = ''
	k = 0
	for j,xcmt in enumerate(vcmt):
		if len(xcmt.strip()) < 1:
			continue
		if dirname is None or os.path.exists(dirname) is not True:
			continue
		fn="{}/{}_{}.txt".format(dirname,pfx,k)
		fp=open(fn,"w")
		fp.write(xcmt)
		fp.close() 
		if mp3YN is True and mp3Make is True:
			str2gtts(fn,xcmt,lang)
		k = k+1
		
	fn=xcmt = ''
	if dirname is not None and os.path.exists(dirname) is True:
		fn="{}/{}.txt".format(dirname,pfx)
		xcmt="\n".join(vcmt)
		fp=open(fn,"w")
		fp.write(xcmt)
		fp.close() 
	if mp3YN is True and mp3Make is True and len(fn)>4:
		str2gtts(fn,xcmt,lang)
	else:
		print >> sys.stderr, "**ERROR: Filename:{} for making mp3 not exist for content:{}!".format(fn,xcmt)
	return xcmt

def get_ts_cmt(sqx,ts_alt,pgDB=None,dbname=None):
	try:
		if pgDB is None:
			pgDB=conn2pgdb(dbname=dbname)
		ts_cmt=pd.read_sql(sqx,pgDB).iloc[0][0]
	except Exception,e:
		print >> sys.stderr, "** ERROR: get_ts_cmt(),{}",format(str(e))
		ts_cmt = ts_alt
	print >> sys.stderr, str(ts_cmt)
	return ts_cmt

def run_lsi_mkt(tkLst,**kwargs):
	""" Create market commentary based on macro list
	"""
	for ky,va in kwargs.items():
		exec("{}=va".format(ky))

	if lang=="en":
		ts_title='{} Market Closing Report: brought to you by Beyondbond.'
		headerAddiClause="{macro1Label} published at {vntDateWd}, {pastTrendWd} from last {unitStr}. "
		ts_header="""{indicator1Label} {indicator1XTrendWd}, while the {currency1Label} {currency1XTrendWd} against the US dollar, {macro1Adv} the {rate1Label} {macro11Adv} {rate1XTrendWd}.  In the stock market, {mkt1Label} {mkt1XTrendWd}, while the {mkt2Label} {mkt2Adv} {mkt2XTrendWd}.  Our AI recommended sector is {mkt0Label}.
	Now, here is our exclusive AICaas market updates and forecasts.  """
		ts_footer="""Finally, our forecast calls for {mkt1NTrendWd} {mkt1Label} and {rateNTrendWd} {rate1Label} next week. As these two are {rhoWd} correlated, if the {rate1Label} {rateIfWd} further, then the {mkt1Label} is likely to {mkt1IfWd} {macro1Adv}."""
		ts_disclaim="""This electronic message is our opinion only and is not intended to be an investment advise."""
		ts_stock=ts_indicator=ts_currency=ts_rate=ts_macro=None
	else:
		ts_title='智能伏羲 {} 晨間大盤走勢快報：今日值得觀察國際金融走勢如下\n'
		headerAddiClause="{macro1Label}在{vntDateWd}公佈，比前一{unitStr}{pastTrendWd} 。"
		ts_header="""在股市中，隨者{mkt1Label}的{mkt1XTrendWd}，{mkt1Adv}{mkt2Label}{mkt2Adv}{mkt2XTrendWd}。
{indicator1Label}在日前{indicator1XTrendWd}，{indicator1Adv}{indicator2Label}{indicator2Adv}{indicator2XTrendWd}。
{currency1Label}{currency1XTrendWd}，{currency1Adv}{currency2Label}{currency2Adv}{currency2XTrendWd}。
與此同時，{rate1Label}處於{rate1XTrendWd}狀態，{rate1Adv}{rate2Label}{rate2Adv}{rate2XTrendWd}。
有關大宗商品{commodity1Label}和{commodity2Label}基本上呈現{commodity12Adv}的趨勢。
{commodity1Label}目前{commodity1XTrendWd}，{commodity1Adv}{commodity2Label}{commodity2Adv}{commodity2XTrendWd}。
依據本週走勢分析，智能伏羲 AI推薦的產業為{mkt0Label} 。
接下來是我們獨家智能伏羲有關市場更新和預測。"""
		ts_footer="""最後我們總結一下下週預測， {mkt1Label}預期{mkt1NTrendWd}，{rate1Label}預期{rateNTrendWd}。由於這兩者呈{rhoWd} 相關關係，如果{rate1Label}價格進一步 {rateIfWd}，那麼，{mkt1Label}可能會{mkt1IfWd}{macro1Adv}"""
		ts_disclaim="\n以上名單，是經由國際金融中選出重要指標。此一名單，並不代表本公司的任何持有部位，謝謝您的收聽。"
		ts_stock="{label}在前一個交易日{xdTrendWd} {xdChgWd}，收盤價為{price}元。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
		ts_indicator="{label}在前個一交易日{xdTrendWd} {xdChgWd}，目前為{price} 。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
		#ts_stock="{label}在前一{unitStr}{pastTrendWd} {xwChgWd}，收盤價為{price}元{xdTrendWd}{xdChgWd} 。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
		#ts_indicator="{label}在前一{unitStr}{pastTrendWd} {xwChgWd}，目前為{price} {xdTrendWd}{xdChgWd}。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
		#ts_stock="{label}在前一{unitStr}{pastTrendWd} {xwChgWd}，目前收盤價{price}元 。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
		#ts_indicator="{label}在前一{unitStr}{pastTrendWd} {xwChgWd}，目前為{price} 。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
		ts_currency="{label}在前一{unitStr}{pastTrendWd} {xwBpsWd}分，目前為{price} 。其{pastTrendWd}{movingWd}部分{cmpWd} 過去一年歷史波動率{sigmaWd}。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
		ts_rate="{label}在前一{unitStr}{pastTrendWd} {xwBpsWd}基本點，目前為{price} 。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
		ts_macro="{label}在{vntDateWd}公佈，比前一{unitStr}{pastTrendWd} {xwDifWd}，目前為{price} 。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"

	f={}
	comment_content=''
	vcmt=[] # list of each comment
	tkLst=[] # list of tickers
	ct_ts={"stock":ts_stock,"macro":ts_macro,"rate":ts_rate,"currency":ts_currency,"indicator":ts_indicator}

		
	cmt_temp="SELECT cmt_temp_{} FROM mapping_cmt_temp WHERE category='{}' and cmt_type='{}' AND sub_id={}"
	# collect stock part
	(dm,datax,pgDB) = get_index_list(idxLst=None,pgDB=None,dbname=dbname,lang=lang)
	currdate=dm[0]['pbdate']
	for j,xd in enumerate(dm):
		ky=xd['category'].split()[-1]
		sqx=cmt_temp.format(lang,ky,"mkt",j%3)
		ts_cmt = get_ts_cmt(sqx,ts_rate,pgDB=pgDB,dbname=dbname)
		#ts_cmt = ct_ts[ky] if ky in ct_ts else ts_rate
		xcmt,tkS=create_stock_comment(f,j,xd,ts_cmt,pgDB=pgDB,lang=lang,mp3YN=mp3YN,ctky=ky)
		tkLst +=tkS
		vcmt.append(xcmt)
		comment_content = "\n".join([comment_content,xcmt])
	f['rho']= -0.333333
	f['macroXChg']= 0

	# collect macro part by category
	(dm,df,pgDB) = get_macro_list(currdate=currdate,pgDB=pgDB,dbname=dbname,lang=lang)
	flgAddi=0
	for k,xm in enumerate(dm):
		for j,xd in enumerate(xm):
			ky=xd['category'].split()[-1]
			sqx=cmt_temp.format(lang,ky,"mkt",2)
			ts_cmt = get_ts_cmt(sqx,ts_rate,pgDB=pgDB,dbname=dbname)
			#ts_cmt = ct_ts[ky] if ky in ct_ts else ts_rate 
			xcmt,flgAddi,tkS=create_macro_comment(f,j+1,xd,ts_cmt,flgAddi,pgDB=pgDB,lang=lang,mp3YN=mp3YN)
			tkLst +=tkS
			vcmt.append(xcmt)
			comment_content = "\n".join([comment_content,xcmt])
	if flgAddi>0:
		ts_header = headerAddiClause + ts_header
	currDateWd=ymd2md(currdate,ym="%B %d",ymd="%Y%m%d",lang=lang)
	comment_header = generate_cmt(f,ts=ts_header,lang=lang,funcname="generate_comment_header",mp3YN=mp3YN)
	comment_footer = generate_cmt(f,ts=ts_footer,lang=lang,funcname="generate_comment_footer",mp3YN=mp3YN)
	comment_title=ts_title.format(currDateWd)
	comment_disclaim=ts_disclaim
	vcmt=[comment_title,comment_header]+vcmt+[comment_footer,comment_disclaim]
	tcmt=prn_mkt_cmt(vcmt,region,lang,mp3YN,mp3Make,dirname)
	tkLst = ["','".join(tkLst[:3])]*2+tkLst
	open("mktCmt_US.list","w").write("\n".join(tkLst))
	return "\n".join(vcmt)

def opt_lsi_mkt(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:   list arguments, usually passed from sys.argv
		retParser:      OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class
	"""
	parser = OptionParser(usage="usage: %prog [option]", version="%prog 0.655",
		description="Create market commentary based on macro list" )
	parser.add_option("","--region",action="store",dest="region",default="US",
		help="region [TW|US] (default: US)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara",
		help="database name (default: ara)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host name (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="tablename",
		help="db tablename (default: None)")
	parser.add_option("","--dirname",action="store",dest="dirname",
		help="db dirname to save all mktCmt comments (default: None)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table write-mode [replace|append|fail] (default: replace)")
	parser.add_option("-l","--lang",action="store",dest="lang",default="cn",
		help="db language mode [cn|en] (default: cn)")
	parser.add_option("-n","--numlimit",action="store",dest="numLimit",default=7,type="int",
		help="max number of alert list (default: 7)")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no save to database (default: save to database)")
	parser.add_option("","--use_mp3",action="store_true",dest="mp3YN",default=False,
		help="comment use mp3 style")
	parser.add_option("","--make_mp3",action="store_true",dest="mp3Make",default=False,
		help="create mp3 file")
	parser.add_option("","--debug",action="store_true",dest="debugTF",default=False,
		help="debugging (default: False)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == "__main__" :
	(options, args)=opt_lsi_mkt(sys.argv)
	tkLst=None
	random.seed(int(datetime.datetime.today().strftime("%m%d")))
	ret = run_lsi_mkt(tkLst,**options)
	print >> sys.stdout, str(ret)
