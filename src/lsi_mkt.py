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
	def generate_comment_header_cn
	def generate_comment_header_en
	def generate_comment_footer_cn
	def generate_comment_footer_en
	def generate_cmt
	def get_idxLst
	def get_highVolLst
	def get_rcntLst
	def mkt_list_x
	def get_mkt_list
	def create_macro_comment
	def create_stock_comment
	def str2gtts
	def prn_mkt_cmt
	def run_lsi_mkt
	def opt_lsi_mkt
    Version: 0.654
    Last mod., Fri Jul  6 15:09:44 EDT 2018
"""
from lsi_daily import *
import datetime
import pandas as pd
from _alan_calc import conn2pgdb
from _alan_date import ymd_delta
from _alan_str import ymd2md
from _alan_ohlc_fcs import convert_data_comment_fcst
import subprocess
import sys
reload(sys)

sys.setdefaultencoding('utf8')
import sys
reload(sys)

def generate_comment_header_cn(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="cn",mp3YN=False):
	""" macro market comment
		required 
		required ts fields: currency,indicator,rate,mkt1,mkt2
	"""
	if ts is None:
		return None
	for (ky,va) in f.iteritems(): 
		exec("{}=va".format(ky))
	#xsign = 1 if currencyTicker[-2:] == "US" else -1
	xsign = -1 # cn version always use EURO direction
	currencyXTrendWd=udfStr(xsign*currencyXChg,["貶值","升值","持平"],0.0001,lang=lang)
	indicatorXTrendWd=udfStr(indicatorXChg,udf=udfLst,zs=0.0001,lang=lang)
	rateXTrendWd=udfStr(rateXChg,udf=udfLst,zs=0.0001,lang=lang)
	mkt1XTrendWd=udfStr(mkt1XChg,udf=udfLst,zs=0.0001,lang=lang)
	mkt2XTrendWd=udfStr(mkt2XChg,udf=udfLst,zs=0.0001,lang=lang)
	vntDateWd=ymd2md(str(macroVntdate),ym="%B %d",lang=lang) if 'macroVntdate' in f else ""
	unitStr=fq2unit_str(macroMfreq,lang) if 'macroMfreq' in f else "月"
	pastTrendWd = udfStr(macroXChg,udfLst,0.0001,lang=lang)
	if indicatorXChg*currencyXChg*xsign<0 :
		macro1Adv='並且'
		macro11Adv='也在'
	else:
		macro1Adv='然而'
		macro11Adv='卻在'
	if mkt1XChg*mkt2XChg>0 :
		macro2Adv='也正'
	else:
		macro2Adv='卻正'
	dux = locals()
	ret=ts.format(**dux)
	return(ret)

def generate_comment_header_en(f,ts=None,dotSign='.',prcn=0,usdSign='$',udfLst=None,lang="en",mp3YN=False):
	""" macro market comment
		required 
		required ts fields: currency,indicator,rate,mkt1,mkt2
	"""
	if ts is None:
		return None
	for (ky,va) in f.iteritems(): 
		exec("{}=va".format(ky))
	xsign = 1 if currencyTicker[-2:] == "US" else -1
	currencyXTrendWd=udfStr(xsign*currencyXChg,["depreciated","appreciated","stayed flat"],0.0001)
	indicatorXTrendWd=udfStr(indicatorXChg,["increased","decreased","remained"],0.0001)
	rateXTrendWd=udfStr(rateXChg,["rose","fell","unchanged"],0.0001)
	mkt1XTrendWd=udfStr(mkt1XChg,["increased","decreased","remained"],0.0001)
	mkt2XTrendWd=udfStr(mkt2XChg,["went up","went down","stayed flat"],0.0001)
	vntDateWd=ymd2md(str(macroVntdate),ym="%B %d",lang=lang) if 'macroVntdate' in f else ""
	unitStr=fq2unit_str(macroMfreq,lang) if 'macroMfreq' in f else "month"
	pastTrendWd = udfStr(macroXChg,udfLst,0.0001,lang=lang)
	if indicatorXChg*currencyXChg>0 :
		macro1Adv='and'
		macro11Adv='also'
	else:
		macro1Adv='yet'
		macro11Adv=''
	if mkt1XChg*mkt2XChg>0 :
		macro2Adv='also'
	else:
		macro2Adv=''
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
	rateNTrendWd=udfStr(rateNChg,udf=udfLst,zs=0.0001,lang=lang)
	rhoWd=udfStr(rho,["正","負","獨立不存在"],0.05,lang=lang)
	rateIfWd=udfStr(rateNChg,udf=udfLst,zs=0.0001,lang=lang)
	mkt1IfWd=udfStr(rho*rateNChg,udf=udfLst,zs=0.000001,lang=lang)
	if rho*rateNChg*mkt1NChg<0:
		macro1Adv="，可能不如原始預期，必須持續觀察利率走向。"
	else:
		macro1Adv="，更勝於原有預期。"
	if abs(rho*rateNChg) <= 0.000001:
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
	mkt1NTrendWd=udfStr(mkt1NChg,["an increase of","a decrease of","no change in"],0.0001)
	rateNTrendWd=udfStr(rateNChg,["a rise of","a fall of","no change in"],0.0001)
	rhoWd=udfStr(rho,["positively","negatively","almost not"],0.05)
	rateIfWd=udfStr(rateNChg,["goes up","goes down","stays flat"],0.0001)
	mkt1IfWd=udfStr(rho*rateNChg,["rise","fall","stay flat"],0.000001)
	if rho*rateNChg*mkt1NChg<0:
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

def get_macro_fcs(tkLst=[],pgDB=None,dbname='ara',lang="en"):
	ext= "" if lang=="en" else "_"+lang
	pgDB=conn2pgdb(dbname=dbname)
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

def get_highVolLst(tkLst=[],pgDB=None,dbname='ara'):
	""" 2. highVolLst: top-3 most volatile globalmacro 
	"""
	return (tkLst,pgDB)

def get_rcntLst(tkLst=[],pgDB=None,dbname='ara'):
	""" 3. rcntLst: recent updated globalmacro index (within one-week for non-daily)
	"""
	return (tkLst,pgDB)

def mkt_list_x(j,datax,currdate=None,pgDB=None):
	xqr="category_seq=={}".format(j)
	df=datax.query(xqr)
	if j==1:
		df=df[~df['ticker'].isin(["^DJI","^SOX","^IXIC","^GSPC"])]
	if j==2:
		df=df.query("ticker!='DEXUSAL'")
	elif j==3:
		df=df[df['ticker'].str.contains("DGS")]
	elif j==4:
		try:
			vntLst=pd.read_sql("select * from macro_vintage_date where series in ('UNRATE','SPCS20RSA','A939RX0Q048SBEA','CPIAUCNS','PPIACO','HPIPONM226S','UMCSENT') ORDER BY vntdate DESC",con=pgDB)
		except Exception,e:
			print  >> sys.stderr, "**ERROR @ {}():{},DB:{}".format("mkt_list_x",str(e),pgDB)
			return None
		vntx=vntLst.iloc[0]
		x7d=int(ymd_delta(currdate,days=7))
		if int(vntx['vntdate'])>=x7d:
			df=df[df['ticker'].str.contains(vntx['series'])]
		else:
			return None
	if(len(df)<1):
		return None
	dy=df.loc[int(df.zx.abs().sort_values().index[-1])].to_dict()
	if j==4:
		dy['vntdate']=vntLst['vntdate'].iloc[0]
	return dy

def get_mkt_list(dbname='ara',lang="en",currdate=None):
	ext= "" if lang=="en" else "_"+lang
	pgDB=conn2pgdb(dbname=dbname)
	sqr="SELECT p.*,m.freq as mfreq,m.label{0} as label, m.category,m.category_seq FROM macro_fcs p LEFT JOIN mapping_series_label m ON p.ticker=m.series ORDER BY m.category_seq".format(ext)
	tmpx = pd.read_sql(sqr,con=pgDB)
	datax = tmpx.query("(freq=='W' & mfreq=='D') | (freq=='D' & mfreq!='D')").copy()
	datax['zx']=(datax.prc_chg/datax.rrate_sigma)
	print >> sys.stderr, datax.head(2)
	print >> sys.stderr, datax.tail(3)
	dx=[]
	for j in (datax.category_seq.unique()) :
		#if j>3: continue
		dy=mkt_list_x(j,datax,currdate=currdate,pgDB=pgDB)
		if dy is None or len(dy)<1:
			continue
		dx.append(dy)
	#print >> sys.stderr, dx
	return (dx,datax,pgDB)

def create_macro_comment(f,xd,ts,flgAddi,pgDB=None,lang="cn",mp3YN=True):
	tkLst=[]
	ticker=xd["ticker"]
	ky=xd['category'].split()[-1]
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
	print >> sys.stderr, xcmt
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
	print >> sys.stderr, xcmt
	return (xcmt,tkLst)

def str2gtts(pfx,xstr,lang="cn"):
	glang="zh-tw" if lang=="cn" else "en"
	fname=pfx.replace("_mp3","").replace(".txt","")+".mp3"
	xcmd="python /usr/local/bin/gtts-cli.py -l {} - | sox -G -t mp3 - {} tempo 1.25".format(glang,fname)
	print >> sys.stderr, xcmd
	p = subprocess.Popen(xcmd,shell=True,bufsize=1024,stdout=subprocess.PIPE,stdin=subprocess.PIPE)
	p.stdin.write(xstr)
	p.communicate()[0]
	p.stdin.close()

def prn_mkt_cmt(vcmt,region,lang,mp3YN,mp3Make=False,dirname=None):
	mtype="mp3" if mp3YN is True else ""
	pfx="_".join(filter(None,["mktCmt",region,lang,mtype]))
	for j,xcmt in enumerate(vcmt):
		if dirname is None or os.path.exists(dirname) is not True:
			continue
		fn="{}/{}_{}.txt".format(dirname,pfx,j)
		fp=open(fn,"w")
		fp.write(xcmt)
		fp.close() 
		if mp3YN is True and mp3Make is True:
			str2gtts(fn,xcmt,lang)
	if dirname is not None and os.path.exists(dirname) is True:
		fn="{}/{}.txt".format(dirname,pfx)
		xcmt="\n".join(vcmt)
		fp=open(fn,"w")
		fp.write(xcmt)
		fp.close() 
	if mp3YN is True and mp3Make is True:
		str2gtts(fn,xcmt,lang)
	return xcmt

def run_lsi_mkt(tkLst,pgDB=None,**kwargs):
	""" Create market commentary based on macro list
	"""
	for ky,va in kwargs.items():
		exec("{}=va".format(ky))

	if lang=="en":
		ts_title='{} market Closing Report ：brought to you by Beyondbond.'
		headerAddiClause="{macroLabel} published at {vntDateWd}, {pastTrendWd} from last {unitStr}."
		ts_header="""{indicatorLabel} {indicatorXTrendWd}, while the {currencyLabel} {currencyXTrendWd} against the US dollar, {macro1Adv} the {rateLabel} {macro11Adv} {rateXTrendWd}.  In the stock market, {mkt1Label} {mkt1XTrendWd}, while the {mkt2Label} {macro2Adv} {mkt2XTrendWd}.  Our AI recommended sector is {mkt0Label}.
	Now, here is our exclusive AICaas market updates and forecasts.
	"""
		ts_footer="""Finally, our forecast calls for {mkt1NTrendWd} {mkt1Label} and {rateNTrendWd} {rateLabel} next week. As these two are {rhoWd} correlated, if the {rateLabel} {rateIfWd} further, then the {mkt1Label} is likely to {mkt1IfWd} {macro1Adv}."""
		ts_disclaim="""This electronic message is our opinion only and is not intended to be an investment advise."""
		ts_stock=ts_indicator=ts_currency=ts_rate=ts_macro=None
	else:
		ts_title='智能伏羲 {} 晨間大盤走勢快報：今日值得觀察國際金融走勢如下\n'
		headerAddiClause="{macroLabel}在{vntDateWd}公佈，比前一{unitStr}{pastTrendWd} 。"
		ts_header="""{indicatorLabel}在日前{indicatorXTrendWd}，{macro1Adv}，{currencyLabel}{macro11Adv}{currencyXTrendWd}。與此同時，{rateLabel}{macro2Adv}處於{rateXTrendWd}狀態。在股市中，隨者{mkt1Label}的{mkt1XTrendWd}，{mkt2Label}{macro2Adv}{mkt2XTrendWd} 。我們AI推薦的產業為{mkt0Label} 。
接下來是我們獨家智能伏羲有關市場更新和預測。"""
		ts_footer="""最後我們總結一下下週預測， {mkt1Label}預期{mkt1NTrendWd}，{rateLabel}預期{rateNTrendWd}。由於這兩者呈{rhoWd} 相關關係，如果{rateLabel}價格進一步 {rateIfWd}，那麼，{mkt1Label}可能會{mkt1IfWd}{macro1Adv}"""
		ts_disclaim="\n以上名單，是經由國際金融中選出重要指標。此一名單，並不代表本公司的任何持有部位，謝謝您的收聽。"
		ts_stock="{label}在前一{unitStr}{pastTrendWd} {xwChgWd}，收盤價為{price}元{xdTrendWd}{xdChgWd} 。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
		ts_indicator="{label}在前一{unitStr}{pastTrendWd} {xwChgWd}，目前為{price} {xdTrendWd}{xdChgWd}。依據{label}波動狀況，預估下一{unitStr}有七成可能{rangeWd}。{posPbWd}"
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

	# collect stock part
	mcLst=["^DJI","^SOX","^IXIC","^GSPC"]
	(dm,datam,pgDB)=get_macro_fcs(mcLst,pgDB=pgDB,lang=lang)
	(dmo,datax,pgDB)=get_ohlc_fcs(None,pgDB=pgDB,lang=lang)
	dm = dmo+dm
	currdate=datax['pbdate'].iloc[0]
	for j,xd in enumerate(dm):
		ky=xd['category'].split()[-1]
		ts_cmt = ct_ts[ky] if ky in ct_ts else ts_rate # use ts_rate as default
		xcmt,tkS=create_stock_comment(f,j,xd,ts_cmt,pgDB=pgDB,lang=lang,mp3YN=mp3YN,ctky=ky)
		tkLst +=tkS
		vcmt.append(xcmt)
		comment_content = "\n".join([comment_content,xcmt])
	f['rho']= -0.333333
	f['macroXChg']= 0

	# collect macro part by category
	(dm,df,pgDB) = get_mkt_list(dbname=dbname,lang=lang,currdate=currdate)
	flgAddi=0
	for j,xd in enumerate(dm):
		ky=xd['category'].split()[-1]
		ts_cmt = ct_ts[ky] if ky in ct_ts else ts_rate # use ts_rate as default
		xcmt,flgAddi,tkS=create_macro_comment(f,xd,ts_cmt,flgAddi,pgDB=pgDB,lang=lang,mp3YN=mp3YN)
		tkLst +=tkS
		vcmt.append(xcmt)
		comment_content = "\n".join([comment_content,xcmt])
	if flgAddi>0:
		ts_header = headerAddiClause + ts_header
	currDateWd=ymd2md(str(datax['pbdate'].iloc[0]),ym="%B %d",lang=lang)
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
	parser = OptionParser(usage="usage: %prog [option]", version="%prog 0.65",
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
	parser.add_option("-i","--use_pipe",action="store_true",dest="pipeYN",default=False,
		help="use stdin from pipe")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == "__main__" :
	(options, args)=opt_lsi_mkt(sys.argv)
	tkLst=None
	ret = run_lsi_mkt(tkLst,**options)
	print >> sys.stdout, ret
