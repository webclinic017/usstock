#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Program: lsi_alert.py
    Description: Create alert commentary based on top-7 TTR performance list
    Input table required: ohlc_daily_comment_cn_mp3, mapping_ticker_cik
    Example:
	python lsi_alert.py --region=TW -d ara.tw -n 7
	python lsi_alert.py --region=US -d ara -n 7
	printf "AAPL\nIBM" | python lsi_alert.py --region=US -d ara --use_pipe
    Version: 0.64
"""
from lsi_daily import *
import datetime
import pandas as pd
from _alan_calc import conn2pgdb
from _alan_date import ymd_delta
from _alan_str import ymd2md
import sys
reload(sys)

sys.setdefaultencoding('utf8')

def get_select_list(dbname='ara.tw',tkLst=None):
	pgDB=conn2pgdb(dbname=dbname)
	xqTmp="SELECT b.company_cn as label,a.* FROM (SELECT * FROM ohlc_daily_comment_cn_mp3 WHERE ticker in ({})) as a LEFT JOIN mapping_ticker_cik b ON a.ticker=b.ticker ORDER BY a.trr"
	xqr=xqTmp.format( "'{}'".format("','".join(tkLst)) )
	df=pd.read_sql(xqr,pgDB)
	return (df,pgDB)

def get_alert_list(dbname='ara.tw',numLimit=10):
	fname='ohlc_alert_list.sql.tmp'
	pgDB=conn2pgdb(dbname=dbname)
	xqTmp=open(fname).read()
	cdate=pd.read_sql("SELECT * FROM ara_uptodate",pgDB).pbdate[0]
	x1dYmd=pd.read_sql("SELECT pbdate FROM ohlc_daily_comment_cn GROUP BY pbdate ORDER BY pbdate DESC limit 2",pgDB).iloc[1][0]
	#x1dYmd=ymd_delta(cdate,1)
	x1wYmd=ymd_delta(cdate,7)
	xqr=xqTmp.format(**locals())
	df=pd.read_sql(xqr,pgDB)
	print >> sys.stderr, df
	return (df,pgDB)

def run_lsi_alert(tkLst,pgDB=None,**kwargs):
	for ky,va in kwargs.items():
		exec("{}=va".format(ky))
	print >> sys.stderr, "region={};dbname={};numLimit={};lang={}".format(region,dbname,numLimit,lang)

	if tkLst is None:
		(f,pgDB)=get_alert_list(dbname=dbname,numLimit=numLimit)
		numLimit=len(f['label'])
		ts_title='智能伏羲 {} 晨間綜合快報：今日值得觀察股票名單如下： {}。\n接下來播報所選個股評估内容：\n'
		ts_disclaim="\n以上名單，是經由一周内一百大熱絡交易股中選出。有關選股進出策咯在這兩天出現，并且回測的獲利為正向的前{}名，此一名單，並不代表本公司的任何持有部位，謝謝您的收聽。"
	else:
		(f,pgDB)=get_select_list(dbname=dbname,tkLst=tkLst)
		numLimit=len(f['label'])
		ts_title='智能伏羲 {} 綜合快報：今日您的觀察股票名單如下： {}。\n接下來播報所選個股評估内容：\n'
		ts_disclaim="\n以上名單，是經由您的觀察股中選出。有關選股進出策咯在這兩天出現的前{}名，此一名單，並不代表本公司的任何持有部位，謝謝您的收聽。"

	if region.upper() == "US":
		aTmp=f[['label','ticker']].apply(lambda x:'{}代號{}'.format(*x),axis=1)
	else :
		aTmp=f[['label']].apply(lambda x:'{}'.format(*x),axis=1)
	aStr=", ".join(list(aTmp))
	print >> sys.stderr, aStr
	currDateWd=ymd2md(str(f['curr_date'][0]),lang=lang)

	comment_title=ts_title.format(currDateWd,aStr)
	comment_disclaim=ts_disclaim.format(numLimit)
	commentLst=f[['comment_pricing','comment_ohlc','comment_pppscf','comment_fcst']].sum(axis=1)
	xstr = comment_title+"\n".join(commentLst)+comment_disclaim
	return xstr

def opt_lsi_alert(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:   list arguments, usually passed from sys.argv
		retParser:      OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class
	"""
	parser = OptionParser(usage="usage: %prog [option] SYMBOL1 ...", version="%prog 0.64",
		description="Create alert commentary based on top-7 TTR performance list" )
	parser.add_option("","--region",action="store",dest="region",default="TW",
		help="region [TW|US] (default: TW)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara.tw",
		help="database name (default: ara.tw)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host name (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="tablename",
		help="db tablename (default: None)")
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
	parser.add_option("-i","--use_pipe",action="store_true",dest="pipeYN",default=False,
		help="use stdin from pipe")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == "__main__" :
	(options, args)=opt_lsi_alert(sys.argv)
	if options['pipeYN'] is True:
		print >> sys.stderr,"\nRead from pipe\n\n"
		tkLst = sys.stdin.read().strip().split("\n")
	elif len(args)>0:
		tkLst=args
	else:
		tkLst=None
	ret = run_lsi_alert(tkLst,**options)
	print ret
