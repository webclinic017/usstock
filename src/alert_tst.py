#!/usr/bin/env python
# -*- coding: utf-8 -*-
from lsi_daily import *
import datetime
import pandas as pd
from _alan_calc import conn2pgdb
from _alan_date import ymd_delta
from _alan_str import ymd2md
import sys

def get_alert_list(dbname='ara.tw',numLimit=10):
	fname='ohlc_alert_list.sql.tmp'
	pgDB=conn2pgdb(dbname=dbname)
	xqTmp=open(fname).read()
	cdate=pd.read_sql("SELECT * FROM ara_uptodate",pgDB).pbdate[0]
	x1dYmd=ymd_delta(cdate,1)
	x1wYmd=ymd_delta(cdate,7)
	xqr=xqTmp.format(**locals())
	df=pd.read_sql(xqr,pgDB)
	return (df,pgDB)

if __name__ == '__main__':
	args=sys.argv[1:]
	dbname='ara.tw' if len(args)<1 else args[0]
	numLimit=10 if len(args)<2 else args[1]
	(f,pgDB)=get_alert_list(dbname=dbname,numLimit=numLimit)
	aStr=", ".join(list(f['label']))
	lang="cn"
	numLimit=len(f['label'])
	print >> sys.stderr, aStr
	currDateWd=ymd2md(str(f['curr_date'][0]),lang=lang)
	ts_title='智能伏羲 {} 晨間綜合快報：今日值得觀察股票名單如下： {}。\n接下來播報所選個股評估内容：\n'
	ts_disclaim="\n以上名單，是經由一周内一百大熱絡交易股中選出。有關選股進出策咯在這兩天出現，并且回測的獲利為正向的前{}名，此一名單，並不代表本公司的任何持有部位，謝謝您的收聽。".format(numLimit)

	comment_title=ts_title.format(currDateWd,aStr)
	commentLst=f[['comment_pricing','comment_ohlc','comment_pppscf','comment_fcst']].sum(axis=1)
	print comment_title,"\n".join(commentLst),ts_disclaim
