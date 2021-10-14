#!/usr/bin/env python
'''
Get [CIK] info from [TICKER] via
http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner=exclude&action=getcompany&output=atom
Using feedparser
[feed] keys:
['addresses', 'links', 'address', 'updated', 'assigned-sic', 'id', 'updated_parsed', 'city', 'zip', 'author', 'title_detail', 'state', 'state-of-incorporation', 'conformed-name', 'state-location-href', 'street1', 'assigned-sic-desc', 'phone', 'assigned-sic-href', 'link', 'authors', 'author_detail', 'tk_ishare', 'cik-href', 'fiscal-year-end', 'cik', 'title', 'guidislink', 'company-info', 'assitant-director', 'state-location']

Last mod.:  Sat Sep  2 20:24:39 EDT 2017
Last mod.:  Thu Sep  7 10:46:26 EDT 2017
Last mod.:  Tue Sep 12 21:52:26 EDT 2017
Last mod.:  Fri Jan 25 14:38:22 EST 2019
by Ted
'''

import os,sys,time
#from urllib2 import urlopen
import feedparser
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData
from pandas.io import sql

def get_company_info(tkX):
	urx = 'http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner=exclude&action=getcompany&output=atom'
	d = feedparser.parse(urx.format(tkX))
	return d

def parse_feed(f):
#	vhd=[("cik","cik"),( "company","conformed-name"),( "yymm","fiscal-year-end")]
	vky={"tk_ishare":"tk_ishare","cik":"cik", "company":"conformed-name", "sic":"assigned-sic","yymm":"fiscal-year-end"}
	a={}
	#for (j,(xk,xv)) in enumerate(vhd) :
	for (j,(xk,xv)) in enumerate(vky.items()) :
		if f.has_key(xv):
			a[xk]=f[xv]
		else:
			a[xk]=''
		if xk in ["cik","yymm"] and len(a[xk]) :
			a[xk]=int(a[xk])
		elif xk in ["company"]:
			a[xk]=a[xk].upper()
	return a

def df_feed(tkLst):
	vf=[]
	for tkx in tkLst:
		print >> sys.stderr,"----- checking %s" % (tkx)
		d=get_company_info(tkx)
		if d.feed.has_key('cik') == False :
			continue 
		d.feed['tk_ishare']=tkx.upper()
		f=parse_feed(d.feed)
		vf.append(f)
		print >> sys.stderr,"\n".join([str(x) for x in f.items() ])
	if not vf :
		return vf
	sDF=pd.DataFrame(vf,columns=vf[0].keys())
	sDF=sDF[["tk_ishare","cik","company","sic","yymm"]]
	return sDF

def write2db(sDF,engine):
	if engine == None:
		return None
	print >> sys.stderr,repr(sDF)
	tbX='tk2cik_temp'
	sDF.to_sql(tbX, engine, index=False, schema='public', if_exists='replace')
	engine.dispose()

def tk2cik_main():
	engine=None
	engine = create_engine('postgresql://sfdbo:sfdbo0@localhost:5432/ara')
	if len(sys.argv) == 1:
		print >> sys.stderr,"\nRead from stdin\n\n"
		tkLst = sys.stdin.read().strip().split("\n")
	else:
		tkLst=sys.argv[1:]
	print >> sys.stderr,tkLst
	sDF = df_feed(tkLst)
	if len(sDF)>0  and len(sys.argv)<2:
		write2db(sDF,engine)

if __name__ == '__main__':
	tk2cik_main()

	# TBD, sDR = pd.read_sql("SELECT DISTINCT ticker FROM stock_ishare_temp ORDER BY ticker limit 10", con=engine)
	# TBD, used sql.execute(xql, con=engine)

