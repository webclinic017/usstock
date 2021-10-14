#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Tables required: mongoDB::ara::headline_hist, ara:record_hilo 
Usage of:
# simulation run
headline_writer.py --lang=cn --extra_xs=nlookback=30 --debug
# OR onTheFly run 
headline_writer.py --lang=cn --extra_xs='onTheFly=True;dirname="templates"'
# OR onTheFly run with specific script
headline_writer.py --lang=cn --extra_xs='onTheFly=True;j2ts="{% include \"daily_headline.j2\" %}";dirname="templates"' --start=20190705
# OR onTheFly run with specific script (most common)
headline_writer.py --lang=cn --extra_xs='onTheFly=True;j2ts="{% include \"daily_briefing_cn.j2\" %}";dirname="templates"' --start=20190705 2>/dev/null

# sample record from headline_hist
#dd={'close1': 2892.74, 'chgPct3': 0.59, 'chgLevel3': 47, 'Index3': '^IXIC', 'chgPct2': 0.15, 'UpDn2': 'UP', 'close2': 26425, 'topDate2': 20190405, 'close3': 7938.69, 'chgLevel1': 13, 'chgLevel2': 40, 'close4': 1477.65, 'topUD1': 'UP', 'Index4': '^SOX', 'topUD2': 'UP', 'UpDn3': 'UP', 'chgLevel4': 12, 'pbdate': 20190405, 'topLst2': "[{'AMGN': 1.6}, {'NVDA': 1.43}, {'CVX': 1.3}]", 'topDate1': 20190405, 'chgPct1': 0.46, 'Index2': '^DJI', 'conflict': 0, 'chgPct4': 0.8, 'UpDn1': 'UP', 'UpDn4': 'UP', 'topLst1': "[{'AMGN': 1.6}, {'NVDA': 1.43}, {'CVX': 1.3}]", 'Index1': '^GSPC'}
'''

import sys
import ast
#from _alan_calc import sqlQuery
from _alan_date import next_date
from _alan_str import jj_fmt, find_mdb, write2mdb, popenCall, jj_fmt
from _alan_optparse import parse_opt, subDict
from ticker2label import ticker2label
from iex_peers import iex_peers

if sys.version_info.major == 2:
        reload(sys)
        sys.setdefaultencoding('utf8')
import json

def get_hiloRecord(ticker,pbdate):
	from _alan_str import find_mdb
	ret,_,_=find_mdb({"ticker":ticker,"pbdate":pbdate},tablename="record_hilo",dbname="ara")
	if len(ret)<1:
		hiloRecord={}
	else:
		hiloRecord=ret[0]['YTD'] if len(ret[0]['YTD'])>0 else {}
	return hiloRecord
	
def create_headline(dd,ts='',jobj=None,**opts):
	dd['topLst1']=ast.literal_eval(dd['topLst1']) if hasattr(dd['topLst1'], "__len__") else {}
	dd['topLst2']=ast.literal_eval(dd['topLst2']) if hasattr(dd['topLst2'], "__len__") else {}
	if jobj is None:
		dd['hiloRecord'] = get_hiloRecord(dd['Index1'],dd['pbdate'])
	else:
		dd['hiloRecord'] = jobj['YTD'] if jobj['YTD'] else {}
	hiloName = dd['hiloRecord']['name'] if 'name' in dd['hiloRecord'] else ''
	sys.stderr.write("{}|{}|".format(dd['pbdate'],hiloName))
	dd.update(pcall=popenCall,ticker2label=ticker2label,iex_peers=iex_peers,jj_fmt=jj_fmt)
	ret = jj_fmt(ts,dd,**opts)
	opts.update(mp3YN=True)
	dbname='ara';tablename='mkt_briefing_details'
	mp3ret = jj_fmt(ts,dd,**opts)
	block='HEADLLINE';attr='EOD'
	cdt=next_date()
	datax=dict(block=block,attr=attr,comment=ret,mp3comment=mp3ret,pbdt=cdt,data={})
	for k,v in dd.items():
		if not hasattr(v,'__call__'):
			 datax['data'].update({k:v})
	write2mdb(datax,dbname=dbname,tablename=tablename)
	return ret

def headline_writer(opts={},**optx):
	if not opts:
		opts, args = parse_opt(sys.argv)
	if optx:
		opts.update(optx)
	lang = opts['lang']
	nlookback = int(opts['nlookback'])
	#xqr="SELECT * FROM headline_hist ORDER BY pbdate DESC limit {}".format(nlookback)
	#dj=sqlQuery(xqr).to_dict(orient='records')
	dj,_,_=find_mdb({},tablename="headline_hist",dbname="ara",sortLst=['pbdate'],limit=nlookback)
	ts="""
	{%- set xcase = 2 if chgLevel2>200 or chgLevel2<-200 else 1 -%}
	{%- set headline_file='daily_headline_{}_{}.j2'.format(xcase,lang) -%}
	{%- include headline_file -%}
	"""

	for dd in dj:
		try:
			ret = create_headline(dd,ts=ts,**opts)
			sys.stderr.write("{}\n".format(ret))
		except Exception as e:
			sys.stderr.write("{}:{}".format(dd['pbdate'],str(e)))
			continue

	
def generate_headline(opts={},**optx):
	if not opts:
		opts, args = parse_opt(sys.argv)
	if optx:
		opts.update(optx)
	debugTF = opts['debugTF'] if 'debugTF' in opts else False
	from headline_sts import headline_hist
	from record_hilo import record_hilo_tst
	if 'nlookback' not in opts:
		nlookback=1
	else:
		nlookback = max(1,int(opts['nlookback']))
	lang = opts['lang']
	ndays=nlookback+1
	if debugTF:
		sys.stderr.write("{}\n{}\n".format("===generate_headline():opts",opts))
	if 'j2ts' in opts and opts['j2ts'] is not None and len(opts['j2ts'])>0:
		ts=opts['j2ts']
		del opts['j2ts']
	elif 'j2name' in opts and len(opts['j2name'])>0:
		fj2name = opts['j2name'].replace('.j2','')+'.j2'
		ts='{} include "{}" {}'.format('{%',fj2name,'%}')
		del opts['j2name']
	else:
		ts="""
		{%- set xcase = 2 if chgLevel2>200 or chgLevel2<-200 else 1 -%}
		{%- set headline_file='daily_headline_{}_{}.j2'.format(xcase,lang) -%}
		{%- include headline_file -%}
		"""
	start = opts['start'] if 'end' in opts else None
	df = headline_hist(ndays=ndays,saveDB=False,end=start)
	jobj = record_hilo_tst(opts) # hilo report
	if df.shape[0]<1:
		return ''
	dd = df.iloc[0].to_dict() # headline data output
	ret = create_headline(dd,ts=ts,jobj=jobj,**opts)
	if debugTF:
		sys.stderr.write("{}\n{}\n{}\n{}\n".format("===generate_headline():Comment,HeadlineData,HiLoData",ret,dd,jobj))
	return ret

if __name__ == '__main__':
	description="""Write daily closing comment, e.g., headline_writer.py SPY --extra_xs=nlookback=10;onTheFly=True;"""
	opts, args = parse_opt(sys.argv, description=description)
	if 'nlookback' not in opts:
		opts['nlookback'] = 1
	if 'onTheFly' in opts and opts['onTheFly'] is True:
		# TBD, in the fly run
		print generate_headline(opts)
	else:
		print headline_writer(opts)

