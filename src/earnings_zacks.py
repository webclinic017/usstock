#!/usr/bin/env python3
# -*- coding: utf-8 -*-
''' zacks eps earnings reports
Tables used: earnings_zacks, earnings_upcomings
Usage of:
#for date 2020-02-24 save to earnings_zacks
python3 earnings_zacks.py --start=20200224
#OR for the next day and save to earnings_upcomings
python3 earnings_zacks.py --days=1 --table=earnings_upcomings
#OR for the next 7 days and save to earnings_upcomings (on Friday EoD)
python3 -c "from earnings_zacks import earnings_upcomings_batch as eub;eub()"
Note, zacks calendar use epoch date internally

Last mods, Tue Oct  6 12:19:28 EDT 2020
'''
import json,pandas as pd
import sys
import re
import datetime
from _alan_optparse import parse_opt, subDict
from _alan_date import next_date
from _alan_str import upsert_mdb,strc2float
import pandas as pd
import requests
if sys.version_info.major == 2:
	reload(sys)
	sys.setdefaultencoding('utf8')

from bs4 import BeautifulSoup as bsp
def process_data(d):
	kItm=d.items()
	for ky,va in kItm:
		if not isinstance(va,str):
			continue
		vb = bsp(va,'lxml').get_text().replace('...','')
		if ky in ['marketCap','Estimate','Reported']:
			vb = strc2float(vb,sc=',%')
		d.update({ky:vb})
	if all([ ky is not None for ky in [d['Reported'],d['Estimate']] ]):
		d['ESP'] = d['Reported']-d['Estimate']
		d['pChg'] = d['Reported']/d['Estimate']-1 if abs(d['Estimate'])>0 else None
	else:
		d.pop('ESP',None)
		d.pop('pChg',None)
	return d

def earnings_zacks(start=0,days=0,debugTF=False,saveDB=True,dbname='ara',tablename='earnings_zacks',zpk=['ticker','pbdate'],**optx):
	if debugTF:
		sys.stderr.write("=== earnings_zacks() locals:{}\n".format(locals()))
	if days>100: 
		days = 0 
	ctime = next_date(start,days=days,endOfDay=True)
	epochy= ctime.strftime('%s')
	pbdate= int(ctime.strftime('%Y%m%d'))
	urx='https://www.zacks.com/includes/classes/z2_class_calendarfunctions_data.php?calltype=eventscal&date={}&type=1&search_trigger=0'
	url = urx.format(epochy)
	headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
	jd = requests.Session().get(url,headers=headers)
	if jd.status_code != 200:
		sys.stderr.write("**ERROR: HTTP code:{}\n".format(jd.status_code))
		return {}
	ret =  jd.json()
	datax = ret['data']
	sys.stderr.write("===URL:\n{}\n".format(url))
	if debugTF:
		sys.stderr.write("===JSON:\n{}\n".format(datax[:5]))
	hdr=['web','company','marketCap','Time','Estimate','Reported','ESP','pChg','Report','href']
	dd=[]
	for j,x in enumerate(datax):
		try:
			if debugTF and j<5:
				sys.stderr.write("==Running:{}:{}\n".format(j,x))
			dx = dict(zip(hdr,datax[j]))
			d = process_data(dx)

			## parse web for ticker
			#xdu = re.search('rel="(\w+[.]?\w?)"',d['web'])
			#if xdu is not None and len(xdu.groups()[0])>0:
			#	tkX = xdu.group(1)
			#	d.update(ticker=tkX)
			#else:
			#	continue

			## parse pChg
			#xdu = re.search('">([+-]?\d*[.]?\d*%)', d['pChg'])
			#if xdu is not None and len(xdu.groups()[0])>0:
			#	pChg =xdu.groups()[0]
			#	d.update(pChg=pChg)

			## parse company
			#xdu = re.search('title="(.*.)"',d['company'])
			#if xdu is not None and len(xdu.groups()[0])>0:
			#	company =xdu.groups()[0]
			#	d.update(company=company)

			#for xc in ['marketCap','Estimate','Reported'] :
			#	if xc in d:
			#		d[xc] = strc2float(d[xc])

			d.update(ticker=d['web'])
			d.update(pbdate=pbdate)
			d.pop('Report',None)
			d.pop('web',None)
			d.pop('href',None)
			if debugTF and j<5:
				sys.stderr.write("==Running:{}:{}\n".format(j,d))
			dd.append(d)
		except Exception as e:
			sys.stderr.write("**ERROR:{} {} of\n{}\n".format(j,str(e),x))
	df = dd
	if not saveDB:
		return df
	# Save MDB ara::earnings_zacks
	try:
		if not tablename:
			tablename='earnings_zacks'
		mobj,dbM,err_msg = upsert_mdb(df,dbname=dbname,tablename=tablename,zpk=zpk)
		sys.stderr.write("==Update to MDB:{}::{} ({} records)\n{}\n".format(dbname,tablename,len(df),df))
	except Exception as e:
		sys.stderr.write("**ERROR:{} {} of\n{}\n".format(j,str(e),x))
	return df

def earnings_upcomings_batch(start=0,dayLst=[1,2,3,4,5,6,7],debugTF=True,saveDB=True,dbname='ara',tablename='earnings_upcomings',zpk=['ticker','pbdate'],**optx):
	''' pull the next 7 days and save to earnings_upcomings
	'''
	# To redict and adjust parent function args+**optx to **kwags for the child function 
	kwargs=locals().copy()
	kwargs.pop('optx',None)
	kwargs.update(**optx)
	kwargs.pop('dayLst',None)
	for days in dayLst:
		kwargs.update(days=days)
		if debugTF:
			sys.stderr.write("=== earnings_zacks() args:{}\n".format(kwargs))
		try:
			df = earnings_zacks(**kwargs)
		except Exception as e:
			sys.stderr.write("**ERROR:{} of\n{}\n".format(str(e),days))
			continue
	return df

if __name__ == '__main__':
	opts, args = parse_opt(sys.argv)
	df = earnings_zacks(**opts)
	if len(df)>0:
		df=pd.DataFrame(df)
		print(df) 
