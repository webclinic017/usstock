#!/usr/bin/env python
""" 

a supplement run to get earnings report from Nasdaq Calendar
and save info to table: [earnings_nasdaq]
Ref: https://www.nasdaq.com/earnings/earnings-calendar.aspx?date=2019-01-16
https://finance.yahoo.com/calendar/economic
Usage of:
python eps_nasdaq.py --date=20190116
Also see: macro_event_yh.py
Last Mod., Sat Jan 26 13:44:43 EST 2019
	
"""
import sys
from optparse import OptionParser
import numpy as np
import requests
import pandas as pd
import re
from datetime import datetime
#from sqlalchemy import create_engine, MetaData
from pymongo import MongoClient
from _alan_str import write2mdb

def get_sector_list(xdate,url=None):
	if xdate is None:
		xdate = datetime.today().strftime('%Y-%m-%d')
	elif xdate.isdigit() and len(xdate)==8:
		d = xdate
		xdate = '{}-{}-{}'.format(d[:4],d[4:6],d[-2:])
	elif '/' in xdate:
		xdate.replace('/','-')
	if url==None:
		url="https://api.nasdaq.com/api/calendar/earnings?date={date}".format(date=xdate)
	try:
		res=requests.get(url)
		jtmp= res.json()
		df = pd.DataFrame(jtmp['data']['rows'])
	except Exception as e:
		print >> sys.stderr, "**ERROR: {}".format(str(e))
		print >> sys.stderr, "to pull info from \n{}\n...".format(url)
		return {}
	return df

def adjust_nasdaq(dg):
	vtmp = [ ''.join(re.split('\$',s)[1:]).replace('B','*1000000000').replace('M','*1000000')  for s in dg['Company Name (Symbol) Market Cap Sort by: Name / Size']]
	dg['marketCap'] = [ eval(s) if s!='' else 0 for s in vtmp]
	if 'EPS' in dg:
		dg['actualEPS'] = [np.nan if 'n/a' in s else float(s[1:]) for s in dg['EPS'].values]
	if 'Consensus EPS* Forecast' in dg:
		dg['estimatedEPS'] = [np.nan if 'n/a' in s else float(s[1:]) for s in dg['Consensus EPS* Forecast'].values]
	dg['suprise'] = [np.nan if 'n/a' in s else float(s[1:]) for s in dg['Consensus EPS* Forecast'].values]
	if 'estimatedEPS' in dg and 'actualEPS' in dg:
		dg['pchg'] = dg[['estimatedEPS','actualEPS']].pct_change(axis=1).iloc[:,1]
	return dg

def run_eps_nasdaq(dbM,url=None,xdate=None,saveDB=True,tablename=None,wmode='replace'):
	""" find EPS report via calendar date YYYY-MM-DD
	"""
	dv=get_sector_list(xdate,url=url)
	if len(dv)<1:
		return None
	dg=pd.DataFrame(dv)
	dfyc=adjust_nasdaq(dg)
	
	if any([saveDB is False, tablename is None]):
		print >> sys.stderr, dfyc.to_csv(sep="|")
	else:
		zpk = {'ticker','pbdate'} 
		mobj,dbM,err_msg = write2mdb(dfyc,dbM,dbname=dbname,tablename=tablename,zpk=zpk)
		#dfyc.to_sql(tablename,pgDB,index=False,schema='public',if_exists=wmode)
		print >> sys.stderr, "{}\n...\n{}\n saved to {}:{}".format(dfyc.head(1),dfyc.tail(1),dbM,tablename)
	return dfyc

def opt_eps_nasdaq(argv,retParser=False):
	parser = OptionParser(usage="usage: %prog [option]", version="%prog 0.1",
		description="get up-to-date macro info from yahoo finance")
	parser.add_option("","--url",action="store",dest="url",
		help="url (default: None)")
	parser.add_option("","--date",action="store",dest="xdate",
		help="yyyymmdd (default: Today)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara",
		help="database (default: ara)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="hostname (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="tablename",default="earnings_nasdaq",
		help="db tablename (default: earnings_nasdaq)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table write-mode [replace|append|fail] (default: replace)")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no save to database (default: save to database)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	opt,args =opt_eps_nasdaq(sys.argv)
	for (ky,va) in opt.iteritems():
		exec("{}=va".format(ky))
	#pgDB = create_engine('postgresql://sfdbo@{}:5432/{}'.format(hostname,dbname))
	dbM=MongoClient("{}:27017".format(hostname))[dbname]
	run_eps_nasdaq(dbM,url,xdate,saveDB,tablename,wmode)
