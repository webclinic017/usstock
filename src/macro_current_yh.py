#!/usr/bin/env python
""" a supplement run to patch FRED commodity prices for missing up-to-date info
    and save info to table: [macro_current_yh] and [macro_temp_yc]
    ref_uri: https://finance.yahoo.com/commodities/
    Usage of:
	python macro_current_yh.py -d ara
"""
import sys
from optparse import OptionParser
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, MetaData
from datetime import datetime

def get_sector_list(sector='commodities',uri=None):
	if uri==None:
		uri="https://finance.yahoo.com/{}/".format(sector)
	print >> sys.stderr, "Pulling info from \n{}\n...".format(uri)
	df=pd.read_html(uri,attrs={"class":"yfinlist-table W(100%) BdB Bdc($tableBorderGray)"},index_col=0,header=0)[0]
	dv=[]
	Ymd=int(datetime.now().strftime("%Y%m%d"))
	hdr=("ticker","label","sector","value","change","chg_pct","pbdate")
	n=len(df)
	for xv in zip(df.index,df.Name,[sector]*n,df['Last Price'],df['Change'],df['% Change'],[Ymd]*n):
		dx=dict( zip(hdr,xv) )
		dx['value']= float(dx['value'])
		if not isinstance(dx['change'], (np.integer, int, long, float, complex)):
			dx['change']= float(dx['change'].replace(',',''))
		dx['chg_pct']= float(dx['chg_pct'].strip('%'))
		dv.append(dx)
	print >> sys.stderr, dv[-2:]
	return dv

def run_macro_current_yh(pgDB=None,uri=None,saveDB=True,tablename="macro_current_yh",wmode='replace'):
	""" find lastest treasury curve
	    usage of run_yc_current(pgDB) 
	"""
	dv=get_sector_list("commodities")
	dv+=get_sector_list("currencies")
	dv+=get_sector_list("bonds")
	dv+=get_sector_list("world-indices")
	df=pd.DataFrame(dv)
	hdr=["ticker","label","sector","value","change","chg_pct","pbdate"]
	df['value'] = df['value'].astype(float)
	df['change'] = df['change'].astype(float)
	dfyc=df[hdr]
	
	if any([saveDB is False, pgDB is None, tablename is None]):
		print >> sys.stderr, dfyc
	else:
		dfyc.to_sql(tablename,pgDB,index=False,schema='public',if_exists=wmode)
		print >> sys.stderr, "{}\n...\n{}\n saved to {}:{}".format(dfyc.head(1),dfyc.tail(1),pbDB,tablename)
	return dfyc

def opt_macro_current_yh(argv,retParser=False):
	parser = OptionParser(usage="usage: %prog [option]", version="%prog 0.1",
		description="get up-to-date macro info from yahoo finance")
	parser.add_option("","--uri",action="store",dest="uri",
		help="uri (default: https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yield)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara",
		help="database (default: ara)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="tablename",default="macro_current_yh",
		help="db tablename (default: macro_current_yh)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table write-mode [replace|append|fail] (default: replace)")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no save to database (default: save to database)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	opt,args =opt_macro_current_yh(sys.argv)
	for (ky,va) in opt.iteritems():
		exec("{}=va".format(ky))
	pbDB = create_engine('postgresql://sfdbo@{}:5432/{}'.format(hostname,dbname))
	run_macro_current_yh(pbDB,uri,saveDB,tablename,wmode)
