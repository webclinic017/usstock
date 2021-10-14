#!/usr/bin/env python
""" a supplement run to update yield curve info
    and save info to table: [yc_temp] and [macro_temp_yc]
    ref_uri: https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx
    Usage of:
	ppython yc_current.py -d $dbname
    Note: prefer to run following commands afterward:
	sed 's/temp_fred/temp_yc/g' fred2hist.sql | /apps/fafa/bin/psql.sh -d $dbname
	/apps/fafa/bin/psql.sh -d $dbname < yc2hist.sql
    last mod., Fri May 25 10:11:36 EDT 2018
"""
import sys,os
from optparse import OptionParser
import pandas as pd
from sqlalchemy import create_engine, MetaData
from datetime import datetime
import pandas as pd

def get_yc(uri=None):
	if uri==None:
		uri='https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yield'
	print >> sys.stderr, "Pulling info from \n{}\n...".format(uri)
	classname="t-chart"
	df=pd.read_html(uri,attrs={"class":classname},index_col=0,header=0)[0]
	yyyymmdd=int(datetime.strptime(df.index.values[-1],"%m/%d/%y").strftime("%Y%m%d"))
	yc=[]
	for x,y in zip(df.columns.values,df.iloc[-1].values):
		v=str(x).split()
		m=int(v[0])*(12 if v[1].lower()=='yr' else 1)
		s="DGS"+v[0]+("MO" if m<12 else "")
		t="TSY_"+v[0]+("_mo" if m<12 else "_yr")
		dx=dict(yc_type="TSY",ticker=t,symbol=s,mo2mat=m,pbdate=yyyymmdd,value=y)
		yc.append(dx)
	if len(yc)<1:
		print >> sys.stderr, "No data pulled from {}".format(uri)
		return None
	print >> sys.stderr, yc[-2:]
	dfyc=pd.DataFrame(yc)
	dfyc=dfyc[["yc_type","ticker","mo2mat","value","pbdate","symbol"]]
	return dfyc

def run_yc_current(pgDB=None,uri=None,saveDB=True,tablename="yc_temp",table2="macro_temp_yc",wmode='replace'):
	""" find lastest treasury curve
	    usage of run_yc_current(pgDB) 
	"""
	dfyc=get_yc(uri)
	if dfyc is None:
		return dyfc
	if saveDB is True and pgDB is not None:
		dfyc.to_sql(tablename,pgDB,index=False,schema='public',if_exists=wmode)
		dfgm=dfyc[["symbol","value","pbdate"]].rename(columns={"symbol":"series"})
		dfgm.to_sql(table2,pgDB,index=False,schema='public',if_exists=wmode)
	else:
		xreq=os.getenv('REQUEST_METHOD')
		if xreq in ('GET','POST') : # WEB MODE
			print "Content-type:application/json;charset=utf-8\r\n"
		print >> sys.stdout, dfyc.to_json(orient="records")
	return dfyc

def opt_yc_current(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:	list arguments, usually passed from sys.argv
		retParser:	OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class 
	"""
	parser = OptionParser(usage="usage: %prog [option]", version="%prog 0.65",
		description="Pull up-to-date yield curve from www.treasury.gov")
	parser.add_option("","--uri",action="store",dest="uri",
		help="uri (default: https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yield)")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara",
		help="database (default: ara)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="tablename",default="yc_temp",
		help="db tablename (default: yc_temp)")
	parser.add_option("","--table2",action="store",dest="table2",default="macro_temp_yc",
		help="db additional table (default: macro_temp_yc)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table write-mode [replace|append] (default: replace)")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no save to database (default: save to database)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	opt,args =opt_yc_current(sys.argv)
	for (ky,va) in opt.iteritems():
		exec("{}=va".format(ky))
	pbDB = create_engine('postgresql://sfdbo@{}:5432/{}'.format(hostname,dbname))
	run_yc_current(pbDB,uri,saveDB,tablename,table2,wmode)
