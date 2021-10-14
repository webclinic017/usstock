#!/usr/bin/env python
''' Get lastest vintagedates from FRED site
printf "select series,freq from mapping_series_label where source='fred' and freq<>'D' ORDER BY freq" | /apps/fafa/bin/psql.sh -d ara -At | python macro_vintage_date.py -d ara
    Last mod., Tue May 22 14:35:46 EDT 2018
'''
import datetime
import sys
import pandas as pd
from optparse import OptionParser
from sqlalchemy import create_engine

def get_fred_vintagedates(series_id,file_type='json',api_key='c795a86b73f40e7b3dca282948645b83'):
	''' Get series from fred.gov site
	    Package required:
		import pandas
	    e.g.,
		content=get_fred_vintagedates('DGS10')
	    args:
		 series_id:  string as FRED series ID, e.g., "DEXCHUS", "DGS30"
		 file_type: json|xml
		 api_key: fred.gov api key
	    return:
		 latest release date (vindate) in dataframe
	'''
	#urx='https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type={file_type}'
	urx='https://api.stlouisfed.org/fred/series/vintagedates?series_id={series_id}&api_key={api_key}&file_type={file_type}'
	uri=urx.format(**locals())
	d=pd.read_json(uri)
	return d

def macro_vintage_date(tkLst,args):
	for (ky,va) in args.iteritems():
		exec("{}=va".format(ky))

	dx=[]
	vh=("freq","vntdate","series")
	for tkX in tkLst:
		if '|' in tkX:
			(tkX,freq) = tkX.split('|')
			if freq=='D':
				continue
		tkX=tkX.replace('_PCTCHG','')
		d=get_fred_vintagedates(tkX)
		vd=(freq,str(d['vintage_dates'].iloc[-1]).replace('-',''),tkX)
		xdc=dict(zip(vh,vd))
		dx.append(xdc)
	df=pd.DataFrame(dx)
	if saveDB is False:
		return df
	dbURL='postgresql://sfdbo@{}:5432/{}'.format(hostname,dbname)
	engine = create_engine(dbURL)
	df.to_sql(tablename,engine,schema='public',index=False,if_exists=wmode)
	print >> sys.stderr, "save info into {} in {}".format(tablename,engine)
	return df

def opt_macro_vintage_date(argv,retParser=False):
	parser = OptionParser(usage="usage: %prog [option] SYMBOL1 ...", version="%prog 0.1",
		description="Download lastest globalmacro vintagedates from FRED")
	parser.add_option("-d","--database",action="store",dest="dbname",default="ara",
		help="database (default: ara)")
	parser.add_option("","--host",action="store",dest="hostname",default="localhost",
		help="db host (default: localhost)")
	parser.add_option("-t","--table",action="store",dest="tablename",default="macro_vintage_date",
		help="db tablename (default: macro_vintage_date)")
	parser.add_option("-w","--wmode",action="store",dest="wmode",default="replace",
		help="db table write-mode [replace|append] (default: replace)")
	parser.add_option("","--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no save to database (default: save to database)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	(options, args)=opt_macro_vintage_date(sys.argv)
	tkLst = args if len(args)>0 else sys.stdin.read().strip().split("\n")
	try:
		df=macro_vintage_date(tkLst,options)
		print >> sys.stderr, df
	except Exception,e:
		print  >> sys.stderr, "***ERROR:",str(e) 

