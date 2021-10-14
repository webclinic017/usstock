#!/usr/bin/env python 
""" Create macro attribute variable hist from [ara]::[macro_hist_fred]
    and save them into
    ara_macro_hist_curve
    ara_macro_hist_inflation
    ara_macro_hist_volatility
    respectively
    should be part of cronjob after [macro_hist_fred] 
    Last mod., Sat Dec  9 16:58:27 EST 2017
"""
import sys
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd

def macro_attribute_hist(fromSql,tbx,engine):
	ret = pd.read_sql(fromSql,con=engine) 
	df=ret.pivot_table(index='pbdate',columns='series',values='value')
	df.columns=[x.lower() for x in df.columns]
	df.to_sql(tbx, engine, schema='public', index=True, if_exists='replace')
	print >> sys.stderr, df.tail()
	print >> sys.stderr, "data saved to table:[{}]".format(tbx)
	return df

def run_ara_macro_hist(dbURL='postgresql://sfdbo@localhost:5432/ara'):
	# Connect to DB
	engine = create_engine(dbURL)

	# Assign Macro Attribute Variable List
	attrLst={
		"volatility": ["vixcls","vxeemcls","vxvcls","vxxlecls"],
		"curve": ["dgs1mo","dgs3mo","dgs6mo","dgs1","dgs2","dgs3","dgs5","dgs7","dgs10","dgs30"],
		"inflation": ["dcoilwtico","goldpmgbd228nlbm","t5yifr"]}
	sqTmp="SELECT series,value,pbdate FROM macro_hist_fred WHERE pbdate>20101231 AND series SIMILAR TO '({})'"
	for (ky,va) in attrLst.iteritems() :
		#print >> sys.stderr, ky,va
		tableName='ara_macro_hist_{}'.format(ky)
		vaStr="|".join(va).upper()
		sqr = sqTmp.format(vaStr)
		print >> sys.stderr, sqr
		try:
			df=macro_attribute_hist(sqr,tableName,engine)
			print >> sys.stderr, '{}[{}] is ready.'.format(tableName,len(df))
		except:
			print >> sys.stderr, "***ERROR:", sys.exc_info()[1]
			print >> sys.stderr, 'No update for {}'.format(tableName)
			continue
	return engine
	# Close DB

if __name__ == '__main__':
	if len(sys.argv) == 1:
		dbName='ara'
        else:
                dbName=sys.argv[1]
	dbURL='postgresql://sfdbo@localhost:5432/{}'.format(dbName)
	print >> sys.stderr, 'USE DB:{}'.format(dbName)
	# Create tables [ara_macro_hist_volatility] [ara_macro_hist_curve] [ara_macro_hist_inflation]
	pgDB=run_ara_macro_hist(dbURL=dbURL)

	# Create table [macro_fred_d_hist]
	xqr="SELECT f.* FROM macro_hist_fred f, mapping_series_label m WHERE f.series=m.series AND m.freq='D' ORDER BY f.Series,f.pbDate"
	tablename="macro_fred_d_hist"
	macro_attribute_hist(xqr,tablename,pgDB)

	pgDB.dispose()
