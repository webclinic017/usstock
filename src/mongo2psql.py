#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Convert mongodb ara_tw::hourly_report to postgresql ara.tw::hourly_report
Usage of:
python mongo2psql.py hourly_report ara.tw
'''

import sys
import datetime
import re
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
from pandas.io.json import json_normalize
import json
from bson import json_util
if sys.version_info.major == 2:
	reload(sys)
	sys.setdefaultencoding('utf8')

def mongo2psql(tbName,dbName,engine=None,clientMg=None):
	uri='mongodb://localhost:27017'
	mdbName=dbName.replace('.','_')
	if clientMg is None:
		clientMg=MongoClient(uri)
	connMg=clientMg[mdbName]
	currMg=connMg[tbName]

	vLst=list(currMg.find())
	"""
	for j,xv in enumerate(vLst):
		#print(j,xv)
		for ky,va in xv.items():
			if isinstance(va,list):
				#print(j,ky)
				vLst[j][ky]=json.dumps(va,default=json_util.default)
	df=pd.DataFrame(json_normalize(vLst))
	"""
	df=pd.DataFrame(vLst)
	if '_id' in df.columns:
		df = df.drop('_id', 1)
	cmn=[]
	for x in df.columns:
		rx = '[' + re.escape(''.join('()#%|*,:/ ')) + ']'
		x = re.sub(rx, '', x)
		cmn.append(x)
	df.columns=cmn
	print(df.columns)
	#print(df.head(2).to_csv(sep="|"))
	#print(df.tail(2).to_csv(sep="|",header=False))
	try:
		if engine is None:
			engine = create_engine('postgresql://sfdbo@localhost:5432/{}'.format(dbName))
		#engine.execute('DROP TABLE IF EXISTS "{}"'.format(tbName))
		print(engine,dbName,tbName,file=sys.stderr)
		df.to_sql(tbName, engine, schema='public', index=False, if_exists='replace')
	except Exception as e:
		print(str(e),file=sys.stderr)
	return df

if __name__ == '__main__':
	args=sys.argv[1:]
	dbName='ara.tw'
	tbName='hourly_report'
	if len(args)>0:
		tbName=args[0]
	if len(args)>1:
		dbName=args[1]
	df = mongo2psql(tbName,dbName)
