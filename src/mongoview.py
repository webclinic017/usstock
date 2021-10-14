#!/usr/bin/env python3
''' View mongo data
Usage of,
mongoview [-h] [--host HOSTNAME] [-p PORT] [-d DBNAME]
  [--output OUTPUT] [-f FIELDS] [--sep SEP] [--limit LIMIT]
  [--debug] [-q QUERYS] [-s SORTS] collection
e.g.,
mongoview yh_quote_curr -f ticker,close,epochs --debug --query='{"ticker":{"$in":["AAPL","AMZN"]}}'
OR
mongoview yh_quote_curr -f ticker,close,epochs --debug --query="{'ticker':{'\$in':['AAPL','AMZN']}}"
'''
import sys
import pandas as pd
import json 
import ast 
from pymongo import MongoClient
from argparse import ArgumentParser

def parse_args(description='View mongo data',nargs='+'):
	description += ", e.g., {} {}".format(sys.argv[0],"""yh_quote_curr -f ticker,close,epochs""")
	parser = ArgumentParser(description=description)
	parser.add_argument(action="store", nargs=nargs,dest='collections', help="collection name, e.g. yh_quote_curr")
	parser.add_argument('--host',action='store',dest='hostname',default='localhost', help='host name (default: localhost)')
	parser.add_argument('-p','--port',action='store',dest='port',default=27017,type=int, help='port number (default: 27017)')
	parser.add_argument('-d','--db',action='store',dest='dbname',default='ara', help='database name, default: ara')
	parser.add_argument('--output',action='store',dest='output', default='csv',help='output-type of [csv|json] default: csv')
	parser.add_argument('-f','--fields',action='store',dest='fields', help='comma separated list of field names (required for CSV output) e.g. -f "ticker,close,pbdt"')
	parser.add_argument('--sep',action='store',dest='sep', default='|',help='separator, default to "|"')
	parser.add_argument('--limit',action='store',default=10,dest='limit',type=int,help='limit rows default to 10; note, -1 for unlimit.' )
	parser.add_argument('--debug',action='store_true',default=False,dest='debugTF',help='debug mode' )
	parser.add_argument('--noHeaderline',action='store_false',default=True,dest='headerTF',help='ColumnHeader mode for "csv" output type' )
	parser.add_argument('-q', '--query',action='store',dest='queryS', help='query, e.g. -q \'{"ticker":{"$in":["AAPL","AMZN"]}}\'')
	parser.add_argument('--queryFile',action='store',dest='queryF', help='query filename in JSON format')
	parser.add_argument('-s', '--sort',action='store',dest='sortS', help='JSON sort, e.g. -s \'[("pbdt",-1)]\'')
	args=parser.parse_args()
	opts=vars(args)
	opts['sep']=opts['sep'].encode().decode('unicode_escape') if sys.version_info.major==3 else opts['sep'].decode('string_escape')
	return opts

def mongoview():
	opts=parse_args()
	host='{hostname}:{port}'.format(**opts)
	dbname=opts['dbname']
	collectname=opts['collections'][0]
	output=opts['output']
	sep=opts['sep']
	limit=opts['limit']
	debugTF=opts['debugTF']
	if debugTF:
		sys.stderr.write("OPTS: {}\n".format(opts))
	mdb=MongoClient(host)[dbname]
	mCur=mdb[collectname]
	queryObj={}
	sortObj=[('_id',1)]
	dspObj={'_id':0}
	if opts['queryS'] is not None:
		queryObj=ast.literal_eval(opts['queryS'])
	elif opts['queryF'] is not None:
		try:
			xq=open(opts['queryF']).read()
			queryObj=ast.literal_eval(xq)
		except Exception as e:
			sys.stderr.write(" --Warning:{} @ {}\n".format(str(e),"query"))
	if opts['sortS'] is not None:
		sortObj =ast.literal_eval(opts['sortS'])
	if opts['fields'] is not None:
		du = {x:1 for x in opts['fields'].split(',') if x.strip()  }
		dspObj.update(du)
	if debugTF:
		sys.stderr.write("Query:{}\nDisplay:{}\nSort:{}\n".format(queryObj,dspObj,sortObj))
	if limit>0:
		d=mCur.find(queryObj,dspObj,sort=sortObj).limit(limit)
	else:
		d=mCur.find(queryObj,dspObj,sort=sortObj)
	d=list(d)
	if debugTF:
		sys.stderr.write("Out:{}\n".format(d))

	#---------------------------------------------------------------------------
	# OUTPUT
	# For output type 'json'
	if output.lower()=='json':
		#import datetime
		#dtCvt = lambda x: x.__str__() if isinstance(x, datetime.datetime) else x
		#sys.stdout.write("{}\n".format(json.dumps(list(d),default=dtCvt)))
		return d
	#
	# For output type other than 'json'
	df=pd.DataFrame(d)
	if debugTF:
		sys.stderr.write(" --fields:{}:{}\n".format(opts['fields'],df.columns))
	if opts['fields'] is not None:
		xcol = [x for x in opts['fields'].split(',') if x in df.columns ]
		df = df[xcol]
	#
	# Print output to stdout
	sys.stdout.write(df.to_csv(index=False,sep=sep,header=opts['headerTF']))
	return df

if __name__ == '__main__':
	try:
		mongoview()
	except Exception as e:
		sys.stderr.write('**ERROR: {}\n'.format(str(e)))
