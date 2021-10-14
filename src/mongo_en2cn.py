#!/usr/bin/env python3
'''  RSS news title translator
Program: mongo_en2cn.py

Description:
Read mongoDB data and translate 'field'  from 'src' to 'dest' (default: "en" -> "zh-tw")

Usage of :
python3 mongo_en2cn.py

Example:
# For multiple stock news translation via select 'ticker' and tanslate 'field' up-to  'limit' items
python3 -c "from mongo_en2cn import mongo_en2cn as me;me({'ticker':'AAPL'},tablename='rssCompany',field={'title','title_cn','summary','summary_cn'},sort=[('pubDate',-1)],limit=5,debugTF=True)"
# OR
# For specified news/item translation in 'rssCompany' 
python3 -c "_id='5ec2c26ae0a744292ef523ff';from mongo_en2cn import ObjectId,mongo_en2cn as me;me({'_id':ObjectId(_id)},tablename='rssCompany',field={'title','title_cn','summary','summary_cn'},debugTF=True)"
# OR
# For specified news/item translation  in 'rssNews'
python3 -c "_id='5ec540e484ec217c0a25d1c4';from mongo_en2cn import ObjectId,mongo_en2cn as me;me({'_id':ObjectId(_id)},tablename='rssNews',field={'title','title_cn'},debugTF=True)"

# OR
python3 -c "_id='5ecaeedf8b6c152c9dd901ce';from mongo_en2cn import ObjectId,mongo_en2cn as me;me({'_id':ObjectId(_id)},tablename='rssNews',field={'summary','summary_cn'},debugTF=True)"

Last Mod., Fri May 22 16:07:53 EDT 2020
------------------------------------------------------------------------------
'''
import sys
sys.path.append('/apps/fafa/pyx/alan')
from _alan_calc import getKeyVal
from gtranslate_en2cn import en2cn
import time
import re

def remove_tags(raw_html):
	''' remove HTML tags
	'''
	cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
	cleantext = re.sub(cleanr, '', raw_html)
	return cleantext

def prn_dict(file=sys.stderr,**optx):
	for ky,val in optx.items():
		print( " {}: {}".format(ky,val) ,file=file)

def title2cn(incr,debugTF=False,src='en',dest='zh-TW',**dd):
	newDd={}
	kyLst=list(dd)
	for ky,val in dd.items():
		try:
			if ky+'_cn' in kyLst or ky[-3:]=='_cn':
				continue
			if not isinstance(val,str) or len(val)<1:
				continue
			newKy="{}_cn".format(ky)
			if newKy in dd and dd[newKy] is not None and len(dd[newKy])>0:
				continue
			if incr['count']%60==0:
				print("==Sleep 60 sec @:{}\n".format(incr['count']),file=sys.stderr)
				time.sleep(60)
			val = remove_tags(val)
			newVal = en2cn(val,src=src,dest=dest)
			if debugTF:
				print("==TRANSLATING {}:{}->\n{}".format(ky,val,newVal),file=sys.stderr)
				print("==Num. times:{}\n".format(incr['count']),file=sys.stderr)
			incr['count'] += 1
			if len(newVal)<1:
				continue
			newDd.update({newKy:newVal})
		except Exception as e:
			print("**ERROR title2cn: {} @ {}".format(str(e),val),file=sys.stderr)
	return newDd

from pymongo import MongoClient
from bson.objectid import ObjectId
def mongo_en2cn(jobj={},limit=0,sort=[],field={},debugTF=False,src='en',dest='zh-TW',**optx):
	'''
	RSS news title translator
	Find data via 'jobj' of 'dbname':'tablename' of 'field'
	Then translate 'field' excepnt 'field'_cn  from 'src' to 'dest'
	Note: any field has value in 'field'_cn won't be re-translated
	'''
	hostname,port,dbname,tablename = getKeyVal(optx,
		['hostname','port','dbname','tablename'], ['localhost',27017,'ara','rssNews'])
	def upd_data(currM,incr,**myquery):
		myquery.pop("_id",None)
		newSet = title2cn(incr,debugTF=debugTF,src=src,dest=dest,**myquery)
		mysetting = { "$set": newSet }
		if len(newSet)<1:
			print("===NOTHING TO UPDATE {}\n".format(myquery),file=sys.stderr)
			return myquery
		print("===UPDATING From:{}:\nTo:{}\nVia:{}\n".format(myquery,newSet,currM),file=sys.stderr)
		xout = currM.update_many(myquery, mysetting)
		if debugTF:
			print(" --updated out[{}]:{}\n".format(xout.modified_count,xout.raw_result),file=sys.stderr)
		return newSet

	clientM=optx.pop('clientM',None)
	if not clientM:
		dbM=MongoClient("{}:{}".format(hostname,port))[dbname]
	else:
		dbM=clientM[dbname]
	currM=dbM[tablename]
	if debugTF:
		print(dbM,currM,file=sys.stderr)

	# DO NOT TRANSLATE, AD not used 
	jobj.update({"source.title":{"$nin":["Insider Monkey","TipRanks"]}})

	dLst=list(currM.find(jobj,field,sort=sort,limit=limit))
	if len(dLst)<1:
		return []
	incr={"count":1}
	oLst=[]
	for j,dd in enumerate(dLst):
		try:
			print("===INPUT[{}]: {} : {}\n".format(j,jobj,dd),file=sys.stderr)
			ret = upd_data(currM,incr,**dd)
			ret.update(jobj)
			oLst.append(ret)
		except Exception as e:
			print("**ERROR:{} mongo_en2cn: {} @ {}".format(j,str(e),dd),file=sys.stderr)

	if debugTF:
		prn_dict(field=field,sort=sort,limit=limit,**optx,file=sys.stderr)
	return oLst

from _alan_str import find_mdb, upsert_mdb
# DEPRECATED
def mongo_upd_tst(jobj={},limit=0,sortLst={},field={},dfTF=False,debugTF=False,**optx):
	hostname,port,dbname,tablename = getKeyVal(optx,
		['hostname','port','dbname','tablename'], ['localhost',27017,'ara','rssNews'])
	data,_,_=find_mdb(jobj,tablename=tablename,dbname=dbname,field=field,sortLst=sortLst,limit=limit,dfTF=dfTF)
	if len(data)<1:
		return None
	if debugTF is True:
		prn_dict(field=field,sortLst=sortLst,limit=limit,**optx)
	return data

if __name__ == '__main__':
	#mongo_upd_tst(tablename='rssNews',field={"title"},sortLst={'pubDate'},limit=2,debugTF=True)
	args=sys.argv[1:]
	limit=50 if len(args)<1 else int(args[0])
	tablename='rssNews' if len(args)<2 else args[1]
	print("===Translating mongo_en2cn:{} items from {}".format(limit,tablename),file=sys.stderr)
	mongo_en2cn(tablename=tablename,field={"title","title_cn"},sort=[('pubDate',-1)],limit=limit,debugTF=True)
