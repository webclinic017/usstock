#!/usr/bin/env python3
'''
主題概念股
Pull holding tickers  from fintel site +
Process ticker list + daily quote and ytd performance
for 'topic' selection  based on large fund performance
MDB: topic_fintel_hist, topic_theme_majorplayer
Also see, 
topic_theme_media.py 
MDB: madmoney_screener, madmoney_hist, topic_theme_media

#Usage of:
topic_theme_majorplayer.py 1 2020-09-30
#then
topic_theme_majorplayer.py 2 2020-09-30
#Next update:2020-10-15

Prev mod., Wed Oct 23 15:15:11 EDT 2019
Prev mod., Tue Jan 14 17:09:25 EST 2020
Prev mod., Sat May 16 18:23:38 EDT 2020
Last mod., Thu Sep 10 12:18:38 EDT 2020
'''

import sys
import pandas as pd
import requests
from io import StringIO
import re
from _alan_str import upsert_mdb
from _alan_calc import renameDict,subDF
import pickle

def df2pickle(df={},prefix='',ext='.pickle3',dirname='pickle'):
	if len(df)<1 or len(prefix)<1:
		return False
	filepath = "{dirname}/{prefix}{ext}".format(**locals())
	fp = open(filepath,"wb")
	pickle.dump(df,fp)
	fp.close()
	return True

# DEPRECAPTED
def get_holdingschannel(urlName=''):
	if len(urlName)<1:
		urlName='https://m.holdingschannel.com/13f/citadel-advisors-llc-top-holdings/'
	#urlName='https://m.holdingschannel.com/13f/renaissance-technologies-llc-top-holdings/'
	ret = pd.read_html(urlName)[0]
	sys.stderr.write("{}\n".format(ret.head()))
	return ret


# DEPRECAPTED
def mthd_bpc(ret):
	''' remove Bond,Put,Call rows
	'''
	ret.columns=['ticker','amount','chgPos','position']
	vx=[]
	for j,(x,y) in enumerate(zip(ret.iloc[:,0].values,ret.iloc[:,1].values)):
		if x not in ['Call','Put','Bond']:
			vx.append(j)
	df=ret.iloc[vx,:].dropna(subset=['amount'])
	df.reset_index(drop=True,inplace=True)
	return df

# DEPRECAPTED
def mthd_dup(ret,np=3):
	''' remove duplicate
	'''
	ret.columns=['ticker','amount','chgPos','position']
	x0=''
	vx=[]
	for j,x in enumerate(ret.iloc[:,np]):
		if x==x0:
			vx.append(j)
		x0=x
	sys.stderr.write("{} {}\n".format("+++++",vx))
	df=ret.iloc[vx,:]
	df.reset_index(drop=True,inplace=True)
	return df

# DEPRECAPTED
def process_holdingschannel(urlName='',mthdName=''):
	sys.stderr.write("=====URL:{}\n-----MTHD:{}\n".format(urlName,mthdName))
	mthdName = mthdName if mthdName in globals() else 'mthd_bpc'
	funcArg = globals()[mthdName]
	sys.stderr.write("=====URL:{}\n-----MTHD:{}\n".format(urlName,funcArg))
	ret = get_holdingschannel(urlName=urlName)
	df = funcArg(ret)
	for clx in df.columns[2:4]:
		df.loc[:,clx] = df[clx].apply(lambda x:x.replace(',','').replace('$','') if isinstance(x,str) else x).values.astype(float)
	sys.stderr.write("{}\n".format(df.head()))
	return df

# DEPRECAPTED
def batch_holdingschannel():
	urlName='https://m.holdingschannel.com/13f/citadel-advisors-llc-top-holdings/'
	umLst = [ ['renaissance-technologies-llc','mthd_dup'],
		['citadel-advisors-llc','mthd_bpc'] ] 
	for (u,m) in umLst:
		urlName="".join(['https://m.holdingschannel.com/13f/',u,'-top-holdings/'])
		mthdName=m
		df = process_holdingschannel(urlName=urlName,mthdName=mthdName)
		df['fund']=u
		print(df.head(10).to_string())

# note 403 may appear, if overtry)
def get_fintel(fund='',dats='2020-03-31',sn=0,tn=1,debugTF=False):
	if len(fund)<1:
		return {}
	urlName='https://fintel.io/i13f/{}/{}-{}'.format(fund,dats,sn)
	headers={'Content-Type': 'text/html', 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
	sys.stderr.write("==PULLING {} from {}\n".format(fund,urlName))
	ret=requests.Session().get(urlName,headers=headers)
	#ret=requests.get(urlName)
	if ret.status_code!=200:
		sys.stderr.write("**ERROR: No Data!\n{}\n{}\n".format(urlName,ret))
		return {}
	dx = pd.read_html(StringIO(ret.text))[tn]
	if debugTF:
		sys.stderr.write("{}\n".format(dx.head()))
	print(type(dx))
	return dx

def read_fintel(fund='',dirname='pickle',funddate=0,ext='pickle3',sep='\t',debugTF=False):
	if len(fund)<1 or funddate<12345678:
		return {}
	#fpath="{dirname}/i13f_{fund}.{funddate}{ext}".format(**locals())
	fpath="{dirname}/{fund}.{ext}".format(**locals())
	sys.stderr.write("==Loading file from:{} @{}\n".format(fpath,'read_fintel'))
	#dx = pd.read_csv(fpath,sep=sep)
	try:
		dx = pickle.load(open(fpath,'rb'))
	except Exception as e:
		sys.stderr.write("**ERROR:{} @{}\n".format(str(e),'read_fintel'))
		dx={}
	return dx

def strc2float(s):
	try:
		return float(s.replace(',',''))
	except:
		return s

def process_fintel(fund='',dx={},debugTF=False):
	if len(dx)<1 or len(fund)<1:
		return {}
	dx['ticker'] = [x.split('/')[0].strip() for x in dx['Security']]
	dx['fund']=fund
	dx.columns=[ re.sub("[():.\s\xa0\xc2]","",xs) for xs in dx.columns]
	if 'Unnamed1' in dx:
	    dx = dx.drop(['Unnamed1'], axis=1)
	for c in ['PrevShares','CurrentShares','PrevValueUSDx1000','CurrentValueUSDx1000']:
		if c in dx:
			dx[c] = dx[c].apply(strc2float)
	dx.rename(columns={'ChangePercent':'SharesChangePercent','ChangePercent1':'ValueChangePercent'},inplace=True)
	df=dx.sort_values(by='CurrentValueUSDx1000',ascending=False)
	df.reset_index(drop=True,inplace=True)
	tsum=df['CurrentValueUSDx1000'].sum()
	df['percentPos']=df['CurrentValueUSDx1000']/tsum
	return df

def run_fintel(fund='',dats='2020-06-30',sn=0,tn=1,zpk=['fund','ticker','funddate'],saveDB=False,debugTF=False,dbname='ara',tablename='topic_fintel_hist'):
	if len(fund)<1:
		return {}
	if debugTF:
		sys.stderr.write("===fund:{},dats:{},sn:{},tn:{}\n".format(fund,dats,sn,tn))
	#dx = get_fintel(fund=fund,dats=dats,sn=sn,tn=tn,debugTF=debugTF)
	funddate=int(dats.replace('-',''))
	dx = read_fintel(fund=fund,funddate=funddate,debugTF=debugTF)
	if len(dx)<1:
		return {}
	if debugTF:
		sys.stderr.write("===dx:\n{}\n".format(dx))
	df = process_fintel(fund,dx,debugTF=debugTF)
	df['funddate']=int(dats.replace('-',''))
	df2pickle(df,prefix=fund)
	if len(df)<1:
		return {}
	if debugTF:
		sys.stderr.write("===df:\n{}\n".format(df.head()))
	if saveDB:
		ret = upsert_mdb(df,zpk=zpk,dbname=dbname,tablename=tablename)
	return df

# caseX_3
from _alan_str import find_mdb
def get_fund_quote(jobj={},clientM=None,**optx):
	'''
	Get first set data: data1 based on jobj={"fund":'fundName'}
	then based on ticker list: 'tkLst' and combine 'data1' with quote data 
	base on mergerOn=['ticker'] and mergeHow='left' to merge
	'''
	if len(jobj)<1:
		return {},clientM,'jobj is {}',0 
	limit=optx.pop('limit',100)
	dbname=optx.pop('dbname',None)
	tablename=optx.pop('tablename','')
	#
	# get first set data: data1
	data0,clientM,err = find_mdb(jobj,clientM,dbname=dbname,tablename=tablename,sortLst={"funddate"},limit=1,dfTF=False)
	if len(data0)<1:
		return {},clientM,"wrong fundName",0
	mxdate=data0[0]['funddate']	
	jobj.update({"funddate":mxdate})
	sortLst={'CurrentValueUSDx1000'}
	data1,clientM,err = find_mdb(jobj,clientM,dbname=dbname,tablename=tablename,sortLst=sortLst,limit=100,dfTF=True)
	if len(data1)<1:
		return {},clientM,"no data found",mxdate
	tkLst=list(data1['ticker'])
	#
	# Get ticker list: 'tkLst' and combine 'data1' with quote data 'data2' on 'left' merge
	mergeOn=optx.pop('mergeOn',['ticker'])
	mergeHow=optx.pop('mergeHow','left')
	kobj={"ticker":{"$in":tkLst}}
	data2,clientM,err = find_mdb(kobj,clientM,tablename='yh_quote_curr',dfTF=True)
	data2['pbdt']=data2['pbdt'].astype(object).where(data2['pbdt'].notnull(), None)
	data1 = data1.merge(data2,on=mergeOn,how=mergeHow)
	return data1,clientM,err,mxdate

# caseX_2
def batch_fintel_majorplayer(fundLst=[],dbname='ara',tablename='topic_fintel_hist',themename='topic_theme_majorplayer',saveDB=False,debugTF=False):
	''' Process ticker list + daily quote and ytd performance
	'''
	if len(fundLst)<1:
		fundLst=['berkshire-hathaway','blackrock','bridgewater-associates-lp',
			'goldman-sachs-group','renaissance-technologies-llc']
	zpk=["fund","ticker","funddate"]
	colX=['fund','funddate','pbdate','pbdt','ticker','shortName','change','changePercent','sector_cn','company_cn','marketCap','close','peRatio','CurrentShares','percentPos','SharesChangePercent','fiftyTwoWeekRange']
	clientM=None
	for fn in fundLst:
		sys.stderr.write("==GET fund:{}\n".format(fn))
		jobj={"fund":fn}
		try:
			df,clientM,err,funddate = get_fund_quote(jobj,clientM=clientM,dbname=dbname,tablename=tablename)
			df = subDF(df,colX)
			if saveDB:
				sys.stderr.write("==SAVING {} of\n{}\n".format(fn,df.iloc[:10]))
				dg = df.to_dict(orient='records')
				ret = upsert_mdb(dg,clientM=clientM,dbname=dbname,tablename=themename,zpk=zpk)
		except Exception as e:
			sys.stderr.write("**ERROR:{}:{}\n".format(fn,str(e)))
			df={}
	return df

# caseX_1
def batch_fintel(fundLst=[],dats='2020-03-31',sn=0,tn=1,zpk=['fund','ticker'],saveDB=False,debugTF=False):
	''' Pull holding tickers  from fintel site
	'''
	if len(fundLst)<1:
		fundLst=['berkshire-hathaway','blackrock','bridgewater-associates-lp',
			'goldman-sachs-group','renaissance-technologies-llc']
	for j,fund in enumerate(fundLst):
		try:
			df = run_fintel(fund=fund,dats=dats,sn=sn,tn=tn,saveDB=saveDB,debugTF=debugTF)
		except Exception as e:
			sys.stderr.write("**ERROR:{}:{} @{}\n".format(j,str(e),'batch_fintel'))
			df={}
			continue
	return df

def theme_majorplayer(caseX=2,dats="2020-06-30"):
	sys.stderr.write("==RUNNING:case{} @{}\n".format(caseX,'theme_majorplayer'))
	if caseX==1:
		#- Pull holding tickers from fintel site 
		# (every quarter manually, next time:2020/08/15)
		batch_fintel(saveDB=True,dats=dats,debugTF=True)
	elif caseX==2:
		#- Process ticker list + daily quote and ytd performance
		batch_fintel_majorplayer(tablename='topic_fintel_hist',saveDB=True,debugTF=True)
	elif caseX==3:
		#- directly get fund info with quotes
		ret=get_fund_quote(jobj={"fund":"berkshire-hathaway"},**optx)

if __name__ == '__main__':
	args = sys.argv[1:]
	caseX = int(args[0]) if len(args)>0 else 2
	dats = args[1] if len(args)>1 else "2020-06-30"
	theme_majorplayer(caseX,dats)
