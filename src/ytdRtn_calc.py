#!/usr/bin/env python3

import sys
sys.path.append('/apps/fafa/pyx/tst/')
from _alan_calc import conn2mgdb,sqlQuery,renameDict
from _alan_str import find_mdb,upsert_mdb
from IPython.core.display import display, HTML
import pandas as pd,numpy as np

def ytdRtn_calc(start=20200219,group='sector',loDate=20200323,dbname='ara',**optx):
	sys.stderr.write(" --INPUTS:{}\n".format(locals()))
	groupLst=[group]
	sqx="SELECT name as ticker,close as ytdclose from prc_hist where pbdate={} ORDER BY ticker".format(start)
	dhi = sqlQuery(sqx)
	sqx="SELECT name as ticker,close as loclose from prc_hist where pbdate={} ORDER BY ticker".format(loDate)
	dlo = sqlQuery(sqx)
	sys.stderr.write(" --sqx:{}".format(sqx))
	#display(print("<style>div.output_scroll { height: 30em; }</style>"))
	dytd=dhi.merge(dlo,on='ticker')
	dytd['loRtn']=dytd['loclose']/dytd['ytdclose']*100-100
	#print(dytd.tail().to_string(index=False),file=sys.stderr)

	tkLst=list(dytd['ticker'].values)
	jobj = {"ticker":{'$in':tkLst}}
	dq,clientM,_=find_mdb(jobj,clientM=None,dbname='ara',tablename='yh_quote_curr',dfTF=True)
	#print(dq.head().to_string(index=False),file=sys.stderr)
	pbdt=dq['pbdt'].max()

	colX=['ticker','close','marketCap','trailingPE','shortName','pbdt']
	dh=dq[colX].merge(dytd,on='ticker')
	#print(dh.tail().to_string(index=False),file=sys.stderr)

	dh['ytdRtn']=dh['close']/dh['ytdclose']*100-100
	#print(dh.head().to_string(index=False))

	field={'ticker','sector','industry'}
	ds,clientM,_=find_mdb(jobj,clientM=None,dbname='ara',tablename='yh_summaryProfile',field=field,dfTF=True)
	dh=dh.merge(ds,on='ticker')
	#print(dh.head().to_string(index=False))

	d500,clientM,_=find_mdb(jobj,clientM=None,dbname='ara',field={"ticker","GICS Sector","GICS Sub Industry"},tablename='sp500_component',dfTF=True)
	#print(d500.head().to_string(index=True))

	find_mdb({"ticker":{'$nin':tkLst}},clientM=None,dbname='ara',tablename='sp500_component',dfTF=True)[0]
	dh1=dh.query('marketCap>3000000000')
	#print(dh1.head().to_string(index=True))
	dh1 = dh1[dh1[group]!='']


	dss = dh1.groupby(groupLst).apply(lambda x:
		pd.Series([np.average(x.ytdRtn, weights=x.marketCap),
		x.ytdRtn.mean(),
		np.average(x.loRtn, weights=x.marketCap),
		x.loRtn.mean(),
		x.marketCap.sum(),
		x.ytdRtn.count()],index=['ytdRtnWAvg','ytdRtnAvg','loRtnWAvg','loRtnAvg','marketCap','count']))
	dss['recoverRtnWA'] = dss['ytdRtnWAvg']-dss['loRtnWAvg']

	# use index as first column
	dss.reset_index(inplace=True)

	# add additional columns
	if group=='industry':
		colx=['sector','industry']
		dss = dss.merge(dh1[colx].drop_duplicates(colx),on=['industry'],how='left')
	elif group=='ticker':
		colx=['ticker','sector','industry','pbdt']
		dss = dss.merge(dh1[colx],on=['ticker'],how='left')

	if 'pbdt' not in dss:
		dss['pbdt']=pbdt

	colD=dict(
		ytdRtnWAvg="WA Return% since_2/19",
		ytdRtnAvg="Avg Return% since_2/19",
		loRtnWAvg="WA Return% 3/23-2/19",
		loRtnAvg="Avg Return% 3/23-2/19",
		recoverRtnWA="Recovered Return% since_3/23"
		)
	dss = renameDict(dss,colD)

	cfm = {'marketCap': "{:,.0f}".format, 'count':"{:.0f}".format }
	#print(dss.to_string(index=True,formatters=cfm) ,file=sys.stderr)
	#print(dss.to_string(index=True,formatters=cfm) ,file=sys.stderr)
	return dss

def ytdOTF(funcArg,**optx):
	'''
	real-time data is only grab based on 'deltaTolerance' in seconds
	current setup is half-days
	'''
	if isinstance(funcArg,str):
		if funcArg in globals() and hasattr(globals()[funcArg],'__call__'):
			funcArg =  globals()[funcArg]
		else:
			return []
	sys.stderr.write("==START Running {}\n".format(funcArg))
	deltaTolerance=optx.pop('deltaTolerance',43200)
	dbname=optx.pop('dbname','ara')
	tablename=optx.pop('tablename','')
	tableChk=optx.pop('tableChk',tablename+'_chk')
	objChk=optx.pop('objChk',{})
	zpkChk=optx.pop('zpkChk',list(objChk.keys()))
	if not all([tablename]):
		return []
	renewTF,objChk,clientM = lastRunChk(objChk=objChk,dbname=dbname,tableChk=tableChk,deltaTolerance=deltaTolerance,**optx)
	if renewTF:
		sys.stderr.write("==Data outdated or never run, Running:{}\n".format(funcArg))
		retObj = funcArg(dbname=dbname,**optx)
		if len(retObj)<1:
			return []

		retObj,clientM,errChk = upsert_mdb(retObj,dbname=dbname,tablename=tablename,**optx)
		sys.stderr.write(" --Update {} to {}\n".format(objChk,tableChk))
		objChk,clientM,errChk = upsert_mdb(objChk,clientM=clientM,tablename=tableChk,zpk=zpkChk)
	else:
		sys.stderr.write("==Data exist, LoadFromTable:{}\n".format(tablename))
		objChk.pop('pbdt',None)
		optx.pop('zpk',None)
		optx.pop('field',{})
		retObj,clientM,errMsg = find_mdb(objChk,clientM=clientM,dbname=dbname,tablename=tablename,field={},**optx)
	return retObj

def renewChk(pbdtCurr,pbdtMod,deltaTolerance=86400):
	deltaPassed=pd.Timedelta(pbdtCurr - pbdtMod).total_seconds()
	sys.stderr.write(" --curr:{},last:{}:deltaPassed:{}\n".format(pbdtCurr,pbdtMod,deltaPassed))
	return deltaPassed>deltaTolerance

def lastRunChk(objChk={},tableChk='',deltaTolerance=43200,clientM=None,dbname='ara',**optx):
	pbdtCurr=pd.datetime.now()
	lastObj,clientM,_=find_mdb(objChk,clientM=clientM,dbname=dbname,tablename=tableChk,limit=1)
	if not lastObj:
		pbdtMod=pbdtCurr
		renewTF=True
	else:
		pbdtMod=lastObj[0]['pbdt']
		renewTF=renewChk(pbdtCurr,pbdtMod,deltaTolerance)
		if renewTF:
			pbdtMod=pbdtCurr
	objChk.update(pbdt=pbdtMod)
	return renewTF,objChk,clientM

if __name__ == "__main__":
	start,group=(20200221,'sector')
	if len(sys.argv)==2:
		start=sys.argv[1]
	elif len(sys.argv)>2:
		start,group=sys.argv[1:3]	
	ytdRtn_calc(start,group)
