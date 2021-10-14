#!/usr/bin/env python
'''
Note: python3 for _alan_str part is not compatible yet
'''
import sys
from _alan_calc import sqlQuery,subDict

def  get_eqtLst(minMarketCap=50000000000):
	'''
	return equity list for import highlite based on dow_component,sp500_component, yh_quote_curr
	and minimum size (100B) of marketCalp
	'''
	xqTmp = '''SELECT a.ticker FROM sp500_component a, yh_quote_curr b 
		WHERE a.ticker=b.ticker AND 
		(b."marketCap">{} OR a.ticker in (SELECT ticker FROM dow_component))'''
	xqr = xqTmp.format(minMarketCap)
	try:
		eqtLst =  list(sqlQuery(xqr)['ticker'])
	except Exception as e:
		eqtLst=['AAPL','ABT','ACN','ADBE','AMGN','AMZN','AVGO','AXP','BA','BAC','BRK-B','C','CAT','CMCSA','COST','CRM','CSCO','CVX','DHR','DIS','DOW','FB','GOOG','GOOGL','GS','HD','HON','IBM','INTC','JNJ','JPM','KO','LIN','LLY','LMT','MA','MCD','MDT','MMM','MRK','MSFT','NEE','NFLX','NKE','NVDA','ORCL','PEP','PFE','PG','PM','PYPL','SBUX','T','TMO','TRV','TXN','UNH','UNP','UPS','UTX','V','VZ','WBA','WFC','WMT','XOM']
	return eqtLst

def headline_calc(tkLead='^GSPC',idxLst=None,eqtLst=None,np=3,xCol='changePercent',colLst=[],thd=0.05):
	'''
	return object of {'topLst', 'indexOrder', 'topIndex', 'indexLst', 'topUpDn'}
	Where
	  topIndex: ticker name of lead index defined as 'tkLead'='^GSPC'
	  topUpDn: sign in string UP/FLAT/DOWN within the range of 'thd'=[0.05,-0.05]
	  allUpDn: 1,0,-1 indecis all up/TBD/down
	  topLst: selected 'eqtLst' stock quote info ranked via 'changePercent'
	      w.r.t. the 'sign'/'topUpDn' of 'topIndex'
	  bttmLst: selected 'eqtLst' stock quote info oppsite to topLst
	  indexLst: 'idxLst' stock  quote info listed in the order of 'indexOrder'

	  Note aht topIndex quote info should be in the 'indexLst'
	'''
	from _alan_str import udfStr,find_mdb
	if eqtLst is None or len(eqtLst)<1:
		eqtLst = get_eqtLst()
	if idxLst is None or len(idxLst)<1:
		idxLst = ['^GSPC','^DJI','^IXIC'] #,'^SOX']
	if colLst is None or len(colLst)<1:
		#colLst=['open','high','low','close','volume','ticker','change','changePercent','pbdate']
		colLst=['close','volume','ticker','change','changePercent','pbdate','pbdt']
	#xqTmp="SELECT * from yh_quote_curr WHERE ticker in ('{}')"

	# get selected equities quote performance
	#tkStr = "','".join(eqtLst)
	#eqtRtn = sqlQuery(xqTmp.format(tkStr))[colLst]
	jobj = dict(ticker={'$in':eqtLst})
	eqtRtn = find_mdb(jobj,dbname='ara',tablename='yh_quote_curr',dfTF=True)[0][colLst]

	# get indices quote performance
	#tkStr = "','".join(idxLst)
	#idxRtn = sqlQuery(xqTmp.format(tkStr))[colLst]
	jobj = dict(ticker={'$in':idxLst})
	idxRtn = find_mdb(jobj,dbname='ara',tablename='yh_quote_curr',dfTF=True)[0][colLst]


	# calc 'topLst' w.r.t. the 'sign'/'topUpDn' of 'topIndex'
	pbdate=idxRtn.query("ticker=='{}'".format(tkLead))['pbdate'].iloc[0]
	chgPct=idxRtn.query("ticker=='{}'".format(tkLead))[xCol].iloc[0]
	topUpDn = udfStr(chgPct,['UP','DOWN','FLAT'],thd)
	topSign = udfStr(chgPct,[1,0,-1],thd)
	sign = False if chgPct>=0 else True
	xd = eqtRtn.sort_values(by=[xCol],ascending=sign)
	leadLst = xd.iloc[:np]
	if(xd['changePercent'].iloc[0]*xd['changePercent'].iloc[-1])<0:
		bttmLst = xd.iloc[-1:]
	else:
		bttmLst = []

	# update my lead index in the top level
	dd = dict(topIndex=tkLead)
	dd.update(topUpDn=topUpDn)

	# add  all indices info to idxLst
	dd.update(indexLst=idxRtn[colLst].to_dict(orient='records'))
	indexOrder=[x['ticker'] for x in dd['indexLst']]
	dd.update(indexOrder=indexOrder)

	# determine if indices are all Up/Undetermined/Down
	if all([x['changePercent']<0 for x in dd['indexLst']]):
		allUpDn = -1
	elif all([x['changePercent']>0 for x in dd['indexLst']]):
		allUpDn = 1
	else:
		allUpDn = 0
	dd.update(allUpDn=allUpDn)

	# add  topLst 
	if len(leadLst)>0:
		dd.update(topLst=leadLst[colLst].to_dict(orient='records'))
	else:
		dd.update(topLst=[])
	if len(bttmLst)>0:
		dd.update(bttmLst=subDict(bttmLst,colLst).to_dict(orient='records'))
	else:
		dd.update(bttmLst=[])

	# get hiloRecord (based on past 1-year daily change since end date)
	hiloRecord=find_hiloRecord(ticker=tkLead,end=pbdate,days=366)
	dd.update(hiloRecord=hiloRecord)
	dd.update(start=pbdate)
	dd.update(mp3YN=False)

	return dd

def find_hiloRecord(ticker='^GSPC',end=None,days=366,debugTF=False):
	#from record_hilo import find_record_hilo as frh
	from record_hilo import recordHiLo as frh
	from _alan_calc import pull_stock_data as psd
	df=psd(ticker,end=end,days=days,pchgTF=True)
	endDT=df.index[-1]
	jobj=frh(df,endDT,ticker,debugTF=debugTF)
	hiloRecord = jobj['YTD'] if jobj['YTD'] else {}
	return(hiloRecord)

import numpy as np
import datetime
if __name__ == '__main__':
	from _alan_str import jj_fmt
	cdt=datetime.datetime.now()
	hm=int(cdt.strftime("%H00"))
	end_hm=np.clip(hm,900,1600)
	dd= headline_calc()
	sys.stderr.write("{},{}\n".format(end_hm,dd))
	print(jj_fmt("{% include 'intraday_headline_cn.j2' %}",dd,dirname='templates/',outdir="US/mp3_hourly/",end_hm=end_hm ) )
	#print(jj_fmt("{% include 'intraday_briefing_cn.j2' %}",dirname='templates/',outdir="US/mp3_hourly/",end_hm=end_hm,**dd) )
