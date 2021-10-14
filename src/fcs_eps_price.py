#!/usr/bin/env python
import sys
from pprint import pprint
import pandas as pd
from sector_eps_price import get_dh, wrap_eps_fcs, sqlQuery, json

def fcs_eps_price(ticker='IBM',dprm=None,sector=None,pngname=None):
	sqx =  "select * from earnings_temp where ticker={!r}".format(ticker)
	epsInfo = sqlQuery(sqx).iloc[0].to_dict()
	print >> sys.stderr, epsInfo
	if sector is None:
		sqx="SELECT s.sector FROM spdr_sector s, (SELECT b.sector FROM (SELECT ticker FROM temp_eps where ticker={!r} GROUP BY ticker) a, (SELECT ticker,sector FROM iex_company_hist) b WHERE a.ticker=b.ticker) x WHERE s.sector_alias=x.sector".format(ticker)
		try:
			sector = str(sqlQuery(sqx)['sector'].iloc[0])
		except:
			print >> sys.stderr, "Use [ALL] sector as default"
			sector = 'ALL'
	if dprm is None:
		sqx = "SELECT * FROM fa_eps_param WHERE sector={!r}".format(sector)
		dprm = sqlQuery(sqx).iloc[0]
	dFcs, dh, dg = get_dh(ticker)
	dfcs,hwY =  wrap_eps_fcs(dFcs,sector,dprm,pngname=pngname,fcsTF=False)
	epsInfo['prcFcsQ1'] = json.loads(dfcs[0])[0]
	epsInfo['prcFcsY1'] = json.loads(dfcs[0])[3]
	epsInfo['epsFcsQ1'] = json.loads(dFcs.loc[0,'hwX'])[0]*dFcs.loc[0,'prc_cur']/100+epsInfo['eps']
	print >> sys.stderr, dFcs
	
	return epsInfo,dfcs,hwY


def run_eps_fcs(tkLst=None,dprm=None,sector=None,pngname=None):
	""" based on EPS/P difference to project price returns for next 4 quarters
	"""
	epsV=[]
	for ticker in tkLst:
		epsInfo, dfcs,hwY =  fcs_eps_price(str(ticker).upper())
		print >> sys.stderr, dfcs
		epsV.append(epsInfo)
	#print(pd.DataFrame.from_dict(epsV).to_html(index=False))
	print(pd.DataFrame.from_dict(epsV).to_csv(sep="|",index=False))
	return(epsV)

if __name__ == "__main__":
	tkLst= sys.argv[1:] if len(sys.argv)>1 else ['AAPL']
	run_eps_fcs(tkLst=tkLst,dprm=None,sector=None,pngname=None)
