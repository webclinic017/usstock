#!/usr/bin/env python
'''
DEPRECATED, no longer used
 get up-to-date utd_earnings_list
comppare yahoo earnings vs. iex release
Usage of 
http://vm2.beyondbond.com/aicaas/api/?topic=utd_earnings_list&sector=Financial%20Services&pbdate=20190101

SECTOR:
Utilities
Industrials
Financial Services
Basic Materials
Healthcare
Consumer Defensive
Communication Services
Consumer Cyclical
Technology
Real Estate
Energy
'''

import pandas as pd
from _alan_str import find_mdb
from _alan_calc import sqlQuery

def utd_earnings_list(sector='Technology',pbdate='20190101'):
	xg,_,_ = find_mdb(dbname='ara',tablename='earnings_yh',jobj={'pbdate':{'$gt':int(pbdate)}},field={"ticker":1,"pbdate":1,"estimatedEPS":1,"actualEPS":1}) #- use yahoo earnings
	dg = pd.DataFrame(xg)
	# use yh_earningsHistory
	xqr= '''SELECT s.sector,s.industry,s.company,s."marketCap",t.* FROM (SELECT a.ticker,a.sector,a.industry,b."shortName" as company, b."marketCap" from "yh_summaryProfile" a, yh_quote_curr b WHERE a.sector='{sector}' AND a.ticker=b.ticker ORDER BY a.ticker) as s LEFT JOIN (SELECT a.* FROM (SELECT *,to_char(to_timestamp(quarter),'YYYYMMDD')::int as recdate FROM "yh_earningsHistory" WHERE quarter IS NOT NULL) as a, (SELECT ticker,to_char(to_timestamp(max(quarter)),'YYYYMMDD')::int as mxdate FROM "yh_earningsHistory" WHERE quarter IS NOT NULL GROUP BY ticker) as b WHERE a.ticker=b.ticker AND a.recdate=b.mxdate) as t ON s.ticker=t.ticker'''
	xqr = xqr.format(sector=sector)
	df=sqlQuery(xqr)
	return( pd.merge(df,dg,on='ticker').sort_values(by=['pbdate','marketCap'],ascending=[True,False]) )

if __name__ == '__main__':
	print utd_earnings_list(sector='Technology',pbdate=20190123).to_string()
