#!/usr/bin/env python
'''
TBD, Wed Jan 30 21:47:38 EST 2019
to automate 1). sp500 ticker selection, 2.) additional ticker price hist update 3. split adj (TBD), 4. dividends adj (TBD), 5. earnings update 6. financials update

Procedure:
1. pull sp500 stock list to [sp500_component]
2. pull available stock list to [iex_company_temp]
3. add additional new sp500 list to [mapping_ticker_cik] via "mapping_ticker_cik.add.upd.sql"
4. check stock status in [mapping_ticker_cik]
   update [mapping_ticker_cik] set act_code=0 
   if act_code=1 is no longer available in [iex_company_temp]
5. pull iex stock "quotes" in daily basis based on [iex_company_temp]
6. pull iex stock "chart" in weekly basis based on [iex_company_temp]
7. check splits, M&A activies daily basis
'''

import sys
import pandas as pd
from pprint import pprint
from _alan_calc import sqlQuery,conn2pgdb,upd_temp2hist

def printerr(s,file=sys.stderr,end='\n'):
	file.write(s+end)


dbname='ara';hostname='localhost'
pgDB=conn2pgdb(dbname=dbname,hostname=hostname)
xqr = """select * from (select a.pbdate,a.name,a.close,b.close,(a.close/b.close-1)*100. as pchg from prc_hist_iex a,prc_temp_iex b, (select name,min(pbdate) mndate from prc_temp_iex group by name) as c where a.pbdate=c.mndate and b.pbdate=c.mndate and a.pbdate=b.pbdate and a.name=b.name and a.name=c.name ) as x where abs(pchg)>0.5 ORDER BY abs(pchg)
"""
scLst = sqlQuery(xqr,engine=pgDB)
if len(scLst)>0:
	tb_temp='temp_list'
	scLst.to_sql(tb_temp,pgDB,index=False,schema='public',if_exists='replace')
	xlst = "('{}')".format("','".join(scLst['name']))
	#fp=open('stock_list_upd.tmp','w')
	#fp.write(scLst.to_csv(index=False,sep='|'))
	#fp.close()
	printerr(xlst)

	# delete entire hist if temp in the earliest is not consistent
	xqr = """delete from prc_hist_iex where name in {}""".format(xlst)
	printerr(xqr)
	pgDB.execute(xqr,pgDB)

# update temp to hist
upd_temp2hist(pgDB,temp='prc_temp_iex',hist='prc_hist_iex',pcol=['name','pbdate'])
