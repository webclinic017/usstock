#!/usr/bin/env python
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import pandas as pd
import sys

dbname='ara' if len(sys.argv)==1 else sys.argv[1]
pgDB=create_engine('postgresql://sfdbo@localhost:5432/'+dbname)
cymd=pd.read_sql('SELECT max(pbdate) FROM prc_hist',pgDB).iloc[0][0]
nday=7
dformat="%Y%m%d"
dt=datetime.strptime(str(cymd),dformat)-timedelta(days=nday)
x7ymd=datetime.strftime(dt,dformat)

xqTmp="""SELECT p.name as ticker, p.close as price, p.pbdate, v.volume, v.nday, v.startdate FROM (SELECT name,sum(volume) AS volume, {} as nday,min(pbdate) as startdate,max(pbdate) as pbdate FROM prc_hist WHERE pbdate>{} GROUP by name) as v, prc_hist p WHERE p.name=v.name AND p.pbdate={} AND p.pbdate=v.pbdate """
xqr=xqTmp.format(nday,x7ymd,cymd)
df=pd.read_sql(xqr,pgDB)
print df.head()
wmode='replace'
tablename='price_volume_temp'
df.to_sql(tablename, pgDB, schema='public', index=False, if_exists=wmode)
