#!/usr/bin/env python
# DEPRECATED

#mongoexport -c earnings_yh -d ara --query='{pbdate:20190418"}' --type=csv --fields=ticker --noHeaderLine > x

import sys;
from _alan_calc import conn2mgdb
from _alan_str import write2mdb,find_mdb
import pandas as pd
import datetime

sdate=sys.argv[1] if len(sys.argv)>1 else datetime.datetime.now().strftime('%Y%m%d')
endOfDayTF= int(sys.argv[2]) if len(sys.argv)>2 else 1
from macro_event_yh import search_earnings
df=search_earnings(sdate);
vs= df['ticker']
#if endOfDayTF :
#	vs= df['ticker']
#else:
#	vs= df[df['CallTime']!='After Market Close']['ticker']

clientM = conn2mgdb()
pbdate=int(sdate)
print(pbdate)
xg,_,_ = find_mdb(clientM=clientM,dbname='ara',tablename='iex_news_eps',jobj={"pbdate":pbdate},field={"ticker"})

from peers_chart import eps_news_grabber;
finalvs = vs
if len(xg)>0:
	if len(xg[0])>0:
		vg= pd.DataFrame(xg)
		finalvs = list(set(vs)-set(vg['ticker'].values))
finalvs = vs
sys.stderr.write("{}\n".format(finalvs))
#vs=sys.stdin.read().split('\n');
dd=[]
tablename='iex_news_eps'
dbname='ara'
#import dateutil.parser
for x in finalvs:
	if x=='GOOGL':
		x='GOOG'
	try:
		#da = eps_news_grabber(x)
		da = {}
	except Exception as e:
		sys.stderr.write("**ERROR 1:{}, {}".format(x,str(e)))
		continue
	try:
		if 'ticker' not in da or 'eps' not in da:
			continue
		da.update(pbdate=pbdate)
		mobj,_,_ = write2mdb([da],clientM,dbname=dbname,tablename=tablename,zpk={'ticker','pbdate'})
		dd.append(da)
	except Exception as e:
		sys.stderr.write("**ERROR 2:{}, {}".format(x,str(e)))
		continue
#if dd:
#	df = pd.DataFrame(dd)
#	mobj,_,_ = write2mdb(df,clientM=None,dbname=dbname,tablename=tablename,zpk={'ticker','pbdate'})
#	print(df)
