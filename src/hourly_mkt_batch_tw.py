# -*- coding: utf-8 -*-
import sys
import datetime
from _alan_str import sysCall
from _alan_calc import sqlQuery,conn2pgdb
import numpy as np
reload(sys)
sys.setdefaultencoding('utf8')

def get_tkLst(tkLst=[],xqr="select etfname from spdr_sector",pgDB=None):
	xLst = [str(x) for x in sqlQuery(xqr,pgDB).iloc[:,0].values]
	if len(tkLst)<1:
		return xLst
	else:
		return np.append(tkLst,xLst)

start = int(sys.argv[1]) if len(sys.argv)>1 else int(datetime.datetime.now().strftime('%Y%m%d'))
lang = sys.argv[2] if len(sys.argv)>2 else 'cn'
pgDB=conn2pgdb(dbname='ara.tw')
#tkLst = ['1101','1102','1216','1301','1303','1326','1402','2002','2105','2301','2303','2308','2317','2327','2330','2354','2357','2382','2395','2408','2409','2412','2454','2474','2492','2633','2801','2823','2880','2881','2882','2883','2884','2885','2886','2887','2890','2891','2892','2912','3008','3045','3481','3711','4904','4938','5871','5880','6505','9904']
tkLst = ['000','001','050','1101','1102','1216','1301','1303','1326','1402','2002','2105','2301','2303','2308','2317','2327','2330','2354','2357','2382','2395','2408','2409','2412','2454','2474','2492','2633','2801','2823','2880','2881','2882','2883','2884','2885','2886','2887','2890','2891','2892','2912','3008','3045','3481','3711','4904','4938','5871','5880','6505','9904']

xqr = "select ticker,company_cn as label from mapping_ticker_cik where ticker in ('{}')".format("','".join(tkLst))
tkLbLst = sqlQuery(xqr,pgDB).to_dict(orient='records')
print xqr,tkLbLst
for v in tkLbLst:
	ticker,title = v['ticker'],v['label']
	d = dict(start=start,title=title,ticker=ticker,lang=lang)
	xcmd="python hourly_mkt.py {ticker} --start={start} --extra_xs='archiveTest=True;dirname=\"TW/mp3\"' --title='{title}' --lang={lang} --src=tw".format(**d)
	print >> sys.stderr, xcmd
	sysCall(xcmd)
