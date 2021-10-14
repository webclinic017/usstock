#!/usr/bin/env python3
import sys
import requests
import datetime
import pandas as pd
import json
from bs4 import BeautifulSoup, element as bs4Element
from _alan_str import write2mdb,insert_mdb
if sys.version_info.major == 2:
	from urlparse import parse_qs
else:
	from urllib.parse import parse_qs

def madmoney_screener(hostname='localhost',dbname='ara',tablename='madmoney_screener',airdate='07',showrows=500,rawTF=False,saveDB=True,debugTF=False,**kwargs):
	'''
	get madmoney list in daily basis
	ref table: madmoney_screener
	ref site: https://madmoney.thestreet.com/screener/index.cfm?showview=stocks&showrows=500
	'''
	sys.stderr.write('{}\n'.format(locals()))
	urx = 'https://madmoney.thestreet.com/screener/index.cfm?showview=stocks&showrows={showrows}&airdate={airdate}'
	CallD={'5':'buy','4':'positive','3':'hold','2':'negative','1':'sell'}
	SegmentD=dict(F='Featured Stock',D='Discussed Stock ',C='Callers Stock',I='Guest Interview',L='Lighting Round',M='Mail Bag',G='Game Plan',S='Sudden Death')
	#url = urx.format(showrows=500,airdate='2019-07-17')
	url = urx.format(showrows=showrows,airdate=airdate)
	d = dict(symbol='',airdate=airdate,called='%',industry='%',sector='%',segment='%',pricelow=0,pricehigh=1000,sortby='airdate')
		
	sys.stderr.write('URL:\n{}\n'.format(url))
	try:
		ret = requests.get(url,timeout=10)
		#ret = requests.post(url,data=json.dumps(d),timeout=10)
		xstr=ret.content
		df = pd.read_html(xstr,attrs={'id':'stockTable'},index_col=False,header=0)[0]
		ret = requests.get(url)
		s = BeautifulSoup(ret.content,'lxml')
		trLst=s.find_all('table')[0].find_all('tr')
		dd=[]
		for j,rwx in enumerate(trLst):
			if j<1:
				tagX='th' 
				tdLst = rwx.find_all(tagX)
				vHdr = [x.text for x in tdLst]
				continue
			else:
				tagX='td' 
			tdLst = rwx.find_all(tagX)
			vLst=[]
			if len(tdLst)<len(vHdr):
				continue
			for k,xTd in enumerate(tdLst):
				if isinstance(xTd.next,bs4Element.Tag) and xTd.next.has_attr('alt'):
					xv = xTd.next['alt']
				elif isinstance(xTd.next,bs4Element.Tag) and xTd.next.has_attr('href'):
					xv =  parse_qs(xTd.next['href'])['symbol'][0]
				else:
					xv = xTd.text
				vLst.append(xv)

			dx = dict(zip(vHdr,vLst))
			dd.append(dx)
		df=pd.DataFrame(dd)
	except Exception as e:
		sys.stderr.write('**ERROR: {} @{}\n'.format(str(e),'madmoney_screener()'))
		return {}
	if rawTF:
		return df
	try:
		df['CallDscript']=[CallD[x] for x in df['Call']]
		df['SegmentDscript']=[SegmentD[x] for x in df['Segment']]
		df['Price'] = df['Price'].apply(lambda x:float(x.strip('$')))
		mmdd = [int("".join(x.split('/'))) for x in df['Date']]
		tdate=datetime.datetime.today()
		cmmdd = int(tdate.strftime("%m%d"))
		cYr = tdate.year
		xYr = tdate.year -1
		pbdate = [(cYr if cmmdd>x else xYr)*10000 + x for x in mmdd]
		df['pbdate'] = pbdate
		df['ticker'] = df['Portfolio'].copy()
	except Exception as e:
		sys.stderr.write('**ERROR: {} @ PULLING {}\n'.format(str(e),url))
		return {}
		
	if saveDB:
		#mobj,_,msg = write2mdb(df,clientM=None,dbname=dbname,tablename=tablename,zpk={'ticker','pbdate'})
		mobj,_,msg = insert_mdb(df,clientM=None,dbname=dbname,tablename=tablename,zpk={'ticker','pbdate'})
		sys.stderr.write('==Save to: MDB:{}: {}\n'.format(tablename,msg))

	return df

if __name__ == '__main__':
	args=sys.argv
	airdate= args[1] if len(args)>0 else '07'
	df=madmoney_screener(hostname='localhost',dbname='ara',tablename='madmoney_hist',airdate=airdate,showrows=500,debugTF=False)
	print(df)
