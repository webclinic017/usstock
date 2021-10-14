#!/usr/bin/env python3
'''
create MDB:topic_theme_media based on "madmoney_hist"
Usage of:

'''
import sys
import pandas as pd
from _alan_calc import sqlQuery,getKeyVal,subDict,renameDict
from _alan_str import find_mdb,insert_mdb,upsert_mdb
from _alan_date import next_date,dt2ymd,ymd_diff
import pickle

def create_theme_media(dbname="ara",tablename="topic_theme_media",debugTF=False,**optx):
	''' create MDB::'topic_theme_media' table based on 'madmoney_hist' info
	arguments:
	'end' default to 2-weeks of today in YYYYMMDD 
	'callLst' default to ["4","5"] types
	'''
	zpk=getKeyVal(optx,'zpk',{'ticker','pbdate','start','end'})
	start=getKeyVal(optx,'start',0)
	end=getKeyVal(optx,'end',None)
	callLst=getKeyVal(optx,'callLst',["4","5"])
	if end is None:
		end=int(dt2ymd(next_date(weeks=-2)))
	xqTmp="SELECT pbdate,adjusted as price FROM prc_hist where name='{}' and pbdate>{} and pbdate <={} ORDER BY pbdate"
	jobj={"Call":{"$in":callLst},"pbdate":{"$lte":end,"$gte":start}}
	df,mdb,emsg = find_mdb(jobj,sortLst=["pbdate"],dbname="ara",tablename="madmoney_hist",dfTF=True)

	dd=[]
	for j in df.index:
		dx = df.iloc[j].to_dict()
		dx['pbdate']=int(dx['pbdate'])
		ticker=dx['ticker']
		pbdate=period1=dx['pbdate']
		period2=int(dt2ymd(next_date(period1,months=1)))
		xqr=xqTmp.format(ticker,period1,period2)
		dp = sqlQuery(xqr)
		vprc = dp['price'].values
		if len(vprc)<1:
			continue
		vdate = [int(x) for x in dp['pbdate'].values]
		sPrice, ePrice = vprc[0],vprc[-1]
		period1, period2 = vdate[0],vdate[-1]
		rrt = vprc[-1]/vprc[0]*100.-100
		day_diff=ymd_diff(period1,period2)
		dx.update(rrt=rrt,day_diff=day_diff,start=period1,end=period2)
		dx.update(sPrice=sPrice,ePrice=ePrice)
		sys.stderr.write("{j:03d}|{ticker:5s}|{Call}|{rrt:8.2f}|{day_diff:3d}|{start:8d}|{end:8d}|{pbdate:8d}|{Company}|\n".format(j=j,**dx))
		dd.append(dx)
		dy,_,emsg = insert_mdb([dx],tablename=tablename,dbname=dbname,zpk=zpk)

	if start > 12345678:
		jobj={"day_diff":{"$lt":20},"pbdate":{"$lte":start}}
		mdb[dbname][tablename].delete_many(jobj)
	df=pd.DataFrame(dd)
	return df

def df2pickle(dirname="pickle",tablename="topic_theme_media",ext="pickle3"):
	pname = "{dirname}/{tablename}.{ext}".format(**locals())
	fp = open(pname,"wb")
	pickle.dump(df,fp)
	fp.close()

def df2excel(dirname="pickle",tablename="topic_theme_media",ext="xls"):
	pname = "{dirname}/{tablename}.{ext}".format(**locals())
	df.to_excel(pname)

# create MDB:topic_theme_media
def create_topic_theme_media(start=20200101,dbname="ara",tablename="topic_theme_media",tablesrc="madmoney_hist",**optx):
	from _alan_str import find_mdb
	from yh_chart import yh_quote_comparison as yqc
	dtmp,mDB,errmsg=find_mdb({'pbdate':{'$gt':start},'Call':{'$in':['4','5']}},sortLst={'ticker','pbdate'},dbname=dbname,tablename=tablesrc,dfTF=True)
	dg = dtmp.groupby(['ticker']).apply(lambda x: pd.Series([
		x.Call.count(),x.pbdate.max()],index=['buyCount','buyDate']))
	renameDict(dtmp,{'pbdate':'buyDate','Price':'buyPrice'})
	mediaD = dtmp.merge(dg,on=['ticker','buyDate'])
	colX= ['ticker','buyCount','buyDate','buyPrice','sector','industry']
	mediaD = subDict(mediaD,colX)

	quoLst=yqc(mediaD['ticker'].values)
	quoD = pd.DataFrame(quoLst)
	colX = ['ticker','close','fiftyTwoWeekRange','marketCap','pbdate','shortName','changePercent','epsTrailingTwelveMonths','pbdt']
	quoD=subDict(quoD,colX)
	quoD=renameDict(quoD,dict(epsTrailingTwelveMonths='EPS',close='closePrice',shortName='Company',fiftyTwoWeekRange='Range52Week',changePercent='dayChg%',change='Chg',pbdt='pubDate'))
	df = mediaD.merge(quoD,on='ticker') #- remove no-quote rows # ,how='left')

	df.dropna(subset=['marketCap'],inplace=True)
	df['buyChg%']=(df['closePrice']/df['buyPrice'].astype(float)-1)*100
	colX = ['ticker','buyCount','buyDate','marketCap','buyPrice','closePrice','buyChg%','dayChg%','EPS','Company','Range52Week','pbdate','pubDate','sector','industry']
	#df=subDict(df,colX)
	print(" --media DF:\n{}".format(df),file=sys.stderr)
	zpk=optx.pop('zpk',{'ticker'})
	upsert_mdb(df,dbname=dbname,tablename=tablename,zpk=zpk)
	sys.stderr.write(" --DF:\n{}\n".format(df.head().to_string(index=False)))
	return df

if __name__ == '__main__':
	#start=int(dt2ymd(next_date(weeks=-6)))
	#df = create_theme_media(debugTF=True,start=start)
	start=int(dt2ymd(next_date(months=-7,day=31)))
	df = create_topic_theme_media(debugTF=True,start=start)
	colx=['ticker','rrt','end','Company','pbdate','buyCount']
	#print(subDict(df,colx).sort_values(['buyCount'],ascending=False),file=sys.stderr)
