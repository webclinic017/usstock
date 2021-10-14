#!/usr/bin/env python3
'''
Create IPO list with quotes in 'topic_theme_ipo' table
based on nasdaq IPO list 'nasdaq_ipos'
Steps for update:
1. Update nasdaq_ipos table (crontab in upd_mapping_ticker.sh for weekly run)
python3 theme_list.py nasdaq_ipos 
2. Update topic_theme_ipo table (crontab in pull_earnings.sh for daily run)
python3 -c "from ipoReport import create_topic_theme_ipo as ctt;ctt(limit=500)"

Last Mod., Thu Jun  4 13:18:17 EDT 2020
'''
import sys
#sys.path.append("/apps/fafa/pyx/tst/")
#from IPython.core.display import HTML
import pandas as pd,numpy as np, datetime
from _alan_str import find_mdb,insert_mdb,merge_t2l,merge_yqc
from _alan_calc import getKeyVal,pqint,subDF

def create_topic_theme_ipo(updTF=False,**opts):
	''' create 'topic_theme_ipo' based on
	'nasdaq_ipos' and yh live-quote info 
	'''
	from _alan_calc import renameDict,subDict
	from _alan_str import find_mdb,upsert_mdb
	from yh_chart import yh_quote_comparison as yqc
	# Note: 500 limit may cause close prices of certain tickers not get updated, need further debugging
	limit=opts.pop('limit',500)
	ipoLst,_,_=find_mdb(tablename='nasdaq_ipos',dbname='ara',sortLst=['pbdate'],limit=limit,dfTF=True)
	ipoLst=renameDict(ipoLst,dict(pbdate='ipoDate',price='ipoPrice'))
	ipoD=subDict(ipoLst,['ticker','ipoDate','ipoPrice','sector','industry'])
	quoLst=yqc(ipoD['ticker'].values)
	quoD = pd.DataFrame(quoLst)
	colX = ['ticker','close','fiftyTwoWeekRange','marketCap','pbdate','shortName','changePercent','epsTrailingTwelveMonths','pbdt']
	quoD=subDict(quoD,colX)
	quoD=renameDict(quoD,dict(epsTrailingTwelveMonths='EPS',close='closePrice',shortName='Company',fiftyTwoWeekRange='Range52Week',changePercent='dayChg%',change='Chg',pbdt='pubDate'))
	df = ipoD.merge(quoD,on='ticker') #- remove no-quote rows # ,how='left')
	df.dropna(subset=['marketCap'],inplace=True)
	df['ipoChg%']=(df['closePrice']/df['ipoPrice'].astype(float)-1)*100
	colX = ['ticker','ipoDate','marketCap','ipoPrice','closePrice','ipoChg%','dayChg%','EPS','Company','Range52Week','pbdate','pubDate','sector','industry']
	df=subDict(df,colX)
	pqint(" --ipo DF:\n{}".format(df),file=sys.stderr)
	dbname=opts.pop('dbname','ara')
	tablename=opts.pop('tablename','topic_theme_ipo')
	zpk=opts.pop('zpk',{'ticker'})
	upsert_mdb(df,dbname=dbname,tablename=tablename,zpk=zpk)
	sys.stderr.write(" --DF:\n{}\n".format(df.head().to_string(index=False)))
	return df

# DEPRECATED
def create_ipoData(dbname='ara',tablesrc='nasdaq_ipos',tablename='ipoData'):
	'''Create YTD 'ipoData' table based on 'nasdaq_ipos'
	'''
	from yh_chart import yh_quote_comparison
	from ticker2label import ticker2label as t2l
	df,clientM,_ = find_mdb(dbname=dbname,tablename=tablesrc,dfTF=True)
	sys.stderr.write(" --DF:\n{}\n".format(df))
	try:
		df['price'] = [float(x.replace('$','').replace(',','')) for x in df['price']]
	except Exception as e:
		sys.stderr.write("**ERROR:{}\n".format(str(e)))
	tkLst = [x for x in df['ticker'].values if "'" not in x]
	df = merge_t2l(tkLst,df,quoteTF=False)
	df = merge_yqc(tkLst,df)
	df = df.astype(object).where(df.notnull(), None)
	clientM[dbname][tablename].delete_many({})
	clientM[dbname][tablename].insert_many(df.to_dict(orient='records'))
	return df

# DEPRECATED
def create_ipoReport(dbname='ara',tablesrc='ipoData',topRow=50,tablename='ipoReport',saveDB=True):
	'''Create YTD 'ipoReport' table based on 'nasdaq_ipos'
	'''
	from _alan_date import ymd_diff
	dds,clientM,_ = find_mdb(dbname=dbname,tablename=tablesrc,dfTF=True)
	dds = dds.astype(object).where(dds.notna(), None)
	pbdate=int(dds['pbdate_y'].max())
	dds['daysSinceIPO']=[ymd_diff(x,int(y)) if y is not None else ymd_diff(x,pbdate) for x,y in dds[['pbdate_x','pbdate_y']].values]
	if 'fiftyTwoWeekLow' in dds:
		dds['Range52Week']=["{:.2f} - {:.2f}".format(x,y) if y is not None and y<9999 else "" for x,y in dds[['fiftyTwoWeekLow','fiftyTwoWeekHigh']].values]
	dds['marketCapMM']=dds['marketCap']/10**6
	dds['currDate']=pbdate
	renCol={'pbdate_x':'ipoDate','price':'ipoPrice','close':'currPrice'}
	dds.rename(columns=renCol,inplace=True)
	dds['changeSinceIPO']=dds['currPrice']-dds['ipoPrice']
	dds['changePercent']=dds['currPrice']/dds['ipoPrice']-1
	colX=['ticker','ipoDate','ipoPrice','currPrice','currDate','changeSinceIPO','changePercent','Range52Week','fiftyDayAverage','daysSinceIPO','marketCapMM','trailingPE','shortName']
	df=subDF(dds.sort_values(by=['marketCap'],ascending=False).iloc[:topRow],colX)
	df.reset_index(drop=True,inplace=True)
	if saveDB is True:
		clientM[dbname][tablename].delete_many({})
		clientM[dbname][tablename].insert_many(df.to_dict(orient='records'))
	return df

def find_ipoReport(dbname='ara',tablename='ipoReport',debugTF=False,sort='marketCapMM',**optx):
	output=getKeyVal(optx,'output','html')
	if sort not in ['Range52Week','change','changePercent','currDate','currPrice','daysSinceIPO','fiftyDayAverage','ipoDate','ipoPrice','marketCapMM','shortName','ticker','trailingPE']:
		sort='marketCapMM'
	df,clientM,_ = find_mdb(dbname=dbname,tablename=tablename,dfTF=True,sortLst=[sort])
	pd.options.display.float_format = '{:,.2f}'.format
	cfm = {'marketCapMM': "{:,.0f}".format,'changePercent':"{:.2%}".format }
	if output.lower() in ['csv']:
		return(df.to_csv(sep="|",index=False))
	elif output.lower() in ['tsv']:
		return(df.to_csv(sep="\t",index=False))
	elif output.lower() in ['html']:
		return(df.to_html(formatters=cfm,index=False))
	else:
		return(df)

def get_ipoReport(updTF=True,tablename='ipoReport',sort='marketCapMM',**optx):
	if updTF: # to re-generate ipoReport 
		sys.stderr.write("===Update ipoReport:{}\n".format(tablename))
		ddat = create_ipoData()
		drpt = create_ipoReport()
	# to obtain ipoReport 
	ret = find_ipoReport(sort=sort,tablename=tablename,**optx)
	return ret

if __name__ == '__main__':
	#print(get_ipoReport(updTF=True))
	print(create_topic_theme_ipo())
