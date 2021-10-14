#!/usr/bin/env python
"""
Usage of:
python -c "ticker='AAPL';from peers_chart import eps_news_grabber as eng;print(eng(ticker))"
"""
import matplotlib
matplotlib.use('TkAgg')
import pandas as pd
import matplotlib.pyplot as plt
import sys
import re
import datetime
import requests

def qbyq_chart(ticker,types='financials',kyWord='totalRevenue',ranged='quarter',pbcol='reportDate',plotTF=True,title='',pngname=''):
	"""
	Vertical financial performance between quarter by quarter
	"""
	#urx='https://api.iextrading.com/1.0/stock/{}/{}?period={}&filter={},{}'
	#url=urx.format(ticker,types,ranged,kyWord,pbcol)
	urx='https://api.iextrading.com/1.0/stock/{}/{}'#?period={}&filter={},{}'
	url=urx.format(ticker,types,ranged)#,kyWord,pbcol)
	sys.stderr.write(url+"\n")
	try:
		jtmp = requests.get(url,timeout=2).json()
		#jtmp = pd.read_json(url)
	except Exception as e:
		sys.stderr.write("**ERROR:{} @ {}\n".format(str(e),url))
		return {}
	if types in jtmp:
		dd = list(jtmp[types])
	else:
		dd = jtmp
	df = pd.DataFrame(dd)
	df.index=df[pbcol].values
	if types != 'financials':
		return df
	try:
		da = df[[pbcol,kyWord]][::-1]
		df.loc[:,'profitMargin'] = df['operatingIncome']/df['operatingRevenue']
		db = df[[pbcol,'profitMargin']][::-1]
	except Exception as e:
		sys.stderr.write("**ERROR: {} @{}\n".format(str(e),"qbyq_chart"))
	if plotTF is True:
		ax = plot_2yaxis(da)
	return da

def plot_2yaxis(da,db,figsize=(11,6),title='',kind='bar',fig_name='',fig_format='svg',**optx):
	'''
	plot 2-YAxis via da and db data
	'''
	backend=optx['backend'] if 'backend' in optx else 'Agg'
	# define plot
	fig, ax = plt.subplots(figsize=figsize)

	# 1st plot
	color=optx['color'] if 'color' in optx else 'blue'
	width=optx['width'] if 'width' in optx else 0.2
	position=optx['position'] if 'position' in optx else 1.2
	da.plot(ax=ax,kind=kind,position=position,width=width)

	# 2nd plot
	ax2 = ax.twinx() 
	kind2=optx['kind2'] if 'kind2' in optx else kind
	color2=optx['color2'] if 'color2' in optx else 'red'
	width2=optx['width2'] if 'width2' in optx else width
	position2=optx['position2'] if 'position2' in optx else 0.3
	db.plot(ax=ax2,kind=kind2,color=color2,position=position2,width=width2)

	# plot features
	loc=optx['loc'] if 'loc' in optx else 9
	ncol=optx['ncol'] if 'ncol' in optx else 2
	fancybox=optx['fancybox'] if 'fancybox' in optx else True
	borderaxespad=optx['borderaxespad'] if 'borderaxespad' in optx else 0.
	plt.legend(loc=loc, ncol=ncol, fancybox=fancybox, borderaxespad=borderaxespad)
	rotation=optx['rotation'] if 'rotation' in optx else '30'
	fontsize=optx['fontsize'] if 'fontsize' in optx else 12
	plt.xticks(rotation=rotation,fontsize=fontsize)

	if len(title)>0:
		plt.title(title)

	if fig_name:
		#plt.switch_backend('tkAgg')
		plt.savefig(pngname, format='svg')
	elif backend.lower()='tgagg'
		plt.show()
	return ax

def eps_news_grabber(ticker):
	df = qbyq_chart(ticker,types='news',kyWord='summary',pbcol='datetime',plotTF=False,title='')
	if len(df)<1:
		return {}
	da={}
	for j,s in enumerate(df['summary'].values):
		if 'EPS' in s :
			v = re.search("GAAP\s+EPS\s+of\s+\$?(\d*\.?\d*)\s+(\w+)",s)
			if v is not None:
				eps = float(v.group(1).replace('$',''))
				if 'cent' in v.group(2):
					eps = eps/100.
				da = dict(eps=eps,ticker=ticker)
				try:
					dtm = df['datetime'].iloc[j][:-6]
					sdt = datetime.datetime.strptime(dtm,"%Y-%m-%dT%H:%M:%S")
					da.update(pbdatetime=sdt)
				except Exception as e:
					sys.stderr.write("**ERROR: {}\n".format(str(e)))
			v1 = re.search("Revenue\s+of\s+\$?(\w+\.?\w+)",s)
			if v1 is not None:
				revenue = v1.group(1)
				da.update(revenue=revenue)
			if da:
				break
	return da

def peers_chart(tkLst,types='quote',kyWord='changePercent',plotTF=True,title='',pngname=''):
	"""
	Horizonal performance comparision between a list of peers
	types=[quote|stats]
	kyWord=[changePercent|peRatio|profitMargin]
	"""
	if isinstance(tkLst,list):
		tkStr = ','.join(tkLst)
	else:
		tkStr = tkLst
	urx='https://api.iextrading.com/1.0/stock/market/batch?symbols={}&types={}&range=1d&filter={}'
	url=urx.format(tkStr,types,kyWord)
	sys.stderr.write(url+"\n")
	jtmp = pd.read_json(url)
	dd=[]
	for x in jtmp.columns:
		jtmp[x][types].update(ticker=x)
		dd.append(jtmp[x][types])
	df = pd.DataFrame(dd)
	df.index=df['ticker']
	da = df[[kyWord]].sort_values(kyWord)
	if plotTF is True:
		da.plot(kind='barh')
		if len(title)>0:
			plt.title(title)
		if pngname:
			#plt.switch_backend('tkAgg')
			plt.savefig(pngname, format='svg')
		else:
			plt.show()
	return da

if __name__ == '__main__':
	tkStr='DE,CNHI,AGCO,TEX,ALG,XLI,CAT'
	pngname=''
	if len(sys.argv)>1:
		tkStr = sys.argv[1]
	if len(sys.argv)>2:
		pngname=sys.argv[2]

	# Compare 'changePercent' between peers: tkStr
	peers_chart(tkStr,kyWord='changePercent',pngname=pngname)
	# Compare 'peRatio' between peers: tkStr
	peers_chart(tkStr,kyWord='peRatio',pngname=pngname)
	# Compare 'profitMargin' between peers: tkStr
	peers_chart(tkStr,kyWord='profitMargin',types='stats',pngname=pngname)
	
	ticker=tkStr.split(',')[0]
	# Compare 'totalRevenue' between peers: tkStr
	qbyq_chart(ticker,kyWord='totalRevenue',pngname=pngname)
	
	# get eps from news
	dd = eps_news_grabber(ticker)
	print(dd)
