#!/usr/bin/env python
"""To chart MACD (Moving Average Convergence Divergence) and the RSI (Relative Strength Index).
   Program: alan_plot.py
   Usage of:
	alan_plot.py IBM APPL
	OR
	printf "IBM\nAAPL" | alan_plot.py
   Reference: https://pythonprogramming.net/advanced-matplotlib-graphing-charting-tutorial/

   Note: 
   1. original website program is broken, this is a new working version
   2. matplotlib time-series date  be in float format
   Last mod.,: Fri Oct 26 22:13:28 EDT 2018
"""

import sys
from optparse import OptionParser
import datetime
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
#from matplotlib.finance import candlestick_ohlc
from mpl_finance import candlestick_ohlc
import pandas_datareader.data as web
import pylab
#matplotlib.rcParams.update({'font.size': 9})
from _alan_calc import run_tech,run_ohlc,pull_stock_data,pqint
from _alan_date import ymd_parser, epoch_parser
import os,cgi,cgitb
import matplotlib.font_manager as mfm
font_path = "/usr/share/fonts/truetype/arphic/uming.ttc"
prop = mfm.FontProperties(fname=font_path,size=18)
plt.style.use('dark_background')

def rsiFunc(prices, n=14):
	deltas = np.diff(prices)
	seed = deltas[:n+1]
	up = seed[seed>=0].sum()/n
	down = -seed[seed<0].sum()/n
	rs = up/down
	rsi = np.zeros_like(prices)
	rsi[:n] = 100. - 100./(1.+rs)

	for i in range(n, len(prices)):
		delta = deltas[i-1] # cause the diff is 1 shorter

		if delta>0:
			upval = delta
			downval = 0.
		else:
			upval = 0.
			downval = -delta

		up = (up*(n-1) + upval)/n
		down = (down*(n-1) + downval)/n

		rs = up/down
		rsi[i] = 100. - 100./(1.+rs)

	return rsi

def movingaverage(values,window):
	weigths = np.repeat(1.0, window)/window
	smas = np.convolve(values, weigths, 'valid')
	return smas # as a numpy array

def ExpMovingAverage(values, window): #- applying pandas.ewma() function
	a = pd.Series(values).ewm(span=window).mean()
	#a = pd.ewma(values,span=window)
	return a

def computeMACD(x, slow=26, fast=12):
	"""
	compute the MACD (Moving Average Convergence/Divergence) using a fast and slow exponential moving avg'
	return value is emaslow, emafast, macd which are len(x) arrays
	"""
	emaslow = ExpMovingAverage(x, slow)
	emafast = ExpMovingAverage(x, fast)
	return emaslow, emafast, emafast - emaslow

def plot_candlestick(datax,tsidx=None,chartType='chart',title='',block=True,debugTF=False,ax=None,colorUD=['green','red'],trendTF=False,npar=12):
	'''
	plot candlestick ohlc/volume graph
	'''
	if chartType == 'minute':
		wadj = 24*60
		dtfmt = '%H:%M'
	else:
		wadj = 1
		dtfmt = '%m/%d/%y'
	if 'mpldatetime' not in datax and isinstance(tsidx[0], (datetime.date, datetime.datetime)):
		datax['mpldatetime'] = [mdates.date2num(x) for x in tsidx]
	newAr = datax[['mpldatetime','open','high','low','close']].values
	if debugTF is True:
		pqint(newAr[:10], file=sys.stderr)
	SP = datax.shape[0]
	if ax is None:
		fig = plt.figure(facecolor='#07000d',figsize=(11,6))
		ax = plt.subplot2grid((1,2), (0,0), rowspan=1, colspan=2)
	colorup,colordown = colorUD
	fillcolor = '#00ffe8'
	candlestick_ohlc(ax, newAr[-SP:], width=0.6/wadj, colorup=colorup,colordown=colordown)
	ax.xaxis_date()
	ax.autoscale_view()
	ax.xaxis.set_major_formatter(mdates.DateFormatter(dtfmt))
	ax.grid(True, color='w',linestyle='dotted',linewidth=0.5)
	plt.xticks(rotation=30)
	plt.ylabel('OHLC Price/Volume')
	plt.xlabel('Date Time')
	
	if trendTF is True:
		from _alan_pppscf import vertex_locator
		mpldate,closep = datax[['mpldatetime','close']].T.values
		dg, dh = vertex_locator(closep,npar=npar,debugTF=debugTF)
		trend = dg['trend'].values
		#axt = ax.twinx()
		#ax.plot(mpldate[-SP:],trend[-SP:], color='cyan',alpha=.5)
		ax.plot(mpldate[dg.index.values],trend, color='cyan',alpha=.5)
		if debugTF:
			pqint( "time/trend: ",mpldate[dg.index.values],trend, file=sys.stderr)

	if 'volume' in datax or 'marketVolume' in datax:
		if 'volume' not in datax and 'marketVolume' in datax:
			datax.rename(columns={'marketVolume': 'volume'},inplace=True)
		mpldate,volume = datax[['mpldatetime','volume']].T.values
		axv = ax.twinx()
		axv.fill_between(mpldate[-SP:],0, volume[-SP:], facecolor='lightblue', alpha=.4)
		axv.axes.yaxis.set_ticklabels([])
		axv.grid(False)
		###Edit this to 3, so it's a bit larger
		axv.set_ylim(0, 3*volume.max())
	if len(title)>0:
		ax.set_title("")
	if block is True:
		plt.show(ax)
	return ax

def plot_candlestickCombo(data,stock,MA1=5,MA2=30,savePng=False,block=False,pngDIR='.',chartType='chart',trendTF=False,npar=15,debugTF=False,colorUD=['green','red'],title=None):
	'''
	plot candlestick ohlc/macd/rsi/ma combo graph
	'''
	colorup,colordown = colorUD
	# prepare data
	if 'mpldatetime' not in data:
		data['mpldatetime'] = [ mdates.date2num(x) for x in data.index ]
	newAr = data[['mpldatetime','open','high','low','close','volume']].values
	mpldate, closep, volume = data[['mpldatetime','close','volume']].T.values
	rsi,macd,ema9,macdsigv = data[['rsi','macd_ema','signal_macd','signal_value_macd']].T.values
	ma1c,ma2c = 'ma{}'.format(MA1),'ma{}'.format(MA2)
	Av1, Av2 = data[[ma1c,ma2c]].T.values
	#SP = len(mpldate[MA2-1:])
	SP = newAr.shape[0]

	# start plotting
	fig = plt.figure(facecolor='#07000d',figsize=(11,6))
	ax1 = plt.subplot2grid((6,4), (1,0), rowspan=4, colspan=4, facecolor='#07000d')
	if chartType == 'minute':
		wadj = 24*60
		dtfmt = '%H:%M'
		bym = [0,15,30,45] if SP<=120 else [0,30] if SP<=360 else [0]
	else:
		wadj = 1
		dtfmt = '%m/%d/%y'
	candlestick_ohlc(ax1, newAr[-SP:], width=.6/wadj, colorup=colorup, colordown=colordown)
	ax1.xaxis_date()
	ax1.autoscale_view()
	Label1 = str(MA1)+' SMA'
	Label2 = str(MA2)+' SMA'

	ax1.plot(mpldate[-SP:],Av1[-SP:],'#e1edf9',label=Label1, linewidth=1.5)
	ax1.plot(mpldate[-SP:],Av2[-SP:],'#4ee6fd',label=Label2, linewidth=1.5)

	if trendTF is True:
		from _alan_pppscf import vertex_locator
		dg, dh = vertex_locator(closep,npar=npar,debugTF=debugTF)
		trend = dg['trend'].values
		ax1.plot(mpldate[dg.index.values],trend, label='Trend',color='cyan',alpha=.5)
		#ax1.plot(mpldate[-SP:],trend[-SP:], label='Trend',color='cyan',alpha=.5)
		if debugTF is True:
			pqint( "trendline:\n",dh, file=sys.stderr)

	ax1.grid(True, color='w',linestyle='dotted',linewidth=0.5)
	if chartType == 'minute':
		xlocator = mdates.MinuteLocator(byminute=bym, interval = 1)
		ax1.xaxis.set_major_locator(xlocator)
	else:
		nbins=8
		nsp = (SP/nbins) if SP>nbins*2 else SP
		bymd = [1,5,10,15,20,25] if SP<50 else [1,15] if SP<120 else [1]
		itv = 1 if SP<160 else int(nsp/30.+0.97)
		xlocator = mdates.MonthLocator(bymonthday=bymd,interval=itv)
		ax1.xaxis.set_major_locator(xlocator)
		# check if min/max of xaxis should be included major ticks
		xtcks = list(ax1.get_xticks())
		x1,x2 = xtcks[:2]
		xmin,xmax = ax1.get_xlim()
		if (x1-xmin)>(x2-x1)*0.6:
			xtcks = [xmin] + xtcks
		if (xmax-xtcks[-1])>(x2-x1)*0.6:
			xtcks = xtcks + [xmax]
		ax1.set_xticks(xtcks)
		ax1.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))
		#original
		#ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))

	ax1.xaxis.set_major_formatter(mdates.DateFormatter(dtfmt))
	ax1.yaxis.label.set_color("w")
	ax1.spines['bottom'].set_color("#5998ff")
	ax1.spines['top'].set_color("#5998ff")
	ax1.spines['left'].set_color("#5998ff")
	ax1.spines['right'].set_color("#5998ff")
	ax1.tick_params(axis='y', color='w')
	plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
	ax1.tick_params(axis='x', color='w')
	plt.ylabel('Stock price and Volume')

	maLeg = plt.legend(loc=9, ncol=2, prop={'size':7},
			   fancybox=True, borderaxespad=0.)
	maLeg.get_frame().set_alpha(0.4)
	textEd = pylab.gca().get_legend().get_texts()
	pylab.setp(textEd[0:5], color = 'w')

	volumeMin = 0

	ax0 = plt.subplot2grid((6,4), (5,0), sharex=ax1, rowspan=1, colspan=4, facecolor='#07000d')
	rsiCol = '#c1f9f7'
	posCol = '#386d13'
	negCol = '#8f2020'

	ax0.plot(mpldate[-SP:], rsi[-SP:], rsiCol, linewidth=1.5)
	ax0.axhline(70, color=negCol)
	ax0.axhline(30, color=posCol)
	ax0.fill_between(mpldate[-SP:], rsi[-SP:], 70, where=(rsi[-SP:]>=70), facecolor=negCol, edgecolor=negCol, alpha=0.5)
	ax0.fill_between(mpldate[-SP:], rsi[-SP:], 30, where=(rsi[-SP:]<=30), facecolor=posCol, edgecolor=posCol, alpha=0.5)
	plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
	ax0.set_yticks([30,70])
	ax0.yaxis.label.set_color("w")
	ax0.spines['bottom'].set_color("#5998ff")
	ax0.spines['top'].set_color("#5998ff")
	ax0.spines['left'].set_color("#5998ff")
	ax0.spines['right'].set_color("#5998ff")
	ax0.tick_params(axis='y', color='w')
	ax0.tick_params(axis='x', color='w')
	plt.ylabel('RSI')

	ax1v = ax1.twinx()
	ax1v.fill_between(mpldate[-SP:],volumeMin, volume[-SP:], facecolor='lightgray', alpha=.4,step='pre')
	ax1v.axes.yaxis.set_ticklabels([])
	ax1v.grid(False)
	###Edit this to 3, so it's a bit larger
	ax1v.set_ylim(0, 3*volume.max())
	ax1v.spines['bottom'].set_color("#5998ff")
	ax1v.spines['top'].set_color("#5998ff")
	ax1v.spines['left'].set_color("#5998ff")
	ax1v.spines['right'].set_color("#5998ff")
	ax1v.tick_params(axis='x', color='w')
	ax1v.tick_params(axis='y', color='w')

	ax2 = plt.subplot2grid((6,4), (0,0), sharex=ax1, rowspan=1, colspan=4, facecolor='#07000d')
	fillcolor = '#00ffe8'
	ax2.plot(mpldate[-SP:], macd[-SP:], color='#4ee6fd', lw=2)
	ax2.plot(mpldate[-SP:], ema9[-SP:], color='#e1edf9', lw=1)
	# calc (macd-ema9) as signal_macd  
	ax2.fill_between(mpldate[-SP:], macdsigv[-SP:], 0, alpha=0.5, facecolor=fillcolor, edgecolor=fillcolor)

	plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
	ax2.spines['bottom'].set_color("#5998ff")
	ax2.spines['top'].set_color("#5998ff")
	ax2.spines['left'].set_color("#5998ff")
	ax2.spines['right'].set_color("#5998ff")
	ax2.tick_params(axis='x', colors='w')
	ax2.tick_params(axis='y', colors='w')
	plt.ylabel('MACD', color='w')
	ax2.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5, prune='upper'))

	if title is None or len(title)<1:
		title=stock.upper()
	plt.suptitle(title,color='w',fontproperties=prop)

	plt.setp(ax0.get_xticklabels(), rotation=30, visible=True)
	plt.setp(ax2.get_xticklabels(), rotation=30, visible=False)
	plt.setp(ax1.get_xticklabels(), rotation=30, visible=False)
	"""
	locNews=int(len(mpldate)*.66)
	ax1.annotate('News!',(mpldate[locNews],Av1[locNews]),
		xytext=(0.8, 0.9), textcoords='axes fraction',
		arrowprops=dict(facecolor='white', shrink=0.05),
		fontsize=14, color = 'w',
		horizontalalignment='right', verticalalignment='bottom')
	"""

	plt.subplots_adjust(left=.09, bottom=.14, right=.94, top=.95, wspace=.20, hspace=0.10)

	wintitle="Candlestick OHLC View"
	fig.canvas.set_window_title(wintitle)
	if savePng is True:
		chartName='{0}/OHLC_MACD_RSI_{1}.png'.format(pngDIR,stock.upper())
		fig.savefig(chartName,facecolor=fig.get_facecolor())
	else :
		plt.show(block=block)
	return fig, [ax0,ax1,ax2]

def graphData(data,stock,MA1=5,MA2=30,savePng=False,block=False,pngDIR='.',chartType = 'chart'):
	newAr = data[['mpldatetime','open','high','low','close','volume']].values
	mpldate, closep, volume = data[['mpldatetime','close','volume']].T.values
	Av1 = movingaverage(closep, MA1)
	Av2 = movingaverage(closep, MA2)
	SP = len(mpldate[MA2-1:])

	fig = plt.figure(facecolor='#07000d')
	ax1 = plt.subplot2grid((6,4), (1,0), rowspan=4, colspan=4, facecolor='#07000d')
	if chartType == 'minute':
		wadj = 24*60
		dtfmt = '%m-%d %H:%M'
	else:
		wadj = 1
		dtfmt = '%y-%m-%d'
	candlestick_ohlc(ax1, newAr[-SP:], width=.6/wadj, colorup='red', colordown='darkgray')
	ax1.xaxis_date()
	ax1.autoscale_view()
	Label1 = str(MA1)+' SMA'
	Label2 = str(MA2)+' SMA'

	ax1.plot(mpldate[-SP:],Av1[-SP:],'#e1edf9',label=Label1, linewidth=1.5)
	ax1.plot(mpldate[-SP:],Av2[-SP:],'#4ee6fd',label=Label2, linewidth=1.5)

	ax1.grid(True, color='w')
	ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
	ax1.xaxis.set_major_formatter(mdates.DateFormatter(dtfmt))
	ax1.yaxis.label.set_color("w")
	ax1.spines['bottom'].set_color("#5998ff")
	ax1.spines['top'].set_color("#5998ff")
	ax1.spines['left'].set_color("#5998ff")
	ax1.spines['right'].set_color("#5998ff")
	ax1.tick_params(axis='y', color='w')
	plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
	ax1.tick_params(axis='x', color='w')
	plt.ylabel('Stock price and Volume')

	maLeg = plt.legend(loc=9, ncol=2, prop={'size':7},
			   fancybox=True, borderaxespad=0.)
	maLeg.get_frame().set_alpha(0.4)
	textEd = pylab.gca().get_legend().get_texts()
	pylab.setp(textEd[0:5], color = 'w')

	volumeMin = 0

	ax0 = plt.subplot2grid((6,4), (0,0), sharex=ax1, rowspan=1, colspan=4, facecolor='#07000d')
	rsi = rsiFunc(closep)
	rsiCol = '#c1f9f7'
	posCol = '#386d13'
	negCol = '#8f2020'

	ax0.plot(mpldate[-SP:], rsi[-SP:], rsiCol, linewidth=1.5)
	ax0.axhline(70, color=negCol)
	ax0.axhline(30, color=posCol)
	ax0.fill_between(mpldate[-SP:], rsi[-SP:], 70, where=(rsi[-SP:]>=70), facecolor=negCol, edgecolor=negCol, alpha=0.5)
	ax0.fill_between(mpldate[-SP:], rsi[-SP:], 30, where=(rsi[-SP:]<=30), facecolor=posCol, edgecolor=posCol, alpha=0.5)
	ax0.set_yticks([30,70])
	ax0.yaxis.label.set_color("w")
	ax0.spines['bottom'].set_color("#5998ff")
	ax0.spines['top'].set_color("#5998ff")
	ax0.spines['left'].set_color("#5998ff")
	ax0.spines['right'].set_color("#5998ff")
	ax0.tick_params(axis='y', color='w')
	ax0.tick_params(axis='x', color='w')
	plt.ylabel('RSI')

	ax1v = ax1.twinx()
	ax1v.fill_between(mpldate[-SP:],volumeMin, volume[-SP:], facecolor='#00ffe8', alpha=.4)
	ax1v.axes.yaxis.set_ticklabels([])
	ax1v.grid(False)
	###Edit this to 3, so it's a bit larger
	ax1v.set_ylim(0, 3*volume.max())
	ax1v.spines['bottom'].set_color("#5998ff")
	ax1v.spines['top'].set_color("#5998ff")
	ax1v.spines['left'].set_color("#5998ff")
	ax1v.spines['right'].set_color("#5998ff")
	ax1v.tick_params(axis='x', color='w')
	ax1v.tick_params(axis='y', color='w')
	ax2 = plt.subplot2grid((6,4), (5,0), sharex=ax1, rowspan=1, colspan=4, facecolor='#07000d')
	fillcolor = '#00ffe8'
	nslow = 26
	nfast = 12
	nema = 9
	# calc ema_slow, ema_fast, macd_ema
	emaslow, emafast, macd = computeMACD(closep)
	# calc signal_macd
	ema9 = ExpMovingAverage(macd, nema)
	ax2.plot(mpldate[-SP:], macd[-SP:], color='#4ee6fd', lw=2)
	ax2.plot(mpldate[-SP:], ema9[-SP:], color='#e1edf9', lw=1)
	# calc (macd-ema9) as signal_macd  
	ax2.fill_between(mpldate[-SP:], macd[-SP:]-ema9[-SP:], 0, alpha=0.5, facecolor=fillcolor, edgecolor=fillcolor)

	plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
	ax2.spines['bottom'].set_color("#5998ff")
	ax2.spines['top'].set_color("#5998ff")
	ax2.spines['left'].set_color("#5998ff")
	ax2.spines['right'].set_color("#5998ff")
	ax2.tick_params(axis='x', colors='w')
	ax2.tick_params(axis='y', colors='w')
	plt.ylabel('MACD', color='w')
	ax2.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5, prune='upper'))
	for label in ax2.xaxis.get_ticklabels():
		label.set_rotation(45)

	plt.suptitle(stock.upper(),color='w')

	plt.setp(ax0.get_xticklabels(), visible=False)
	plt.setp(ax1.get_xticklabels(), visible=False)
	locNews=int(len(mpldate)*.66)
	"""
	ax1.annotate('News!',(mpldate[locNews],Av1[locNews]),
		xytext=(0.8, 0.9), textcoords='axes fraction',
		arrowprops=dict(facecolor='white', shrink=0.05),
		fontsize=14, color = 'w',
		horizontalalignment='right', verticalalignment='bottom')
	"""

	plt.subplots_adjust(left=.09, bottom=.14, right=.94, top=.95, wspace=.20, hspace=0.10)

	wintitle="Candlestick OHLC View"
	fig.canvas.set_window_title(wintitle)
	if savePng is True:
		chartName='{0}/OHLC_MACD_RSI_{1}.png'.format(pngDIR,stock.upper())
		fig.savefig(chartName,facecolor=fig.get_facecolor())
	else :
		plt.show(block=block)

def graphOHLC(data,ticker,fields=['buying_ratio'],days=10,stacked=False,kind='bar',block=False,ax=None,dtfmt='%m-%d-%y'):
	nd=data[fields][-days:]
	pdate=list(map(lambda x:x.strftime(dtfmt),nd.index))
	nd.index=nd.index.astype(unicode)
	nd.index=pdate
	nd.plot(ax=ax,title=ticker,grid=True,kind=kind,stacked=stacked,color=['r', 'darkgray', 'b', 'c'])
	for label in ax.xaxis.get_ticklabels():
		label.set_rotation(15)
	ax.legend(loc="lower left")
	return ax

def bsc_plot(data,stock,days=7,block=False,savePng=False,pngDIR='.',dtfmt='%m-%d-%y'):
	'''
	buy-sell-combo 7 days summary
	'''
	#fig=plt.gcf()
	data.set_index(data.pbdatetime,inplace=True)
	fig,(ax1,ax2)=plt.subplots(2,sharex='row')
	ax1=graphOHLC(data,stock,fields=['buying_ratio','selling_ratio'],stacked=True,days=days,ax=ax1,dtfmt=dtfmt)
	plt.setp(ax1.get_xticklabels(), visible=False)
	ax2=graphOHLC(data,stock,fields=['buying_ratio','selling_pressure','candle_ratio'],days=days,ax=ax2,dtfmt=dtfmt)
	ax2.set_title("")
	wintitle="Buy_Sell_Candle"
	if wintitle is not None:
		fig.canvas.set_window_title(wintitle)
	if savePng is True:
		chartName='{0}/{1}_{2}.png'.format(pngDIR,wintitle,stock.upper())
		fig.savefig(chartName,facecolor=fig.get_facecolor())
	else:
		plt.show(block=block)
	return fig

#def run_alan_plot(tkLst,start,end,ma1,ma2,savePng,pngDIR,chartType='chart',bscTF='False'):
def run_alan_plot(tkLst,opts=None,optx=None):
	if opts is None:
		opts, _ = opt_alan_plot([])
	if optx is not None:
		opts.update(optx)
	for ky,va in opts.items():
		exec("{}=va".format(ky))
	colorUD = ['red','green'] if lang=='cn' else ['green','red']
	if pngDIR is None or os.path.isdir(pngDIR) is False:
		pngDIR = "/home/web/bb_site/html/images"
	if debugTF is True:
		pqint( "===options:{}".format(opts), file=sys.stderr)
	for stock in tkLst:
		try:
			if chartType == 'minute':
				optx={'gap':'1m','ranged':'1d','outTF':False}
				datax = pull_stock_data(stock,**optx)
				pqint( datax.tail(), file=sys.stderr)
		
			else:
				datax = pull_stock_data(stock,start,end)
		except Exception as e:
			pqint( "***Data ERROR:", str(e), file=sys.stderr)
			continue
		if chartType == 'minute' and 'epochs' in datax:
			datax['pbdatetime'] = datax.epochs.apply(epoch_parser)
			dtfmt = '%H:%M %b %d'
		else:
			datax['pbdatetime'] = datax.pbdate.apply(ymd_parser)
			dtfmt = '%m-%d-%y'
		try:
			block = False if bscTF is True else True
			datax.dropna(inplace=True)
			pqint( datax.tail(), file=sys.stderr)
			if chartType != 'minute':
				datax.reset_index(inplace=True)
			datax['mpldatetime'] = datax.pbdatetime.apply(mdates.date2num)
			#- calc macd & rsi 
			datax = run_tech(datax, pcol='close',winLst=[ma1,ma2],debugTF=debugTF,nanTF=True)
			fig, axes = plot_candlestickCombo(datax,stock,ma1,ma2,savePng=savePng,block=block,pngDIR=pngDIR,chartType=chartType,trendTF=trendTF,npar=npar,debugTF=debugTF,colorUD=colorUD,title=title)
			if savePng is False:
				plt.show(axes)
			# DEPRECATED, use run_tech() + plot_candlestickCombo()
			#graphData(datax,stock,ma1,ma2,savePng=savePng,block=block,pngDIR=pngDIR,chartType=chartType)
			if bscTF is True:
				run_ohlc(datax)
				bsc_plot(datax,stock,days=7,savePng=savePng,block=True,pngDIR=pngDIR,dtfmt=dtfmt)
		except Exception as e:
			pqint( str(e), file=sys.stderr)
	return datax

def cgi_alan_plot():
	xf, _ = opt_alan_plot()
	xf['savePng'] = True
	xf['dataTF'] = True
	#xf={'ma1': 5, 'start': None, 'ma2': 30, 'end': None, 'savePng': True,'chartType':'chart','bscTF':False,'dataTF':False}
	cgitb.enable(display=0, logdir="/apps/fafa/cronJob/log/cgi-bin/")
	cgitb.enable(format='text')
	mf = cgi.FieldStorage()
	for ky in mf:
		try:
			if ky in ['ma1', 'start','ma2', 'end']:
				xf[ky]=int(mf[ky].value)
			elif ky in ['savPng','dataTF','bscTF']:
				xf[ky]=False if mf[ky].value=='0' else True
			else:
				xf[ky]=mf[ky].value
		except:
			continue
	if 'ticker' in xf:
		ticker=xf['ticker']
	else:
		ticker='AAPL'
	pqint( xf, file=sys.stderr)
	data = run_alan_plot([ticker],xf)
	data.volume=data.volume.astype(int)
	pd.set_option('precision', 4)

	pngDIR = xf['pngDIR']
	if pngDIR is None or os.path.isdir(pngDIR) is False:
		pngDIR = "/home/web/bb_site/html/images"

	pqint("Content-type:text/html;charset=utf-8\r\n\r\n",file=sys.stdout)
	pqint("<PRE>",file=sys.stdout)

	imgCmd = '<P><img src="get_image.py?file=OHLC_MACD_RSI_{0}.png&folder={1}"></P>'.format(ticker,pngDIR)
	pqint(imgCmd,file=sys.stdout)

	if xf['bscTF'] is True:
		imgCmd = '<P><img src="get_image.py?file=Buy_Sell_Candle_{0}.png&folder={1}"></P>'.format(ticker,pngDIR)
		pqint(imgCmd,file=sys.stdout)
	if xf['dataTF'] is True:
		pqint(data.to_html(index=False),file=sys.stdout)
	return(data)

def opt_alan_plot():
	parser = OptionParser(usage="usage: %prog [option] SYMBOL1 ...", version="%prog 0.61",description="To chart OHLC, MACD & RSI")
	parser.add_option("-s","--start",action="store",dest="start",
		help="start YYYY-MM-DD (default: 3-year-ago)")
	parser.add_option("-e","--end",action="store",dest="end",
		help="end YYYY-MM-DD (default: today)")
	parser.add_option("","--title",action="store",dest="title",
		help="plot TITLE")
	parser.add_option("","--ma1",action="store",dest="ma1",default=5,type="int",
		help="1st MA Window (default: 5)")
	parser.add_option("","--ma2",action="store",dest="ma2",default=30,type="int",
		help="2nd MA Window 2 (default: 30)")
	parser.add_option("-l","--lang",action="store",dest="lang",default="en",
		help="language mode [cn|en] (default: en)")
	parser.add_option("","--chart",action="store",dest="chartType",default="chart",
		help="chart type [chart|minute] (default: chart)")
	parser.add_option("","--save_png",action="store_true",dest="savePng",default=False,
		help="save graph as png? (default: FALSE)")
	parser.add_option("","--bsc",action="store_true",dest="bscTF",default=False,
		help="show buy-sell-combo graph? (default: FALSE)")
	parser.add_option("","--dirname",action="store",dest="pngDIR",
		help="directory to save png file")
	parser.add_option("","--trendline",action="store_true",dest="trendTF",default=False,
		help="Draw trendline, apply to the 1st array ONLY")
	parser.add_option("","--npar",action="store",dest="npar",default=15,type="int",
		help="trendline fitting polynomial degree (default: 15)")
	parser.add_option("","--debug",action="store_true",dest="debugTF",default=False,
		help="debugging (default: False)")
	(options, args) = parser.parse_args()
	return (vars(options), args)

if __name__ == '__main__':
	xreq=os.getenv('REQUEST_METHOD')
	if xreq in ('GET','POST') : # WEB MODE
		data=cgi_alan_plot()
	else:
		(options, args)=opt_alan_plot()
		if len(args) == 0:
			pqint("\nRead from stdin\n\n", file=sys.stderr)
			tkLst = sys.stdin.read().strip().split("\n")
		elif args[0] == '-':
			pqint("\nRead from stdin\n\n", file=sys.stderr)
			tkLst = sys.stdin.read().strip().split("\n")
		else:
			tkLst = args[0:]
		pqint( (options, args), file=sys.stderr)

		if options['pngDIR'] is not None and os.path.isdir(options['pngDIR']) :
			pngDIR = options['pngDIR'] 
		run_alan_plot(tkLst,options)
		#run_alan_plot(tkLst,options['start'],options['end'],options['ma1'],options['ma2'],options['savePng'],pngDIR,options['chartType'],options['bscTF'])
