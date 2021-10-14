#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" pppvl.py: Past Price Performance Vertex Locator
	Description: past price performance vertex locations
"""
import sys, pandas as pd, numpy as np
from StringIO import StringIO
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
from datetime import datetime
from _alan_str import popenCall, udfStr, roundUSD
from _alan_calc import ewma_smooth, extrapolate_series
from _alan_date import ymd_parser, epoch_parser
import re
sys.setdefaultencoding('utf8')

import matplotlib.font_manager as mfm
#font_path = "/usr/share/fonts/truetype/arphic/uming.ttc"
font_path = "/usr/share/fonts/truetype/arphic/ukai.ttc"
prop = mfm.FontProperties(fname=font_path)
plt.style.use('dark_background')


def locate_mnmx(vy, locx=0, sign=1, scroll=5):
	""" re-select tx locators to refine the min/max points
	"""
	nobs = len(vy)
	mnmx = max if sign >= 1 else (min if sign <= -1 else None)
	if mnmx is None:
		return locx
	start = max(locx - scroll, 0)
	end = min(locx + scroll, nobs)
	locy = mnmx(vy.iloc[start:end])
	vx = vy.iloc[start:end][vy == locy].index
	tx = vx[-1] if len(vx) else locx
	return tx


def vertex_locator(dy, vx=None, span=5, npar=15, scroll=5, debugTF=False):
	""" locate vertex of array [dy] via polyfit() 
		return dataframe dh[tx,ty,tz]
		where
			span: for running w.moving average [span] period
			npar: for [npar]_th polynomial fitting over time
			scroll: for refine the min-max vertex point +- [scroll] period
			tx are the locators
			ty are the correspoding fitted values
			tz are the correspoding actual values
	"""
	nobs = len(dy)
	dtmp = pd.DataFrame(dy.values, columns=['actual'], index=range(nobs))
	vy = dtmp['actual']
	if vx is None:
		vx = range(nobs)
	vma = ewma_smooth(vy, span) if span > 1 else vy
	params = np.polyfit(vx, vma, npar)
	yfunc = np.poly1d(params)
	ydfunc = yfunc.deriv(m=1)
	vfit = np.polyval(params, vx)
	tx = map(lambda x: int(x.real), ydfunc.roots[~np.iscomplex(ydfunc.roots)])
	tx = filter(lambda x: x >= 0 and x <= nobs - 1, tx)
	tx = sorted(set(tx + [0, nobs - 1]))
	ty = map(lambda x: yfunc(x), tx)
	ty[0] = vy.iloc[0]
	ty[-1] = vy.iloc[-1]
	tz = list(vy[tx])
	tsg=[0]
	tsg+=map(lambda x: 1 if x>0 else -1 if x <=0 else 0,np.diff(ty))
	dh=pd.DataFrame(zip(tx,ty,tz,tsg),columns=['iptday','actual', 'fitted','sign'])

	# re-select tx locator (to refine min/max point
	if scroll > 0:
		tx[1:-1]=map(lambda (x,y): locate_mnmx(vy,locx=x,sign=y,scroll=scroll),zip(tx[1:-1],tsg[1:-1]) )
		tx = sorted(set(tx))
		ty = map(lambda x: yfunc(x), tx)
		ty[0] = vy.iloc[0]
		ty[-1] = vy.iloc[-1]
		tz = list(vy[tx])
		tsg[1:] = map(lambda x: 1 if x>0 else -1 if x <=0 else 0,np.diff(tz))
		dh=pd.DataFrame(zip(tx,ty,tz,tsg),columns=['iptday','actual', 'fitted','sign'])
	else:
			print >> sys.stderr, 'MNMX scroll not apply!'
			dh[1:,'sign'] = map(lambda x: 1 if x>0 else -1 if x <=0 else 0,np.diff(tz))
	dh['difday'] = dh['iptday'].diff().fillna(0).astype('int')
	dh['date'] = dy.index[dh['iptday']]
	if debugTF is True:
		print >> sys.stderr, dh.head()
	vvtx = interp1d(tx, tz, fill_value='extrapolate')(vx)
	dg = pd.DataFrame(zip(vy, vfit, vvtx), columns=['actual', 'fitted', 'trend'], index=vx)
	if debugTF is True:
		print >> sys.stderr, dg.tail()
	return (dg, dh)


def set_time_index(dg, dh, dtCol=None, parser=epoch_parser):
	""" rearrange index as time-series
	"""
	dtIdx = map(parser, dtCol[dh['iptday']])
	dh.set_index(pd.DatetimeIndex(dtIdx), inplace=True)
	dg.set_index(pd.DatetimeIndex(map(parser, dtCol)), inplace=True)
	return ( dg, dh)


from _alan_str import jj_fmt

def mainTest(ticker='IBM', date=20180831, debugTF=True, span=5, npar=15, scroll=5,ax_text=''):
	if str(date).isdigit() and int(date)>12345678: # USE iex minute data via choosing YYYYMMDD date
		npar = 12
		xcmd = ('python iex_types_batch.py --date={} --no_database_save {}').format(date, ticker)
	elif str(date).isdigit(): # USE yahoo daily history via prc_hist or yahoo closing 
		xcmd = ('python -c "from _alan_calc import pull_stock_data as psd; print psd(\'{}\',days={}).to_csv(sep=\'|\',index=False);"').format(ticker,date)
	elif re.search(r'[=^]',ticker) is not None and str(date) in ['1d', '5d']: # USE yahoo minute data via 'yahoo' key word
		xcmd = ('python yahoo_minute_quotes.py {}').format(ticker)
	else: # USE ied daily/minute via choosing [ 1y | 1d ]
		xcmd = ('python iex_types_batch.py --types=chart --range={} --no_database_save {}').format(date, ticker)
	if debugTF is True:
		print >> sys.stderr, xcmd
	xstr, _ = popenCall(xcmd)
	if debugTF is True:
		print >> sys.stderr, xstr
	df = pd.read_csv(StringIO(xstr), sep='|')
	#df = pd.read_csv('tw2317.dat', sep='|')
	df['close'] = extrapolate_series(df['close'].astype('float'))
	dg, dh = vertex_locator(df['close'], vx=df.index, span=span, npar=npar, scroll=scroll, debugTF=debugTF)
	if 'epochs' in df or 'pbdate' in df:
		dtname = 'epochs' if 'epochs' in df else 'pbdate'
		parser = epoch_parser if dtname == 'epochs' else ymd_parser
		dg, dh = set_time_index(dg, dh, dtCol=df[dtname], parser=parser)
	if debugTF is True:
		print >> sys.stderr, dh
		print >> sys.stderr, dg.head(3)
		print >> sys.stderr, dg.tail(3)
		ax = dg[['actual','trend']].plot(title="{} - {}".format(ticker,dg.index[0].strftime("%Y-%m-%d")))
		if ax_text:
			plt.text(0.15,0.1,ax_text,fontsize=12,color='yellow',fontproperties=prop, transform=plt.gcf().transFigure)
		plt.show()
	return ( df, dg, dh)

if __name__ == '__main__':

	ticker='IBM' if len(sys.argv)<2 else sys.argv[1]
	date='1d' if len(sys.argv)<3 else sys.argv[2]
	scroll = 5 if len(sys.argv) < 4 else int(sys.argv[3])
	debugTF=True
	try:
		from _alan_calc import pull_stock_data as psd
		try:
			sp500PctChg=psd('^GSPC',days=7).close.pct_change().dropna().iloc[-1]
			sectorPctChg=psd('XLK',days=7).close.pct_change().dropna().iloc[-1]
		except:
			sp500PctChg,sectorPctChg = (0,0)
	
		#zf, zg, zh = mainTest(ticker='^GSPC', date='1d', debugTF=False, scroll=scroll) # SP500
		#xf, xg, xh = mainTest(ticker='XLK', date=date, debugTF=False, scroll=scroll) # Tech sector 
		hf, hg, hh = mainTest(ticker=ticker, date=date, debugTF=False, scroll=scroll)
		#df, dg, dh = mainTest(ticker=ticker, date='1y', debugTF=False, scroll=scroll)
	except Exception, e:
		print >> sys.stderr, "**ERROR:{}".format(str(e))
		exit(1)

	lang='cn'
	intra1_id = 1 if hh['sign'][1] != hh['sign'][2] else 2
	inix = hh['actual'][0]
	hh['iptchg'] =  map(lambda x:x-inix,hh['actual'])
	hh['iptsign'] = map(lambda x:1 if x>0 else -1 if x<0 else 0, hh['iptchg'])
	#print hh.to_dict(orient="records")
	intra1_udf = udfStr(hh['iptchg'][intra1_id],lang=lang)
	intra2_id = intra1_id + 1
	intra2_udf = udfStr(hh['iptchg'][intra2_id],lang=lang)
	noon_id = [j for j,x in enumerate(hh.iptday) if x>150][0]
	noon_udf = udfStr(np.diff(hh['actual'][[0,noon_id]]),lang=lang)
	noon_str = hh.index[noon_id].strftime("%H點%M分")
	end2_udf = udfStr(hh['iptchg'][-2],lang=lang)
	end1_udf = udfStr(hh['iptchg'][-1],lang=lang)
	end1_sign = hh['iptchg'][-1]*hh['iptchg'][-2]

	"""
	inix = zh['actual'][0]
	zh['iptchg'] =  map(lambda x:x-inix,zh['actual'])
	inix = xh['actual'][0]
	xh['iptchg'] =  map(lambda x:x-inix,xh['actual'])

	index_trend = udfStr(zh['iptchg'][-1],lang=lang)
	sector_trend = udfStr(xh['iptchg'][-1],lang=lang)
	sector_color = udfStr(xh['iptchg'][-1],['紅盤','黑','持平'])
	xz_sign=zh['iptchg'][-1] * xh['iptchg'][-1]
	"""
	index_trend = udfStr(sp500PctChg,lang=lang)
	sector_trend = udfStr(sectorPctChg,lang=lang)
	sector_color = udfStr(sectorPctChg,['紅盤','黑','持平'])
	xz_sign=sp500PctChg*sectorPctChg

	xzWd = udfStr(xz_sign,['亦是','卻',''],0.0001)
	closing_price=roundUSD(hh['actual'][-1],2)
	print >> sys.stderr, hh.head()
	#print >> sys.stderr, zh
	#print >> sys.stderr, xh

	ts_intraday = """
昨天{{label}}開盤小幅{{intra1_udf}}，早盤{{intra1_udf}}後{{intra2_udf}} 。
基本上在中午{{noon_str}}以前呈現{{noon_udf}}狀態。
{% if end1_sign<0 %}
臨近尾盤時，由{{end2_udf}}轉為{{end1_udf}}，收{{closing_price}} 。{{region_index}}集體{{index_trend}}，{{sector}}翻{{sector_color}}收了個{{sector_trend}}。
{% else %}
尾盤仍是{{end1_udf}}，收{{closing_price}} 。
{% endif %}
{%if index_trend %} 
{{region_index}}集體{{index_trend}}，{{sector}}{{xzWd}}收了個{{sector_trend}}終場。
{% endif %}
"""
	label = ticker
	region_index = '美股大盤'
	sector = '科技類股'
	dux = locals()
	ret = jj_fmt(ts_intraday,dux)
	#ret.replace('\n',' ')
	print >> sys.stderr, dux
	print ret
	hf, hg, hh = mainTest(ticker=ticker, date=date, debugTF=debugTF, scroll=scroll,ax_text=ret)
