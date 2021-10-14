#!/usr/bin/env python
"""
Description: regressing TICKER on SERIES with DEGREE power of polynomial function

Examples,
python stats_tst.py 
python stats_tst.py ^GSPC T5YIFR --title="SP500 vs Inflation"
python stats_tst.py ^GSPC CPIAUCNS_PCTCHG --title="SP500 vs CPI"
python stats_tst.py ^GSPC PPIACO_PCTCHG --title="SP500 vs PPI"
printf "SELECT (m2.value-m1.value) as value, m1.pbdate FROM macro_hist_fred m1,macro_hist_fred m2 WHERE m1.series='DGS3MO' AND m2.series='DGS10' AND m1.pbdate=m2.pbdate and m1.pbdate>20000101 ORDER BY m1.pbdate" | psql.sh -d ara  | grep -v rows | python stats_tst.py ^GSPC - --debug --title="SP500 vs. Treasury 10y-3MO"
printf "SELECT (m2.value-m1.value) as value, m1.pbdate FROM macro_hist_fred m1,macro_hist_fred m2 WHERE m1.series='DGS3MO' AND m2.series='DGS10' AND m1.pbdate=m2.pbdate and m1.pbdate>20000101 ORDER BY m1.pbdate" | psql.sh -d ara  | grep -v rows | python stats_tst.py - T5YIFR --debug --title="Treasury 10y-3MO vs. Inflation"
printf "SELECT m1.*,m2.freq FROM macro_hist_fred m1,mapping_series_label m2 WHERE m1.series='USSTHPI_PCTCHG' AND m1.series=m2.series and m1.pbdate>19900101 ORDER BY m1.pbdate" | psql.sh -d ara  | grep -v rows | python stats_tst.py - CPIAUCNS_PCTCHG --debug --pct_chg_prd=0 --start=1990-01-01 --deg=1 --title="HPI vs CPI"
#- GOOD
python stats_tst.py HPIPONM226S_PCTCHG CPIAUCNS_PCTCHG --title="HPA vs CPI" --pct_chg_prd=0 --src=fred
#- GOOD
python stats_tst.py UNRATE CPIAUCNS_PCTCHG --title="Unemployment vs CPI" --pct_chg_prd=1 --src=fred --deg=2 --lag=0 --debug
#- GOOD (TICKER2 via stdin)
printf "SELECT (m2.value-m1.value) as value, m1.pbdate FROM macro_hist_fred m1,macro_hist_fred m2 WHERE m1.series='DGS3MO' AND m2.series='DGS10' AND m1.pbdate=m2.pbdate and m1.pbdate>19900101 ORDER BY m1.pbdate" | psql.sh -d ara  | grep -v rows | python stats_tst.py T5YIFR - --debug --title="Inflation vs. Treasury 10y-3MO" --start=1990-01-01 --deg=2 --pct_chg_prd=0 --src=fred
#- GOOD (both TICKER1&2 via --file with stdin)
#- Note, inputs must contain ticker/series, close/value, pbdate columns
printf "select series as ticker,* from macro_hist_fred where series in ('GOLDAMGBD228NLBM','DCOILBRENTEU') and pbdate>20180101 order by pbdate" | psql.sh -d ara | grep -v rows | python stats_tst.py GOLDAMGBD228NLBM DCOILBRENTEU  --title="Gold vs. Oil" --pct_chg_prd=0 --pct_chg_prd2=0 --lag=0 --log --log2  --src=fred --src2=fred --no_monthly --file=-
#- TBD 
printf "SELECT (m2.value-m1.value) as value, m1.pbdate FROM macro_hist_fred m1,macro_hist_fred m2 WHERE m1.series='DGS3MO' AND m2.series='DGS10' AND m1.pbdate=m2.pbdate and m1.pbdate>19900101 ORDER BY m1.pbdate" | psql.sh -d ara  | grep -v rows | python stats_tst.py - T5YIFR --debug --title="Treasury 10y-3MO vs. Inflation" --start=1990-01-01 --deg=2 --pct_chg_prd=0
#- TBD, 
python stats_tst.py DGS2 T5YIFR --title="2YR-Treasury vs Inflation" --src=fred --pct_chg_prd=0

Last mod., Mon Oct 22 16:02:55 EDT 2018
"""
import sys
sys.path.append("/apps/fafa/pyx/tst/")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import json
from _alan_calc import pull_stock_data as psd,get_datai as gdi,ewma_smooth,sma,subDict
from _alan_date import freq_d2m as fqd2m, ymd_parser
from scipy.optimize import curve_fit # non-linear least squares f() fitting
import statsmodels.api as sm # OLS/GLS regression fitting
#import statsmodels.formula.api as smf # using formula
#import statsmodels.tsa.api as smt # time-series ARIMA/GARCH fitting
#from arch import arch_model # arch model
from argparse_tst import parse_args
#plt.style.use('dark_background')
plt.style.use('classic')
plt.switch_backend('tkAgg')


def df_read_file(filename,sep='|',columns=None,debugTF=False):
	"""
	Use pandas.read_csv to read [sep] delimiter csv file from [filename]
	Where [filename] - for stdin
	Requirement: import pandas as pd
	"""
	if debugTF:
		print >> sys.stderr, filename,sep,columns
	if filename=='-':
		df=pd.read_csv(sys.stdin,sep=sep)
	else:
		df=pd.read_csv(filename,sep=sep)
	if columns is not None:
		df =  df[ list(set(df.columns) & set(columns.split(','))) ]
	return df

def plot_scatter(vx,*vyLst,**opts):
	"""
	Create scatter plot on XAxis[vx] for YAxis [vyLst] vectors
	Example: 
	import numpy as np
	vy1 = np.random.random(10)
	vy2 = np.random.random(10)
	plot_scatter(range(10),vy1,vy2)
	"""
	marks = ['o', '.', 'x', '+', 'v', '<', '>']
	colors= ['red','blue','green','gray','salmon']
	fig, ax=plt.subplots(figsize=(11,6))
	for j, vy in enumerate(vyLst):
		plt.scatter(vx,vy, c=colors[j%6], marker=marks[j%7])
	if 'titlename' in opts and opts['titlename'] is not None:
		plt.title(opts['titlename'])
	if 'pngname' in opts and opts['pngname'] is not None and len(opts['pngname'])>4:
		fig.savefig(opts['pngname'])
	else:
		plt.show()

def run_polyfit(xdata, ydata, deg=2, debugTF=False):
	"""
	polyfit [ydata] over [xdata] for [deg] degree of power
	"""
	params = np.polyfit(xdata,ydata,deg)
	fitted = np.polyval(params, xdata)
	yfunc = np.poly1d(params)
	zroots = sorted(map(lambda x: round(x.real,2), yfunc.roots[~np.iscomplex(yfunc.roots)]))
	if deg > 1:
		ydfunc = yfunc.deriv(m=1)
		zvtx = map(lambda x: round(x.real,2), ydfunc.roots[~np.iscomplex(ydfunc.roots)])
	else:
		zvtx = []
	if debugTF:
		print >> sys.stderr, "params: {}\nzeroRoots: {}\nVertices: {}".format(params, zroots, zvtx)
	return (params, zroots, zvtx, fitted)

def run_OLS(xdata,ydata, const=True, api=None, model='OLS', debugTF=False):
	"""
	OLS to fit ydata over xdata for statsmodels.api.OLS
	Where
		api: statsmodels.api	
		model: model selection of the statsmodels.api list
	Also see, GLS
	"""
	if const:
		xdata = sm.add_constant(xdata)
	try: 
		reg = getattr(api,model)
		rst = reg(ydata, xdata)
		if debugTF:
			print >> sys.stderr, rst.fit().summary()
	except Exception as e:
		print >> sys.stderr, str(e)
		rst={}
	return rst

def polyFnc(xdata,*params):
	"""
	customed function for scipy.optimize.curve_fit as nls-fitting 
	Also see, numpy.polyfit
	"""
	return np.polyval(params,xdata)

def merge2df(m1,m2,keys=['yyyymm'],method=None,lagd=1,debugTF=False):
	# create x, y as [dh] dataframe
	dh = pd.merge(m1,m2,on=keys)
	yname = m1.columns[0]
	dh['xData'] = dh['rate'].shift(lagd) 
	if method is not None and method in globals():
		if debugTF:
			print >> sys.stderr, "Data smoothing Method [{}] applied to {}".format(method,yname)
		dh['yData']=globals()[method](dh['yData'],5)
		dh['xData']=globals()[method](dh['xData'],5)
	else:
		print >> sys.stderr, "Data smoothing Method {} to {}".format(method,yname)
	if 'pbdate' in dh.columns:
		dh.set_index(pd.DatetimeIndex(map(ymd_parser,dh['pbdate'])),inplace=True)
	dh = dh.dropna()
	return dh

def get_dataXY(xname='T5YIFR', yname='^GSPC',start=None,debugTF=True,method=None,src2='fred',src='yahoo',freq2='D',freq='D',pct_chg_prd2=0,pct_chg_prd=1,lagd=1,log2TF=False,logTF=False,filename=None,monthlyTF=True,days=365):
	""" 
	Get xname/yname as TICKER2/TICKER1 to pull data: xdata/ydata
	Then regress ydata on xdata 
	"""
	# get x,y values from filename, must contains ticker, close/value, pbdate columns
	try:
		dxy = df_read_file(filename) if filename is not None else {}
		coltk='ticker' if 'ticker' in dxy else 'series'
	except Exception as e:
		dxy = {}

	# assign y values
	if dxy is not None and len(dxy)>0:
		d1 = dxy.query("{}=='{}'".format(coltk,yname)).copy()
	elif yname == '-':
		d1 = df_read_file(yname)
	else:
		d1 = psd(yname,src=src,start=start,days=days)
	if not d1.size:
		return []
	if 'freq' in d1.columns:
		freq = d1['freq'][0]
	
	if monthlyTF and freq == 'D':
		m1 = fqd2m(d1)
	else:
		m1 = d1

	ycolname = 'close' if 'close' in m1 else 'value'
	m1['yData'] = m1[ycolname].pct_change(pct_chg_prd)*100 if pct_chg_prd>0 else m1[ycolname]
	if logTF:
		m1['yData'] = np.log(m1['yData'])
	if monthlyTF:
		m1['yyyymm']=(m1['pbdate']/100).astype(int)
	else:
		m1['yyyymm']=(m1['pbdate']).astype(int)


	# assign x values
	if dxy is not None and len(dxy)>0:
		d2 = dxy.query("{}=='{}'".format(coltk,xname)).copy()
	elif xname == '-':
		d2 = df_read_file(xname)
	else:
		d2 = psd(xname,src=src2,start=start,days=days)
	if not d2.size:
		return []

	if 'freq' in d2.columns:
		freq2 = d2['freq'][0]

	if monthlyTF and freq2 == 'D':
		m2 = fqd2m(d2) 
	else:
		m2 = d2
	xcolname = 'close' if 'close' in m2 else 'value'
	m2['rate']=m2[xcolname]
	m2['rate'] = m2[xcolname].pct_change(pct_chg_prd2)*100 if pct_chg_prd2>0 else m2[xcolname]
	if log2TF:
		m2['rate'] = np.log(m2['rate'])
	if monthlyTF:
		m2['yyyymm']=(m2['pbdate']/100).astype(int)
	else:
		m2['yyyymm']=(m2['pbdate']).astype(int)
	dh = merge2df(m1[['yData','yyyymm','pbdate']],m2[['rate','yyyymm']],keys=['yyyymm'],method=method,lagd=lagd)
	return dh

def plot_XY(dh,xfmt="%Y%m",pngname=None,debugTF=True,titlename=None,labels=[]):
	"""
	time-series & scatter plot
	"""
	if pngname is None or len(pngname)<5:
		plt.switch_backend('tkAgg')
	else:
		plt.switch_backend('Agg')
	fig, ax=plt.subplots(figsize=(11,6))
	if titlename is None:
		titlename = 'Series'
	title1 = titlename + ' over time'
	dh.plot(ax=ax,title=title1,grid=True,color=['blue','red','green','gray','salmon','purple'])
	ax.grid(linestyle='dotted',linewidth=0.5)
	ax.xaxis.set_major_formatter( mdates.DateFormatter(xfmt))
	if pngname is not None and len(pngname)>4:
		xdu = pngname.replace('.png','_ts.png')
		fig.savefig(xdu)
	else:
		plt.show()
	if dh.shape[1] > 2:
		plot_scatter(dh.iloc[:,0],dh.iloc[:,1],dh.iloc[:,2],pngname=pngname,titlename=titlename)
	elif dh.shape[1] > 1:
		plot_scatter(dh.iloc[:,0],dh.iloc[:,1],pngname=pngname,titlename=titlename)
	if debugTF:
		print >> sys.stderr, dh.tail(50)

def polyfit_XY(series='T5YIFR',ticker='^GSPC',**opts):
	"""
	Regress [ydata] on [xdata] in polyfit() with [deg] power
	opts default values: {'src': 'yahoo', 'monthlyTF': True, 'start': None, 'debugTF': True,
		'pct_chg_prd2': 0, 'pct_chg_prd': 1, 'freq2': 'D', 'days': 450, 'src2': 'fred',
		'logTF': False, 'lagd': 1, 'freq': 'D', 'pngname': None, 'log2TF': False, 'method': None,
		'titlename': '^GSPC / T5YIFR', 'deg': 2}
	"""
	for ky,va in opts.items():
		exec("{}=va".format(ky))
	# GRAB data
	dh = get_dataXY(series,ticker,start=start,debugTF=debugTF,method=method,src2=src2,src=src,freq2=freq2,freq=freq,pct_chg_prd2=pct_chg_prd2,pct_chg_prd=pct_chg_prd,lagd=lagd,log2TF=log2TF,logTF=logTF,filename=filename,monthlyTF=monthlyTF,days=days)
	if not dh.size:
		return [],[],[],[]

	# RUN polyfit of [deg] degree of power
	pprm, zroots, zvtx, dh['fitted'] =  run_polyfit(dh['xData'],dh['yData'],deg,debugTF=debugTF)

	# PLOTTIG 
	plot_XY(dh[['xData','yData','fitted']],pngname=pngname,debugTF=debugTF,titlename=titlename,labels=[series,ticker,series+'Fit'])
	return (dh, pprm, zroots, zvtx)


def mainTst(description = "Regressing TICKER1 on TICKER2", \
optkys = ['deg','method','start','freq','src','pct_chg_prd','debugTF','pngname','titlename','lagd','pct_chg_prd2','freq2','src2','logTF','log2TF','filename','monthlyTF','days']):
	# ASSIGN options & arguments
	options, ns_args = parse_args(version="0.1",description=description,nargs='*')
	opts = subDict(options,optkys)
	for ky,va in opts.items():
		exec("{}=va".format(ky))

	# ASSIGN customed variables & parameters
	ticker='^GSPC'
	series='T5YIFR'
	args = options['tkLst']
	argc = len(args)
	if argc>1:
		ticker, series = args[:2]
	elif argc>0:
		ticker = args[0],
	if titlename is None:
		titlename = "{} / {}".format(ticker,series)
		opts['titlename'] = titlename
	if debugTF:
		print >> sys.stderr, "{}".format(opts)
		print >> sys.stderr, "series:{}, ticker:{}, start:{}, debug:{}".format(series, ticker, start, debugTF)
	
	# REGRESSING [ydata] on [xdata] in polyfit() with [deg] power
	#(dh, pprm, zroots, zvtx) = polyfit_XY(series, ticker, **opts)
	#return (titlename, dh, pprm, zroots, zvtx)

	# GRAB data
	dh = get_dataXY(series,ticker,start=start,debugTF=debugTF,method=method,src2=src2,src=src,freq2=freq2,freq=freq,pct_chg_prd2=pct_chg_prd2,pct_chg_prd=pct_chg_prd,lagd=lagd,log2TF=log2TF,logTF=logTF,filename=filename,monthlyTF=monthlyTF,days=days)

	if debugTF:
		print >> sys.stderr, dh
	# RUN polyfit of [deg] degree of power
	pprm, zroots, zvtx, dh['fitted'] =  run_polyfit(dh['xData'],dh['yData'],deg,debugTF=debugTF)
	# PLOTTIG 
	plot_XY(dh[['xData','yData','fitted']],pngname=pngname,debugTF=debugTF,titlename=titlename,labels=[series,ticker,series+'Fit'])

	# RUN OLS for compare with np.polyfit of deg=1
	oret=run_OLS(dh['xData'],dh['yData'],api=sm,debugTF=debugTF)

	# RUN curve_fit for nls-fitting to compare with np.polyfit
	cprm, ccov = curve_fit(polyFnc,dh['xData'],dh['yData'],p0=np.full(deg+1,1))
	if debugTF:
		print >> sys.stderr, pprm, cprm
	return (titlename, dh, pprm, zroots, zvtx, opts)

if __name__ == '__main__':
	title, dh, pprm, zroots, zvtx, opts = mainTst()
	print >> sys.stdout, json.dumps(dict(title=title,params=list(pprm),zroots=zroots,vertices=zvtx,opts=opts))
