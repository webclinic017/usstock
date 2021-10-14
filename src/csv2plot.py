#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Description: plot time series from a csv file
    Usage of:
	csv2plot.py file --sep=DELIMITER
    Example:
	# FROM data file
	python csv2plot.py csv2plot.dat
	# OR (near realtime data)
	iex_types_batch.py --types=chart --range=1d  --no_database_save AAPL | csv2plot.py - --columns=close,epochs  --xaxis=epochs --title=Apple
	# OR (daily data)
	iex_types_batch.py --types=chart --range=3m  --no_database_save AAPL | csv2plot.py - --columns=open,close,pbdate  --xaxis=pbdate --title=Apple
	# OR (daily return since inception )
	iex_types_batch.py --types=chart --range=3m  --no_database_save AAPL | csv2plot.py - --columns=open,close,pbdate  --xaxis=pbdate --title=Apple --return_since_inception
	# OR (pivot data)
	printf "select m.label as ticker,p.close as price,p.pbdate from prc_hist p,mapping_series_label m where p.name in ('^GSPC','^TWII','000001.SS','^SOX','^DJI') and p.pbdate>20170101 and p.name=m.series order by m.label,p.pbdate" | psql.sh -d ara | grep -v rows  | python2 csv2plot.py --pivot_group=ticker --pivot_value=price  --title='Market Overview 2018-05-25' --interpolate --return_since_inception -
	# OR (pivot data and near realtime per minute)
	iex_types_batch.py --types=chart --range=1d  --no_database_save AAPL XLK SPY| csv2plot.py - --columns=ticker,close,epochs  --xaxis=epochs --pivot_group=ticker --pivot_value=close  --title='Market Closing Overview' --interpolate --return_since_inception --trendline
	# OR (pivot data with minute data)
	python csv2plot.py AAPL_XLK_SPY.dat --columns=ticker,close,epochs  --xaxis=epochs --pivot_group=ticker --pivot_value=close  --title='Market Closing Overview' --interpolate --return_since_inception --trendline
	# OR (stock data with --src)
	csv2plot.py IBM --src=iex --columns=close,open,pbdate --days=90
	# OR (fred data with --src)
	csv2plot.py DGS2 --src=fred --columns=close,pbdate
	# OR (stock data with --src and candlestick graph)
	csv2plot.py IBM --src=iex --columns=close,open,high,low,volume,pbdate --title="IBM OHLC" --days=90 --ohlc
	# OR (minute data and candlestick graph)
	iex_types_batch.py --types=chart --range=1d --no_database_save --output=csv AAPL| csv2plot.py - --columns=close,open,high,low,volume,epochs,ticker --ohlc --title="Intraday AAPL OHLC"  --xaxis=epochs --trendline
	# OR (minute data and candlestick Combo graph)
	iex_types_batch.py --types=chart --range=1d --no_database_save --output=csv AAPL| csv2plot.py - --columns=ticker,close,open,high,low,volume,epochs --ohlc_combo --title="Intraday AAPL"   --xaxis=epochs --trendline 

    Note: return_since_inception will use $1 as the initial investment if the initial is less than $1
    Last mod., Sat Oct 27 20:50:18 EDT 2018
"""

import sys
from optparse import OptionParser
from datetime  import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.image as mimage
import matplotlib.ticker as mticker
import pandas as pd
from scipy.interpolate import interp1d
#font_name = "AR PL UKai CN"
#matplotlib.rcParams['font.family'] = font_name
#matplotlib.rcParams['axes.unicode_minus']=False # in case minus sign is shown as box

import matplotlib.font_manager as mfm
#font_path = "/usr/share/fonts/truetype/arphic/ukai.ttc"
font_path = "/usr/share/fonts/truetype/arphic/uming.ttc"
#font_path = "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf" #Droid Sans Fallback
prop = mfm.FontProperties(fname=font_path)
#prop = mfm.FontProperties()
plt.style.use('dark_background')

if sys.version_info.major == 2:
	reload(sys)
	sys.setdefaultencoding('utf8')
	from cStringIO import StringIO
else:
	from io import StringIO
#----------------------------------------------------------------#

def subDict(myDict,kyLst,reverseTF=False):
	if reverseTF is True: # invert-match, select non-matching [kyLst] keys 
		return { ky:myDict[ky] for ky in myDict.keys() if ky not in kyLst }
	else:
		return { ky:myDict[ky] for ky in myDict.keys() if ky in kyLst }

def ymd_parser(x,fmt='%Y%m%d'): return datetime.strptime(str(x),fmt)
def epoch_parser(x,s=1000): return datetime.fromtimestamp(int(x/s))

def extrapolate_series(yo):
	yg=yo.dropna()
	fn = interp1d(map(int,yg.index.values), yg.values, fill_value='extrapolate')
	return fn(map(int,yo.index.values))

def get_csvdata(args,sep='|',src=None,days=730,start=None,end=None,columns=None,hdrLst=None):
	"""
	Get data in datafram with selected [columns]
	"""
	if isinstance(args,pd.DataFrame):
		df = args
		if columns is not None and df.size > 0:
			df =  df[ list(set(df.columns) & set(columns.split(','))) ]
		if hdrLst is not None:
			xLst,yLst = hdrLst.split('=')
			xyD = dict(zip(xLst.split(','),yLst.split(',')))
			df.rename(columns=xyD,inplace=True)
		return df
	if len(args)<1:
		return None
	filename=args[0]
	if filename=='-':
		df=pd.read_csv(sys.stdin,sep=sep)
	elif src is not None:
		from _alan_calc import pull_stock_data 
		df = pull_stock_data(filename,days=days,src=src,start=start,end=end)
	else:
		df = pd.read_csv(filename,sep=sep)
	if df.size < 1:
		print >> sys.stderr, "**ERROR: Data not found!"
		return {}
	if columns is not None:
		df =  df[ list(set(df.columns) & set(columns.split(','))) ]
	df.dropna(inplace=True)
	if hdrLst is not None:
		xLst,yLst = hdrLst.split('=')
		xyD = dict(zip(xLst.split(','),yLst.split(',')))
		df.rename(columns=xyD,inplace=True)
	return df

def dataj2ts(ts,df,opts=None):
	from _alan_str import jj_fmt
	import ast
	dd = subDict(opts,['j2ts'],reverseTF=True)
	if df.size>0 and ts is not None and len(ts)>1:
		dd=update(f=df)
		return jj_fmt(ts,dd)
	else:
		return ''

def run_csv2plot(args,opts=None,optx=None):
	"""
	plot time series data from csv file 
	"""
	#- Set input parameters
	if opts is None:
		opts, _ = opt_csv2plot([])
	if optx is not None:
		opts.update(optx)
	for ky,va in opts.items():
		exec("{}=va".format(ky))

	#- Get data in datafram with selected [columns]
	df = get_csvdata(args,sep=sep,src=src,days=days,start=start,end=end,columns=columns,hdrLst=hdrLst)

	if df is None or len(df)<1 or df.size<1:
		return None
	if debugTF is True:
		print >> sys.stderr, df.head()

	#- Use backend to 'tkAgg' for cronjob
	if pngname is None or len(pngname)<=4:
		plt.switch_backend(backend)

	#- Create datetime index
	idxname='date'
	pbname=xaxis
	if pbname in df.columns:
		from _alan_date import ymd_parser,epoch_parser
		sdate = str(df[pbname].iloc[0])
		if sdate.isdigit() == True:
			if int(sdate)>123456789:
				idxpt=[epoch_parser(x) for x in df[pbname]]
			else:
				idxpt=[ymd_parser(x,fmt="%Y%m%d") for x in df[pbname]]
		else:
			idxpt=[ymd_parser(x,fmt=x_fmt) for x in df[pbname]]
		df.set_index(pd.DatetimeIndex(idxpt),inplace=True)
		df.index.rename(idxname,inplace=True)
		df = df.drop(pbname,1)
	elif idxname in df.columns:
		df[idxname] = pd.to_datetime(df[idxname])
		df.set_index(idxname,inplace=True)
	else:
		df = df.reset_index(drop=True)

	#- Create a pivot table
	trendName = None
	if pivot_group in df.columns and pivot_value in df.columns:
		trendName = df[pivot_group][0]
		df=df.pivot_table(index='date',columns=pivot_group,values=pivot_value)

	#- Create linear-interpolation for missing data 
	if interpolateYN is True:
		df=df.apply(extrapolate_series,axis=0)


	#- Create return since inception
	if rsiYN is True:
		de=[] 
		for j in range(df.shape[1]): 
			inix = df.iloc[0,j] if df.iloc[0,j]>1 else 1
			de.append(df.iloc[:,j]/inix*100.-100)
		#de = [df.iloc[:,j]/df.iloc[0,j]*100.-100 for j in range(df.shape[1])] 
		df = pd.concat(de,axis=1)

	#- Create trend curve
	if trendTF is True:
		try:
			from _alan_pppscf import vertex_locator
			if trendName is None:
				trendName = df._get_numeric_data().columns[0]
			dg, dh = vertex_locator(df[trendName],npar=npar,debugTF=True)
			#df['trend'] = dg['trend'].values
			if debugTF is True:
				print >> sys.stderr, "Trendline dg:\n",dg
		except Exception, e:
			print >> sys.stderr, "**ERROR: {} @ {}".format(str(e),'vertex_locator()')

	if title is None: 
		title="/".join(df.columns).upper()
		if rsiYN is True:
			title += " Return Since Inception"

	#- plot simple line plot
	if tsTF is False:
		df = df.reset_index(drop=True)

	if debugTF is True:
		print >> sys.stderr, df.head()
		print >> sys.stderr, df.tail()
	nobs=len(df.index)
	nsp = (nobs/nbins) if nobs>nbins*2 else nobs
	#ds=[y for j,y in enumerate(df.index) if j%nsp==0]
	#ax=df.plot(xticks=ds,title=title)
	colorUD = ['red','green'] if lang=='cn' else ['green','red']
	if ohlcComboTF is True:
		from alan_plot import plot_candlestickCombo
		from _alan_calc import run_tech
		chartType = 'minute' if pbname == 'epochs' else 'chart'
		ma1=5;ma2=30
		datax = run_tech(df, pcol='close',winLst=[ma1,ma2],nanTF=True)
		fig, axes = plot_candlestickCombo(datax,title,ma1,ma2,block=False,chartType=chartType,trendTF=trendTF,npar=npar,debugTF=debugTF,colorUD=colorUD)
		if pngname is not None and len(pngname)>4:
			plt.savefig(pngname)#, bbox_inches='tight',dpi=1000)
		else:
			plt.show(axes)
		return datax
	fig, ax=plt.subplots(figsize=(11,6))
	if ohlcTF is True:
		from alan_plot import plot_candlestick
		chartType = 'minute' if pbname == 'epochs' else 'chart'
		ax = plot_candlestick(df,tsidx=df.index,chartType=chartType,title=title,block=False,debugTF=debugTF,ax=ax,trendTF=trendTF,npar=npar,colorUD=colorUD)
		x_fmt = "%H:%M" if chartType == 'minute' else x_fmt
		print >> sys.stderr, df.describe()
	else:
		df.plot(ax=ax,grid=True,color=['yellow','green','red','cyan','lightgray','salmon'])
		#ax=df.plot(figsize=(11,6))
		ax.set_ylabel(df.columns[0])
		if trendTF is True:
			dg.plot(ax=ax)
	if rsiYN is True:
		ax.set_ylabel("return %")
	ax.grid(linestyle='dotted',linewidth=0.5)
	if df.index._typ == "datetimeindex":
		mddfmt=mdates.DateFormatter(x_fmt)
		ax.xaxis.set_major_formatter(mddfmt)
		xtinterval=(df.index[1]-df.index[0])
		if xtinterval.days < 7 and  xtinterval.days>=1 : # daily data
			ax.set_xlim(df.index[0], df.index[-1])
			#ax.xaxis.set_major_locator(mdates.MonthLocator(interval=int(nsp/30.+0.97)))
			bymd = [1,5,10,15,20,25] if nobs<50 else [1,15] if nobs<120 else [1]
			itv = 1 if nobs<160 else int(nsp/30.+0.97)
			xlocator = mdates.MonthLocator(bymonthday=bymd,interval=itv)
			ax.xaxis.set_major_locator(xlocator)
			# check if min/max of xaxis should be included major ticks
			if debugTF is True:
				print >> sys.stderr, ax.get_xticks(),ax.get_xlim()
			xtcks = list(ax.get_xticks())
			x1,x2 = xtcks[:2]
			xmin,xmax = ax.get_xlim()
			if (x1-xmin)>(x2-x1)*0.6:
				xtcks = [xmin] + xtcks
			if (xmax-xtcks[-1])>(x2-x1)*0.6:
				xtcks = xtcks + [xmax]
			ax.set_xticks(xtcks)
			ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))
			if debugTF is True:
				print >> sys.stderr,ax.get_xticks()
			print >> sys.stderr, "Daily data use MonthLocator"
		elif xtinterval.seconds < 30: # second data
			locator = mdates.AutoDateLocator()
			locator.intervald[5] = [0,5,10,15,20,25,30,35,40,45,55]
			mddfmt = mdates.AutoDateFormatter(locator)
			mddfmt.scaled[1/(24.*60.)] = '%M:%S' 
			ax.xaxis.set_major_locator(locator)
			ax.xaxis.set_major_formatter(mddfmt)
			print >> sys.stderr, "Second data use AutoDateLocator",xtinterval.seconds
		elif xtinterval.seconds < 100 : # minute data
			bym = [0,15,30,45] if nobs<=120 else [0,30] if nobs<=360 else [0]
			xlocator = mdates.MinuteLocator(byminute=bym, interval = 1)
			ax.xaxis.set_major_locator(xlocator)
			print >> sys.stderr, "Minute data use MinuteLocator",xtinterval.days
		else: # periodic data
			print >> sys.stderr, "Periodic data use DayLocator" 
			ax.xaxis.set_major_locator(mdates.DayLocator(interval=nsp))
	ax.xaxis.label.set_visible(False)
	plt.title(title,fontsize=30,fontproperties=prop)
	plt.xticks(rotation='20',fontsize=12)
	if len(df.columns)>1 and ohlcTF is False:
		ax.legend(loc="upper left",prop=prop)
	#logo = mimage.imread("aicaas_icon.png")
	#plt.figimage(logo, xo=20,yo=420)
	plt.subplots_adjust(left=0.1,bottom=0.30)
	if pngname is not None and len(pngname)>4:
		plt.savefig(pngname)#, bbox_inches='tight',dpi=1000)
	else:
		plt.show(ax)
	return df

def opt_csv2plot(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:	list arguments, usually passed from sys.argv
		retParser:	OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class 
	"""
	parser = OptionParser(usage="usage: %prog [option] FILENAME", version="%prog 1.0",
		description="Time-series Plotting Utility via matplotlib")
	parser.add_option("-s","--sep",action="store",dest="sep",default="|",
		help="field separator (default: |)")
	parser.add_option("","--xaxis",action="store",dest="xaxis",default="pbdate",
		help="x-axis column name (default: pbdate in yyyymmdd)")
	parser.add_option("","--columns",action="store",dest="columns",
		help="selected columns (default: ALL)")
	parser.add_option("","--ren_header",action="store",dest="hdrLst",
		help="rename header columns")
	parser.add_option("-t","--title",action="store",dest="title",
		help="title (default: combo-colunms)")
	parser.add_option("-n","--nbins",action="store",dest="nbins",default="6",type=int,
		help="number of bins in x-axis (default: 6)")
	parser.add_option("","--return_since_inception",action="store_true",dest="rsiYN",default=False,
		help="use Return since Inception plot. Note: $1 will be used as the initial investment if the initial is less than $1")
	parser.add_option("","--interpolate",action="store_true",dest="interpolateYN",default=False,
		help="use linear-interplation for missing data")
	parser.add_option("","--pivot_group",action="store",dest="pivot_group",
		help="pivot table group by column, must pair with PIVOT_VALUE")
	parser.add_option("","--pivot_value",action="store",dest="pivot_value",
		help="pivot table display value column, must pair with PIVOT_GROUP")
	parser.add_option("","--x_fmt",action="store",dest="x_fmt",default='%m-%d-%y',
		help="graph x-axis format (default: %m-%d-%y)")
	parser.add_option("","--png",action="store",dest="pngname",
		help="graph name (default: None)")
	parser.add_option("","--backend",action="store",dest="backend",default='tkAgg',
		help="matplotlib new backend(default: tkAgg)")
	parser.add_option("","--no_time_series",action="store_false",dest="tsTF",default=True,
		help="Simple line plot no time-series")
	parser.add_option("-l","--lang",action="store",dest="lang",default="en",
		help="language mode [cn|en] (default: en), ohlc/ohlc_combo ONLY")
	parser.add_option("","--ohlc",action="store_true",dest="ohlcTF",default=False,
		help="plot stock OHLC Candlestick")
	parser.add_option("","--ohlc_combo",action="store_true",dest="ohlcComboTF",default=False,
		help="plot stock OHLC Candlestick + MA/RSI/MACD Combo")
	parser.add_option("","--src",action="store",dest="src",
		help="data source (FILENAME is treated as ticker/series if provided. default: None)")
	parser.add_option("","--start",action="store",dest="start",
		help="start YYYY-MM-DD, must pair with SRC (default: 2-years-ago)")
	parser.add_option("","--end",action="store",dest="end",
		help="end YYYY-MM-DD, must pair with SRC (default: today)")
	parser.add_option("","--days",action="store",dest="days",default=730,type=int,
		help="number of days from END date, must pair with SRC (default: 730)")
	parser.add_option("","--trendline",action="store_true",dest="trendTF",default=False,
		help="Draw trendline, apply to the 1st array ONLY")
	parser.add_option("","--npar",action="store",dest="npar",default=15,type="int",
		help="trendline fitting polynomial degree (default: 15)")
	parser.add_option("","--j2ts",action="store",dest="j2ts",
		help="jinja2 template script, (default: None).")
	parser.add_option("","--extra_js",action="store",dest="extraJS",
		help="extra JSON in DICT format.")
	parser.add_option("","--extra_xs",action="store",dest="extraXS",
		help="extra excutable string in k1=v1;k2=v2; format")
	parser.add_option("","--debug",action="store_true",dest="debugTF",default=False,
		help="debugging (default: False)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	try:
		opts = vars(options)
		from _alan_str import extra_opts
		extra_opts(opts,xkey='extraJS',method='JS',updTF=True)
		extra_opts(opts,xkey='extraXS',method='XS',updTF=True)
	except Exception as e:
		print >> sys.stderr, str(e)
	return (opts, args)

if __name__ == '__main__':
	opts,args = opt_csv2plot(sys.argv)
	try:
		df=run_csv2plot(args,opts)
		#print dataj2ts(opts['j2ts'],df,opts)
	except Exception, e:
		print >> sys.stderr, "**ERROR:",str(e)
