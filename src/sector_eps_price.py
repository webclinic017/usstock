#testina!/usr/bin/env python3
# -*- coding: utf-8 -*- 
""" Calc price vs. eps chg in $
	and save results into [fp_eps_price fp_eps_fcs fp_eps_param] tables
	for prepared data, forecasts, and relevant parameters
    Where
	[rt] is the quarterly return in % 
	[rtm] is the monthly return in % on the reporting date
	[ma] is (eps yearAgo difference) / (previous month-end price before the reporting date)
	[hwX] is forecasts of [ma]  period [nfcs] 
	[hwY] is forecasts of [rt]  period [nfcs] 
	[hwMonthly] is forecasts of [rtm]  period [nfcs] 
	[prc_fcs] is forecasts of next [nfcs] quarters price 
    To load parameters and min-max [mnmx] use:
	from _alan_calc import sqlQuery,pd,json;mnmx=sqlQuery('select * from fa_eps_param').mnmx;print pd.DataFrame(list(map(json.loads,mnmx)))
	OR
	from _alan_calc import sqlQuery,pd,json;ep=sqlQuery('select * from fa_eps_param');print pd.merge( pd.DataFrame(map(json.loads,ep.mnmx)),ep[['ns','sector','param']],left_index=True,right_index=True ).to_csv(sep='\t',index=False)
    Last Mod., Wed Sep 12 17:46:11 EDT 2018
"""
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from _alan_calc import pull_stock_data, sqlQuery
from _alan_calc import conn2pgdb,ewma,sma,hw2ewma,extrapolate_series
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from _alan_date import freq_d2m, next_month_date
from scipy import stats
import json

def linear_xy(xi,xy=[]): 
	""" linear interpolated xi within range """ 
	return None if len(xy)<4 else linearXY(xi,*xy[:4])

def linearXY(xi,xmax,xmin,ymax,ymin):
	""" linear interpolated xi within range """ 
	if xi>=xmax:
		yi = ymax 
	elif xi<=xmin or xmin==xmax or ymin==ymax:
		yi = ymin 
	else:
		yi = ymin+(xi-xmin)*(ymin-ymax)/(xmin-xmax)
	return yi

def taylor_appx(vv,n=3):
	""" apply [n] power taylor approxiation to array [vv] 
	"""
	n = min(n,len(vv))
	vd = vv
	d = 1.0
	y = vd[-1] / d
	print( "ta[{}]:{}, appx:{}".format(1,d,y), file=sys.stderr)
	for j in range(2,n+1):
		vd = np.diff(vd)
		d *= j
		y += vd[-1] / d
		print( "ta[{}]:{}, appx:{}".format(j,d,y), file=sys.stderr)
	return y

def get_dh(ticker,nfcs=4,nss=4,pgDb=None,sector=None):
	""" get eps and stock data and then create dg,dh,dfcs
	    where
		dg: earnings data from temp_eps table
		dh: corresponding eps, differnet in PE reverse ratio, and quarterly returns 
		dfcs: [hwX] forecasts of [ma] period
	"""
	#- get EPS data
	sqx = "SELECT *,\"epsDif\" as ma,rptdate::int/100 as yyyymm FROM temp_eps WHERE  ticker={!r} ORDER BY rptdate".format(ticker)
	dg = sqlQuery(sqx,pgDb)
	#dg = dg.rename({"pbdate":"asof"},axis='columns')

	#- get closing price data
	sDu = pull_stock_data(ticker,days=1000,src='iex')[['close','pbdate']]
	sDu = sDu.rename({"close":"price"},axis='columns')
	cur_prc = sDu['price'][-1]
	dx = freq_d2m(sDu,method='last',fq='M')
	dx['pbdate'] = dx['pbdate'].astype('int')
	dx.loc[:,'yyyymm'] = (dx['pbdate'][:]/100).astype('int')
	#dx['rtm'] = dx['price'].diff()
	dx['rtm'] = dx['price'].pct_change()*100.0 # monthly return and rescale to %
	dx['pr1'] = dx['price'].shift()
	"""
	dx['rt1'] = dx['rt'].shift(-1)
	dx['rt2'] = dx['rt'].shift(-2)
	dx['pb1'] = dx['pbdate'].shift(-1)
	dx['pb2'] = dx['pbdate'].shift(-2)
	"""

	#- merge data
	#dh = pd.merge(dx[['rt1','pb1','rt2','pb2','rt','yyyymm','pbdate','price']],dg[['ma','yyyymm','asof','ticker']],on=['yyyymm'])
	dh = pd.merge(dx[['rtm','yyyymm','pbdate','price','pr1']],dg[['ma','yyyymm','asof','ticker']],on=['yyyymm'])
	dh.loc[:,'ma'] = dh['ma']/dh['pr1']*100.0 # use PE-ratio difference
	dh['rt'] = dh['price'].pct_change()*100.0 # quarterly return

	ntr = 5 # nth powter of taylor series
	vv = dh['ma'].dropna().astype(float)
	if len(vv)<1:
		return [],[],[]
	for j in range(nfcs):
		vv.append(taylor_appx(vv,ntr)) 
	hwX = vv[-nfcs:]
	hwN = len(hwX)
	hwS = json.dumps(list(hwX))
	dfcs = pd.DataFrame({'hwX':[hwS],'ticker':[ticker],'prc_cur':[cur_prc],'sector':[sector]},columns=['sector','ticker','hwX','prc_cur'])
	return dfcs,dh,dg

def reg_eps_price(zm,npar=2,pngname=None):
	""" regress price / eps in difference at [npar] order """
	#scaler = MinMaxScaler(feature_range=(-1, 1))
	#zm = scaler.fit_transform(zm)
	#zm = scaler.inverse_transform(zm)
	vx = zm[:,0]
	vy = zm[:,1]
	params = np.polyfit(vx,vy,npar)
	vf = np.polyval(params,vx)
	if pngname is not None:
		plt.scatter(vx,vf,c="red", alpha=0.8, marker='+')
		plt.scatter(vx,vy,c="b",   alpha=0.8, marker='o')
		plt.show()
	mn,mx = stats.describe(vx).minmax
	mn = max(mn,-2)
	mx = min(mx,3)
	vx = np.arange(mn,mx,(mx-mn)/30)
	vy = np.polyval(params,vx)
	if pngname is not None:
		plt.plot(vx,vy)
		plt.show()
	return params

def mnx_eps_price(zm,npar=2,pngname=None): 
	""" regress price / eps in difference at [npar] order minMax scaler
	    DEPRECATED, not improved as expected
	"""
	scaler = MinMaxScaler(feature_range=(-1, 1))
	xm = scaler.fit_transform(zm)
	vx = xm[:,0]
	vy = xm[:,1]
	params = np.polyfit(vx,vy,npar)
	sys.stderr.write(" ---param:{}\n".format(params))
	vf = np.polyval(params,vx)
	um=xm.copy()
	um[:,1] = vf
	zm1 = scaler.inverse_transform(um)
	if pngname is not None:
		plt.scatter(zm1[:,0],zm1[:,1],c="red", alpha=0.8, marker='+')
		plt.scatter(zm[:,0],zm[:,1],c="b",   alpha=0.8, marker='o')
		plt.show()
	vx = um[:,0]
	mn,mx = stats.describe(vx).minmax
	mn = max(mn,-2)
	mx = min(mx,3)
	vx = np.arange(mn,mx,(mx-mn)/100)
	vy = np.polyval(params,vx)
	um = np.array([vx,vy]).transpose()
	zm1 = scaler.inverse_transform(um)
	if pngname is not None:
		#plt.scatter(zm1[:,0],zm1[:,1],c="green", alpha=0.8, marker='+')
		plt.plot(zm1[:,0],zm1[:,1])
		plt.show()
	return params

def wrap_regression(zm,npar=2,sector=None,pngname=None):
	bnmx ={"xmin":-5.0, "xmax":6.0 ,"ymin":-25.0, "ymax":25.0}
	ns = zm.shape[0]
	params = reg_eps_price(zm,npar=npar,pngname=pngname)
	print( "@wrap_regression() - params:", params, file=sys.stderr)
	try:
		yfunc = np.poly1d(params)
		mnmx = bnmx
		if npar>1:
			ydfunc = yfunc.deriv(m=1)
			xr = ydfunc.roots[~np.iscomplex(ydfunc.roots)][0].real
			if params[0]<0: # concave case
				mnmx['xmax'] = round(min(xr,bnmx['xmax']), 2)
				mnmx['xmin'] = bnmx['xmin']
			else:
				mnmx['xmax'] = bnmx['xmax']
				mnmx['xmin'] = round(max(xr,bnmx['xmin']), 2)
			mnmx['ymax'] = round(min(yfunc(mnmx['xmax']),bnmx['ymax']),2)
			mnmx['ymin'] = round(max(yfunc(mnmx['xmin']),bnmx['ymin']),2)
	except Exception as e:
		print( "@wrap_regression: ",bnmx,mnmx,str(e), file=sys.stderr)
	mnmxS = json.dumps(mnmx)
	paramS = json.dumps(list(params))
	dprm = pd.DataFrame({'param':[paramS],'mnmx':[mnmxS],'sector':[sector],'ns':[ns]},columns=['sector','param','mnmx','ns'])
	print( "@wrap_regression: ",ns, params, mnmx,dprm, file=sys.stderr)
	return dprm

def calc_eps_fcs(cur_prc,hwX,params,mnmx,ticker=None,pngname=None):
	bnmx ={"xmin":-5.0, "xmax":6.0 ,"ymin":-25.0, "ymax":25.0}
	hwY = np.polyval(params, hwX)
	print( "----- mnmx/bnmx", mnmx,bnmx, file=sys.stderr)
	print( "----- Before", ticker, hwX,hwY, file=sys.stderr)
	for j,(xi,yi) in enumerate(zip(hwX,hwY)):
		if xi<mnmx['xmin']:
			yi = linearXY(xi,mnmx['xmin'],bnmx['xmin'],mnmx['ymin'],bnmx['ymin']) 
			hwY[j]=yi
		elif xi>mnmx['xmax']:
			yi = linearXY(xi,bnmx['xmax'],mnmx['xmax'],bnmx['ymax'],mnmx['ymax']) 
			hwY[j]=yi
	print( "+++++ After", ticker, hwX,hwY, file=sys.stderr)
	prc_fcs=[]
	p=1
	for r in hwY:
		p = p*(1+r/100.)
		prc_fcs += [p*cur_prc]

	print( "=== {} curr.price:{},\n\tForecasts:1/PE Diff:{}\n\tReturn:{}\n\tPrices:{}".format(ticker,cur_prc,hwX,hwY,prc_fcs), file=sys.stderr)
	if pngname is not None:
		plt.plot(range(len(prc_fcs)),prc_fcs);plt.title(ticker);plt.show()
	return prc_fcs, hwY

def wrap_eps_fcs(dfcs,sector,dprm,pngname=None,fcsTF=True):
	""" create price forecasts [dfcs] based on eps forecasts [hwX] and quadratic paramters [dprm]
	"""
	params = json.loads(dprm['param'])
	mnmx = json.loads(dprm['mnmx'])
	hwY=[]
	prc_fcs=[]
	for j in range(dfcs['hwX'].size):
		try:
			hwX = json.loads(dfcs['hwX'][j])
			prc_cur = dfcs['prc_cur'][j]
			ticker = dfcs['ticker'][j]
			f, h = calc_eps_fcs(prc_cur,hwX,params,mnmx,ticker=ticker,pngname=None)
			prc_fcs.append(json.dumps(list(f)))
			hwY.append(json.dumps(list(h)))
		except Exception as e:
			prc_fcs.append(None)
			hwY.append(None)
			print( "**ERROR: @ wrap_eps_fcs(): ", j, ticker, str(e), file=sys.stderr)
			continue
	if fcsTF is True:
		print( sector, hwY[:-5], file=sys.stderr)
		dfcs.loc[:,'hwY'] = hwY
		print( sector, prc_fcs[:-5], file=sys.stderr)
		dfcs.loc[:,'prc_fcs'] = prc_fcs
	return prc_fcs,hwY

def sector_eps_price(sector=None,nss=4,nfcs=4,pgDb=None,pngname=None):
	if sector is None or sector.upper()=='ALL':
		sqx = "SELECT ticker FROM mapping_ticker_cik where act_code=1"
	else:
		sqx = "SELECT ticker FROM mapping_ticker_cik WHERE sector={!r} and act_code=1".format(sector)
		#sqx = "SELECT a.ticker FROM (SELECT ticker FROM iex_earnings_hist GROUP BY ticker) a, (SELECT ticker FROM iex_company_hist WHERE sector={!r}) b WHERE a.ticker=b.ticker".format(sector)
	tkLst = list(map(str,pd.read_sql(sqx,pgDb).iloc[:,0]))
	#tkLst=tkLst[:10]
	print( "[{}]:{}".format(sector, tkLst), file=sys.stderr)

	tkN = len(tkLst)
	dH = pd.DataFrame()
	dFcs = pd.DataFrame()
	for j,ticker in enumerate(tkLst):
		print( "{}/{}: {} of {}".format(j,tkN,ticker,sector), file=sys.stderr)
		try:
			dfcs,dh,dg = get_dh(ticker,nfcs=nfcs,nss=nss,pgDb=pgDb,sector=sector)
			if len(dh) < 1:
				continue
		except Exception as e:
			print( "**ERROR: @sector_eps_price() - get_dh():", str(e), file=sys.stderr)
			continue
		dH = pd.concat([dH,dh])
		dFcs = pd.concat([dFcs,dfcs])
		print( dH, file=sys.stderr)
		print( "===={}/{}.[{}]{}:{}|{}".format(j,tkN,ticker,sector,dH['ma'].values,dH['rt'].values), file=sys.stderr)

	dH = dH.dropna(axis=0, how='any')
	dH = dH.query('ma<10 and ma>-2')
	dH.reset_index(drop=True,inplace=True)
	dFcs.reset_index(drop=True,inplace=True)
	print( "==== RUNNING QUARTERLY for Sector[{}]".format(sector), file=sys.stderr)
	try:
		zm = np.array(dH[['ma','rt']])
		dprm = wrap_regression(zm,npar=2,sector=sector,pngname=pngname)
	except Exception as e:
		print( "**ERROR: @sector_eps_price() - wrap_regression():", str(e), file=sys.stderr)
	try:
		prc_fcs, hwY = wrap_eps_fcs(dFcs,sector,dprm.iloc[0],pngname=pngname)
		print( "-----", "RUNNING MONTHLY", file=sys.stderr)
		zm1 = np.array(dH[['ma','rtm']])
		dprm1 = wrap_regression(zm1,npar=2,sector=sector,pngname=pngname)
	except Exception as e:
		print( "**ERROR: @sector_eps_price() - wrap_regression() zm1:", str(e), file=sys.stderr)
	try:
		prc_fcs1, hwY1 = wrap_eps_fcs(dFcs,sector,dprm1.iloc[0],pngname=pngname,fcsTF=False)
		print("size of dFcs:{}, hwY1:{}".format(dFcs.shape,len(hwY1)), file=sys.stderr)
		dFcs.loc[:,'hwMonthly'] = hwY1
	except Exception as e:
		print( "**ERROR: @sector_eps_price() - wrap_eps_fcs():", str(e), file=sys.stderr)
	return dprm,dH,dFcs

def mainTst():
	nss = 4 # seasonality period
	nfcs = 4 # quarterly forecast
	pngname = None
	saveDB = True
	pgDb=conn2pgdb(dbname='ara')
	sqx = "SELECT sector AS sector FROM spdr_sector WHERE sector NOT LIKE '%%ETF' ORDER BY sector"
	#sqx = "SELECT sector_alias AS sector FROM spdr_sector WHERE sector NOT LIKE '%%ETF'"
	secLst = [str(x) for x in pd.read_sql(sqx,pgDb).iloc[:,0] ]
	secLst += ['ALL']
	#secLst = secLst [6:7] # for special testing
	dH = pd.DataFrame()
	dFcs = pd.DataFrame()
	dPrm = pd.DataFrame()
	rmode = 'replace'
	for j,sector in enumerate(secLst):
		try:
			dprm,dh,dfcs = sector_eps_price(sector,nss=nss,nfcs=nfcs,pgDb=pgDb,pngname=None)
			""" get [dh] from database
			sqx = "select * from fa_eps_price where ticker in (select ticker from iex_company_hist where sector={!r})".format(sector)
			dH =  pd.read_sql(sqx,pgDb)
			dprm = wrap_regression(dH,npar=2,sector=sector,pngname="hello")
			"""
			print( j,sector, file=sys.stderr)
			print( dprm, file=sys.stderr)
			print( dfcs, file=sys.stderr)
			print( dh, file=sys.stderr)
			if saveDB is True:
				dh.to_sql('fa_eps_price',pgDb,schema='public',index=False,if_exists=rmode)
				dfcs.to_sql('fa_eps_fcs',pgDb,schema='public',index=False,if_exists=rmode)
				dprm.to_sql('fa_eps_param',pgDb,schema='public',index=False,if_exists=rmode)
				rmode = 'append'
			#dH = pd.concat([dH,dh])
			#dFcs = pd.concat([dFcs,dfcs])
			#dPrm = pd.concat([dPrm,dprm])
		except:
			dprm, dh, dfcs = ([],[],[])
			continue
	#return dPrm, dH, dFcs
	return dprm, dh, dfcs

#=====================================================================
if __name__ == '__main__':
	mainTst()
