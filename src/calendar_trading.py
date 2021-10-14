#!/usr/bin/env python
'''
A Python library of exchange calendars meant to be used with Zipline
and saved to postgreSQL ara::calendar_trading

# US Stock Exchanges (includes NASDAQ)
us_calendar = get_calendar('XNYS')
# London Stock Exchange
london_calendar = get_calendar('XLON')
# Toronto Stock Exchange
toronto_calendar = get_calendar('XTSE')
# Tokyo Stock Exchange
tokyo_calendar = get_calendar('XTKS')
# Frankfurt Stock Exchange
frankfurt_calendar = get_calendar('XFRA')

# US Futures
us_futures_calendar = get_calendar('us_futures')
# Chicago Mercantile Exchange
cme_calendar = get_calendar('CMES')
# Intercontinental Exchange
ice_calendar = get_calendar('IEPA')
# CBOE Futures Exchange
cfe_calendar = get_calendar('XCBF')
# Brazilian Mercantile and Futures Exchange
bmf_calendar = get_calendar('BVMF')
'''

import numpy as np
import pandas as pd
import sys,pickle
from _alan_date import dt2ymd
from _alan_calc import save2pgdb
from trading_calendars import get_calendar

def get_exch_time(start=None,end=None,isoCode='XNYS',tz='America/New_York',dbname='ara',tablename='calendar_trading',wmode=None,debugTF=True):
	cal = get_calendar(isoCode)
	open_time = cal._opens.tz_convert(tz).to_pydatetime()[start:end]
	close_time = cal._closes.tz_convert(tz).to_pydatetime()[start:end]
	pbdate=[ int(dt2ymd(x)) for x in close_time]
	df = pd.DataFrame(data=np.array([open_time,close_time,pbdate]).T,columns=['open_time','close_time','pbdate'])
	df['iso_code'] = isoCode
	if wmode:
		save2pgdb(df,db=dbname,tablename=tablename,wmode=wmode)
		pname = "pickle/{tablename}.{isoCode}.pickle".format(**locals())
		fp = open(pname,"wb")
		pickle.dump(df,fp)
		fp.close()
		if debugTF:
			sys.stderr.write("Calendar Saved to Table:{tablename}: Pickle:{pname}\n".format(**locals()))
	return df

start=-700
cal_us=get_exch_time(start=start,isoCode='XNYS',tz='America/New_York',wmode='replace')
cal_cme=get_exch_time(start=start,isoCode='CMES',tz='America/Chicago',wmode='append')
cal_jp=get_exch_time(start=start,isoCode='XTKS',tz='Asia/Tokyo',wmode='append')
