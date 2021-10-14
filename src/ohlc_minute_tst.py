# TBD, for create hourly strategy likt lsi_daily 
# -*- coding: utf-8 -*-
import sys
import pandas as pd
from _alan_str import sysCall
from _alan_calc import conn2pgdb,subDict

def get_stgyData(ticker,date):
	from _alan_calc import get_minute_iex,get_minute_yh,run_tech
	if any([x in ticker for x in ['^','=']]) is True:
		ranged=",{}".format(date)
		dx = get_minute_yh(ticker,ranged=ranged)
	else:
		dx = get_minute_iex(ticker,date=date)
	minute_hist=run_tech(dx,pcol='close',winLst=[5,30,60])
	return minute_hist

#-- START FIND STRATEGIES
# Input: minute_hist
# Output: minute_ptn
def find_stgy(minute_hist,pgDB=None,debugTF=False):
	if 'pbdate' not in minute_hist and 'epochs' in minute_hist:
		minute_hist.loc[:,'pbdate']=minute_hist['epochs'].values
	else:
		minute_hist.loc[:,'epochs'] = [int(x.strftime("%s000")) for x in minute_hist.index]
		minute_hist.loc[:,'pbdate']=minute_hist['epochs'].values
	try:
		mxdate=minute_hist.query('signal_buysell_macd!=0')['pbdate'].sort_values(ascending=False).iloc[0]
		minute_latest_macd = minute_hist.query("pbdate>={}".format(mxdate))
	except Exception as e:
		minute_latest_macd = {}
	if debugTF and len(minute_latest_macd)>0:
		sys.stderr.write("===minute_latest_macd:\n{}\n".format(minute_latest_macd))
	from _alan_pattern import calc_ohlc_pattern,add_MACD_pattern
	minute_ptn=calc_ohlc_pattern(minute_hist)
	minute_ptn=add_MACD_pattern(minute_hist,minute_ptn)

	#-- START CALC PNL
	# Input: minute_hist,minute_ptn
	# Output: minute_pnl
	from _alan_pnl import calc_ohlc_pnl
	minute_pnl=calc_ohlc_pnl(minute_hist,minute_ptn,prd=251,xfl=-1,xcap=1)

	#-- SAVE TO DB
	# mongoDB: ara::minute_pattern, minute_pnl
	from _alan_str import write2mdb
	clientM=None
	mobj_ptn,clientM,msg = write2mdb(minute_ptn,clientM,tablename='minute_pattern',zpk={'ticker','name'})
	print >> sys.stderr, msg,clientM
	#mobj_pnl,clientM,msg = write2mdb(minute_pnl,clientM,tablename='minute_pnl',zpk={'ticker','name','pbdate'})
	if pgDB is not None:
		minute_pnl.to_sql('minute_pnl', pgDB, schema='public', index=False, if_exists='replace')
		minute_hist.to_sql('minute_hist', pgDB, schema='public', index=False, if_exists='replace')
	return minute_pnl

# TBD, expect to replace
# aX=run_comment_ohlc(m1,m2,tempS,dotSign=dotSign,lang=lang)
#
def get_stgyLatest(minute_pnl,minute_hist,pky=['evening_star','morning_star','bearish_MACD','MACD'],debugTF=False):
	from _alan_calc import chk_sign
	from _alan_date import delta2dates
	from lsi_minute import generate_comment_ohlc
	qstr="sig==1 & lsc!='combo' & name in {}".format(pky)
	temp_pnl = minute_pnl.query(qstr).sort_values(['pbdate'],ascending=[False]).iloc[:1]
	if temp_pnl.shape[0]<1:
		return {}
	stgyName, stgyDate = temp_pnl[['name','pbdate']].iloc[0]
	temp_hist = minute_hist.query("pbdate>={}".format(stgyDate)).sort_values(['pbdate'], ascending=False)
	if 'MACD' in stgyName:
		temp_hist['curr_trend'] = temp_hist['signal_value_macd']
	else:
		temp_hist['curr_trend'] = temp_hist['dxma5']
	if len(temp_hist)>1:
		curr_deriv = chk_sign(temp_hist['curr_trend'].iloc[1],temp_hist['curr_trend'].iloc[0])
	else:
		curr_deriv = -1
	if temp_hist.shape[0]>0:
		dstgy = temp_hist[['pbdate','price','ticker','curr_trend']].iloc[0].to_dict()
	else:
		dstgy = {}
	dstgy.update(curr_deriv=curr_deriv)
	dstgy['curr_date'] = dstgy.pop('pbdate')
	if debugTF is True:
		print >> sys.stderr, dstgy
	dstgy.update(temp_pnl.iloc[0].to_dict())
	e,s = minute_hist['epochs'][[-1,0]].values
	dstgy['pnl_prd'] = delta2dates(e,s,fq='HOUR')
	#return pd.Series(dstgy)
	return dstgy

#-- RUN Narratives
def run_narratives_tst(ticker,minute_hist,minute_pnl,pgDB,debugTF=False):
	sysCall("/apps/fafa/bin/psql.sh -d ara < minute_latest_macd.sql")
	sysCall("/apps/fafa/bin/psql.sh -d ara < minute_ls_signal.sql")
	#for fname in ['minute_latest_macd.sql','minute_ls_signal.sql']:
	#	xqr = open(fname).read()
	#	pgDB.execute(xqr)

	mls_macd = minute_pnl.query("sig==1 & lsc!='combo' & name in ['bearish_MACD', 'MACD']").sort_values(['pbdate'], ascending=[False])

	from lsi_minute import getdb_ohlc_minute, run_comment_ohlc,assign_ts_ohlc
	lang = 'cn'
	dotSign = "點".encode('utf-8')
	sqr="SELECT s.*,m.sector,m.company{} as company FROM mapping_ticker_cik m right join (select ticker,min(pbdate) as mndt,max(pbdate) as mxdt from minute_pnl group by ticker) as s ON m.ticker=s.ticker order by s.ticker".format("" if lang=="en" else "_"+lang)
	dataM=pd.read_sql(sqr,pgDB)
	(_,mndt,mxdt,sector,label)=dataM.iloc[0].values

	(tempS,daily_hdr) = assign_ts_ohlc(lang=lang)
	(pbdate,m1,m2)=getdb_ohlc_minute(ticker,mndt,mxdt,sector,label,pgDB)
	if debugTF is True:
		print >> sys.stderr, "+++ m1:\n",m1
		print >> sys.stderr, "+++ m2:\n",m2
	aX=run_comment_ohlc(m1,m2,tempS,dotSign=dotSign,lang=lang)
	print >> sys.stderr, aX['comment_ohlc']
	return aX['comment_ohlc']

#-- RUN Narratives
#-- TBD, expected to replace run_narratives_tst
def run_narratives(ticker,minute_hist,minute_pnl,pgDB=None,lang='cn',debugTF=False,dirname='.',optx={}):
	from lsi_minute import generate_comment_ohlc
	md1 = get_stgyLatest(minute_pnl,minute_hist,pky=['evening_star','morning_star','bearish_MACD','MACD'])
	enhanceClause = ''
	dirname = optx['dirname'] if 'dirname' in optx else '.'
	if 'MACD' not in md1['name']:
		md2 = get_stgyLatest(minute_pnl,minute_hist,pky=['bearish_MACD','MACD'])
		md2['ynCode'] = 1 if md1['lsc']==md2['lsc'] else 0
		md2['enhanceName'] =  md1['name']
		if 'label' in optx:
			md2['label']=optx['label']
		print >> sys.stderr, "Additionals in run_narrative():{}".format(optx)
		dirname = optx['dirname'] if 'dirname' in optx else '.'
		fname = '{}/daily_macdEnhance_{}.j2'.format(dirname,lang)
		ts_enhance = open(fname).read()
		enhanceClause = generate_comment_ohlc(md2,ts_enhance,lang=lang)
	
	if 'fcsChg' in optx:
		md1.update(fcsChg=optx['fcsChg'])
	if 'label' in optx:
		md1['label']=optx['label']
	md1['enhanceClause'] = enhanceClause
	fname = '{}/daily_ohlc_{}.j2'.format(dirname,lang)
	ts_ohlc = open(fname).read()
	ret = generate_comment_ohlc(md1,ts_ohlc,lang=lang)
	if debugTF is True:
		print >> sys.stderr, ret
	return ret

#----- MAIN ------#
if __name__ == '__main__':
	pgDB=conn2pgdb(dbname='ara',hostname='localhost')
	args=sys.argv[1:]
	ticker = 'SPY' if len(args)==0 else args[0]
	date = 20181203 if len(args)<=1 else int(args[1])
	minute_hist = get_stgyData(ticker,date)
	minute_pnl = find_stgy(minute_hist,pgDB)
	#run_narratives_tst(ticker,minute_hist,minute_pnl,pgDB)
	ret = run_narratives(ticker,minute_hist,minute_pnl,pgDB)
	print >> sys.stderr, ret
