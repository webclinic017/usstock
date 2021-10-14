#!/usr/bin/env python
""" argument template
Usage of:
  from argparse_tst import parse_args
  opts, args = parse_args()
"""
#### -*- coding: utf-8 -*-
from __future__ import print_function
import sys
from argparse import ArgumentParser

def parse_args(version="0.1",description="A command-line utility",nargs='*'):
	"""
	command-line options initial setup
	Usage of:
	opts, ns_args = parse_args()
	OR
	opts, ns_args = parse_args(version="0.1",description="A command-line utility",nargs='*')
	Where nargs: [?|+|*] for [0 or 1 | 1+ array | 0+ array ]
	"""
	parser = ArgumentParser(version='%(prog)s:'+ version,description=description)
	parser.add_argument("-s","--start",action="store",dest="start",
		help="START date in YYYY-MM-DD format (default: 3-years-ago)")
	parser.add_argument("-e","--end",action="store",dest="end",
		help="END date in YYYY-MM-DD (default: today)")
	parser.add_argument("--days",action="store",dest="days",default=450,type=int,
		help="DAYS ago from today (default: 450) Note that START has higher precedence order than DAYS if --start=START is assigned.")
	parser.add_argument("-d","--database",action="store",dest="dbname",default="ara",
		help="DATABASE name (default: ara)")
	parser.add_argument("--host",action="store",dest="hostname",default="localhost",
		help="database HOST (default: localhost)")
	parser.add_argument("--table",action="store",dest="tablename",
		help="database TABLE name")
	parser.add_argument("--file",action="store",dest="filename",
		help="input FILENAME for TICKER1&2, use - for stdin. Note that file must contain ticker,close/value,pbdate columns")
	parser.add_argument("--columns",action="store",dest="columns",
		help="selected columns if --file FILENAME is used")
	parser.add_argument("--tag",action="store",dest="tagwords",
		help="tag words separated by [,]")
	parser.add_argument("--title",action="store",dest="titlename",
		help="graph TITLE name")
	parser.add_argument("--png",action="store",dest="pngname",
		help="graph PNG name")
	parser.add_argument("--degree",action="store",dest="deg",default=2,type=int,
		help="DEGREE of polynomial function (default: 2)")
	parser.add_argument("--method",action="store",dest="method",
		help="data smooth method for TICKER1")
	parser.add_argument("--freq",action="store",dest="freq",default="D",
		help="frequency period of TICKER1 [D|W|M|Q|Y](default: D)")
	parser.add_argument("--freq2",action="store",dest="freq2",default="D",
		help="frequency period of TICKER2 [D|W|M|Q|Y](default: D)")
	parser.add_argument("--log",action="store_true",dest="logTF",default=False,
		help="take LOG of TICKER1 (default: False)")
	parser.add_argument("--log2",action="store_true",dest="log2TF",default=False,
		help="take LOG of TICKER2 (default: False)")
	parser.add_argument("--src",action="store",dest="src",default="yahoo",
		help="data source: SRC of TICKER1 (default: yahoo)")
	parser.add_argument("--src2",action="store",dest="src2",default="fred",
		help="data source: SRC of TICKER2 (default: fred)")
	parser.add_argument("--pct_chg_prd",action="store",dest="pct_chg_prd",default=1,type=int,
		help="PCT_CHG_PRD of TICKER1 (default: 1). 0 for no percent change.")
	parser.add_argument("--pct_chg_prd2",action="store",dest="pct_chg_prd2",default=0,type=int,
		help="PCT_CHG_PRD of TICKER2 (default: 0). 0 for no percent change.")
	parser.add_argument("--lag",action="store",dest="lagd",default=1,type=int,
		help="Shift lags of TICKER2 (default: 1). 0 for no lag.")
	parser.add_argument("--no_monthly",action="store_false",dest="monthlyTF",default=True,
		help="running in daily regression (default: monthly)")
	parser.add_argument("--debug",action="store_true",dest="debugTF",default=False,
		help="debugging mode (default: False)")
	parser.add_argument("--no_database_save",action="store_false",dest="saveDB",default=True,
		help="no database save (default: True)")

	if nargs=='?': #- for only 1 argument
		parser.add_argument("command",action="store", nargs='?',default="search",
			help="command (default: search)")
	elif nargs=='+': #- for 1+ arguments
		parser.add_argument("argLst",action="store", nargs='+',
			help="argument list (minimum one)")
	else: #-  default as '*' for 0+ arguments
		parser.add_argument("tkLst",action="store", nargs='*',default=[],
			help="TICKER1 TICKER2 ... (default:[] )")
	ns_args = parser.parse_args()
	return(dict(ns_args._get_kwargs()), ns_args)

if __name__ == '__main__':
	try:
		opts, ns_args = parse_args(nargs='*')
		print((opts,ns_args),file=sys.stderr)
	except Exception as e:
		print(str(e),file=sys.stderr)
