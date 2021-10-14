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
   Last mod.,: Fri Dec 15 14:34:07 EST 2017
"""

import sys,os
import cgitb,cgi

def cgi_image(saveDIR):
	xf={'file': None}
	cgitb.enable(display=0, logdir="/apps/fafa/cronJob/log/cgi-bin/")
	cgitb.enable(format='text')
	mf = cgi.FieldStorage()
	for ky in mf:
		xf[ky]=mf[ky].value
	if 'folder' in xf and os.path.isdir(xf['folder']) is True:
		saveDIR = xf['folder']
	imgname="/".join([saveDIR,xf['file']])
	data=open(imgname, 'rb').read()
	print "Content-type: image/png\n"
	print data

if __name__ == '__main__':
	saveDIR="/home/web/bb_site/html/images"
	if not os.path.exists(saveDIR) :
		saveDIR="/apps/fafa/pyx/images"
	xreq=os.getenv('REQUEST_METHOD')
	if xreq in ('GET','POST') : # WEB MODE
		data=cgi_image(saveDIR)
	elif len(sys.argv)>1:
		fname=sys.argv[1]
		imgname="/".join([saveDIR,fname])
		data=open(imgname, 'rb').read()
		print data
	else:
		print >> sys.stderr, "usage of: {} IMAGENAME".format(__file__)
