#!/usr/bin/env python
'''
Usage of:
TZ=America/New_York python check_work_hours.py 930 1620
'''
import sys
import datetime
#import pytz
#twtime = datetime.datetime.now(pytz.timezone('Asia/Taipei'))
#nytime = datetime.datetime.now(pytz.timezone('America/New_York'))
def isworkhours(workhours=[930,1620]):
	cdt = datetime.datetime.now()
	chm = cdt.hour*100+cdt.minute
	cwkd = cdt.weekday()+1
	isWH=0
	if chm>=workhours[0] and chm<=workhours[1] and cwkd<6:
		isWH = 1
	return isWH

args=sys.argv[1:]
workhours=[930,1620]
for j in range(min(len(args),len(workhours))):
	workhours[j]=int(args[j])
sys.stderr.write("Apply workhours:{}\n".format(workhours))
print(isworkhours(workhours))
