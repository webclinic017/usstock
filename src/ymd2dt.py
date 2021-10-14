#!/usr/bin/env python
from datetime import datetime
def ymd2dt(ymd,dformat="%Y%m%d"):
    """ convert yyyymmdd to datetime struct format
        according to string format: [dformat]

    """
    return datetime.strptime(str(ymd),dformat)

def dt2ymd(dt,dformat="%Y%m%d"):
    """ convert datetime struct format to yyyymmdd
        according to string format: [dformat]
    """
    return dt.strftime(dformat)

if __name__ == '__main__':
	ymd=dt2ymd(datetime.today())
	print 'ymd2dt({})={}'.format(ymd,ymd2dt(ymd))
	print 'dt2ymd(today)={}'.format(dt2ymd(datetime.now()))
