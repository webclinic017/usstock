#!/usr/bin/env python
""" Use .htpasswd file and add an api_key using md5() conversion 
	and the save them to database table htpasswd
    Note, to add newuser/pass
	htpasswd /apps/fafa/phx/loginDir/.htpasswd [newuser]
"""
import sys
from hashlib import md5
from sqlalchemy import create_engine
import pandas as pd

htfile = sys.argv[1] if len(sys.argv)>1 else "/apps/fafa/phx/loginDir/.htpasswd"
print >> sys.stderr, "Input file: {}".format(htfile)

ht=pd.read_csv(htfile,header=None,sep=":",names=["user","pass"])
ht['api_key']=map(lambda x: md5(x.split('$')[-1]).hexdigest(), ht['pass'])

# save output to file
htdat= htfile+'.dat'
print >> sys.stderr, "Output saved to file: {}\n{}".format(htdat,ht)
ht.to_csv(htdat,sep="\t",index=False)

# save output to database
dbname='eSTAR_2'
dbURL='postgresql://sfdbo@{0}:5432/{1}'.format('localhost',dbname)
engine= create_engine(dbURL)
print >> sys.stderr, "htpasswd saved to database {}::{}".format(dbname,'htpasswd')
ht.to_sql('htpasswd', engine, schema='public', index=False, if_exists='replace')
