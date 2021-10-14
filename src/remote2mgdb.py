#!/usr/bin/env python3
import sys
from sshtunnel import SSHTunnelForwarder
import pymongo


def remote2mgdb(sshUser='rstudio',sshPass='',host='',port=27017):
	if not host:
		return None,None
	serverM = SSHTunnelForwarder(
		host,
		ssh_username=sshUser,
		ssh_password=sshPass,
		remote_bind_address=('127.0.0.1', port)
	)
	serverM.start()

	print("==remote server:",serverM,file=sys.stderr)
	# serverM.local_bind_port is assigned local port

	clientM = pymongo.MongoClient('127.0.0.1', serverM.local_bind_port) 
	print("==remote clientM:",clientM,file=sys.stderr)

	#serverM.stop()
	return serverM, clientM
	
	
def example():
	serverM, clientM=remote2mgdb(host='api1.beyondbond.com',sshPass='rs@10279')
	dbname='ara'
	dbM = clientM[dbname]
	#print(dbM.collection_names(),file=sys.stderr)
	r=clientM['ara']['rssNews'].find({},sort=[("pubDate",-1)],limit=1)
	print(list(r),file=sys.stderr)
	if clientM:
		clientM.close()
		serverM.stop()

if __name__ == '__main__':
	example()
