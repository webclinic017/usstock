#!/usr/bin/env python
'''
bb_api.py
BB api to alanapi.py
last mod. 
Ted Hong
Thu Jun 25 15:38:37 EDT 2020
'''
import sys
import gevent
from gevent import socket
from gevent.server import StreamServer
from gevent.pool import Pool
from optparse import OptionParser

#from alanapi import cgi_api

def import_from(modname, fncname):
    module = __import__(modname, fromlist=[fncname])
    return getattr(module, fncname)

def handle(sock, address):

	fout=open('bb_api.log', 'a')
	fp = sock.makefile()

	while True:
		line = fp.readline()
		if line:
			fout.write("INPUT:%s" % line)
			rst=appfnc(line)
			if rst['responseTEXT']:
				fp.write(rst['responseTEXT'])
				fp.flush()
		else:
			break
	sock.shutdown(socket.SHUT_WR)
	sock.close()

def bb_api(conn={}):
	xonn={'host':'localhost','port':7902,'modname':'alanapi','appname':'cgi_api','npool':100}
	xonn.update(conn)
	#locals().update(xonn)
	global appfnc
	appfnc=import_from(xonn['modname'], xonn['appname'])

	# creates a new server
	server = StreamServer((xonn['host'], xonn['port']), handle, spawn=Pool(xonn['npool']))

	# start accepting new connections
	server.start() 
	server.serve_forever()

def opt_pyserver():
	parser = OptionParser(usage="usage: %prog [option] APPNAME", version="%prog 1.0")
	parser.add_option("-H","--host",action="store",dest="host",default="localhost",
		help="server host (default: 'localhost')")
	parser.add_option("-p","--port",action="store",dest="port",default=7902,type="int",
		help="server port (default: 7902)")
	parser.add_option("-n","--npool",action="store",dest="npool",default=100,type="int",
		help="listening pools(default: 100)")
	parser.add_option("-m","--modname",action="store",dest="modname",default="alanapi",
		help="module name (default: 'alanapi')")
	parser.add_option("-a","--appname",action="store",dest="appname",default="cgi_api",
		help="app name (default: 'cgi_api')")
	(options, args) = parser.parse_args()
	return (vars(options), args)

if __name__ == '__main__':
	(options, args)=opt_pyserver()
	bb_api(options)
