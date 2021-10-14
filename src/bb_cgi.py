#!/usr/bin/env python3
""" BB CGI Server with default host:localhost, port:8181 
	Program: bb_cgi.py
	Usage of: bb_cgi.py -H HOSTNAME -p PORT
	Example: http://{HOSTNAME}:{PORT}/cgi-bin/yc_curve.py
	last mod. by  Ted Hong
	Tue Dec  5 11:49:42 EST 2017
"""
import sys
if sys.version_info.major == 2:
	from BaseHTTPServer import HTTPServer
	from CGIHTTPServer import CGIHTTPRequestHandler 
else:
	from http.server import HTTPServer,CGIHTTPRequestHandler
import cgitb
from optparse import OptionParser

def bb_cgi(conn={}):
	""" python CGI Server with default host:localhost, port:8181
	"""
	xonn={'host':'localhost','port':8181,'cgi':'cgi-bin'}
	xonn.update(conn)
	# creates a new server
	cgitb.enable()  ## This line enables CGI error reporting
	server = HTTPServer
	handler = CGIHTTPRequestHandler
	server_address = (xonn['host'], xonn['port'])
	cgi_dirs=list(map(lambda x:'/'+x,xonn['cgi'].split(':')))
	handler.cgi_directories = cgi_dirs
	httpd = server(server_address,handler)
	httpd.serve_forever()

def opt_bb_cgi():
	parser = OptionParser(usage="usage: %prog [option]", version="%prog 1.0")
	parser.add_option("-H","--host",action="store",dest="host",default="localhost",
		help="server host (default: 'localhost')")
	parser.add_option("-p","--port",action="store",dest="port",default=7901,type="int",
		help="server port (default: 7901)")
	parser.add_option("-c","--cgi",action="store",dest="cgi",default="cgi-bin",
		help="cgi folders (default: 'cgi-bin')")
	(options, args) = parser.parse_args()
	return (vars(options), args)

if __name__ == '__main__':
	(options, args)=opt_bb_cgi()
	bb_cgi(options)
