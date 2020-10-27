#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Description: select info from database and display in web via flask
	Usage of:
		python app.py > app.log 2>> app.log
		OR
		FLASK_APP=app.py flask run
		OR
		export FLASK_APP=app.py; flask run
	requirements: flask, jinja2, flask_sqlchemy, pandas and json
    Last Mod., Wed Oct 24 10:31:10 EDT 2018
"""
import sys,os
from gevent.pywsgi import WSGIServer
from flask import Flask, abort, jsonify, request, render_template
from flask import send_from_directory,make_response
import pandas as pd

app = Flask(__name__,static_url_path='')

from sqlalchemy import create_engine
pguri = 'postgresql://sfdbo@localhost:5432/ara'
pgDB = create_engine(pguri)

sys.path.append("/apps/fafa/pyx/alan/")
from _alan_rmc import display_page

@app.route('/favicon.ico') 
def favicon(): 
	return send_from_directory(os.path.join(app.root_path, 'static'), 'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.errorhandler(404)
def not_found_error(e):
	#return jsonify(error_code=404), 404
	sys.stderr.write("==error:{}\n{}:{}:{}\n".format(type(e),e.code,e.name,e.description))
	errh= dict(code=e.code,name=e.name,description=e.description)
	return render_template('errorhandler.html', **errh), 404

# Note:
# hacking /None  due to report bug as img src=/None due to chartpath is None, need to debug
@app.route('/', methods=['POST', 'GET'])
@app.route('/None', methods=['POST', 'GET'])
@app.route('/home', methods=['POST', 'GET'])
@app.route('/index', methods=['POST', 'GET'])
@app.route('/overview', methods=['POST', 'GET'])
def index_page():
	return display_page(pathname='index',request=request)

@app.route('/performance', methods=['POST', 'GET'])
def performance_page():
	return display_page(pathname='performance',request=request)

@app.route('/globalmacro', methods=['POST', 'GET'])
def globalmacro_page():
	return display_page(pathname='globalmacro',request=request)

@app.route('/portfolio', methods=['POST', 'GET'])
def portfolio_page():
	return display_page(pathname='portfolio',request=request)

@app.route('/report', methods=['POST', 'GET'])
def report_page():
	return display_page(pathname='report',request=request)

@app.route('/news', methods=['POST', 'GET'])
def news_page():
	return display_page(pathname='news',request=request)

@app.route('/api/', methods=['POST', 'GET'])
@app.route('/api', methods=['POST', 'GET'])
def api_page():
	return display_page(pathname='api',request=request,rawTF=True)

@app.route('/apitest/', methods=['POST', 'GET'])
@app.route('/apitest', methods=['POST', 'GET'])
def apitest_page():
	return display_page(pathname='apitest',request=request)

#/(login|lost-password|reset-password|print|delete|reset|logout|signup)

@app.route('/login', methods=['POST', 'GET'])
def login_page():
	return display_page(pathname='login',request=request)

@app.route('/signup', methods=['POST', 'GET'])
def signup_page():
	return display_page(pathname='signup',request=request)

from geventwebsocket import WebSocketError
from geventwebsocket.handler import WebSocketHandler
if __name__ == '__main__':
	port = os.environ['PORT'] if 'PORT' in os.environ else 5011
	host = os.environ['HOST'] if 'HOST' in os.environ else 'localhost'
	app.run(host=host, port=port, debug=True, threaded=True)
	#server = WSGIServer(('', port), app, handler_class=WebSocketHandler)
	#server.serve_forever()
	#app.debug = True
	#app.jinja_env.globals.update(__builtins__)
