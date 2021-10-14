#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    Description: select info from database and display in web via flask
	Usage of:
		python app_rmc.py > app_rmc.log 2>> app_rmc.log
		OR
		FLASK_APP=app_rmc.py flask run
		OR
		export FLASK_APP=app_rmc.py; flask run
	requirements: flask, jinja2, flask_sqlchemy, pandas and json
    Last Mod., Wed Oct 24 10:31:10 EDT 2018
"""
import sys
sys.path.append("/apps/fafa/pyx/tst/")
#from __future__ import print_function
from gevent.pywsgi import WSGIServer
from flask import Flask, abort, jsonify, request, render_template
from flask_sqlalchemy import SQLAlchemy
from _alan_rmc import *
import json
import ast
if sys.version_info.major == 2:
	reload(sys)
	sys.setdefaultencoding('utf8')

app = Flask(__name__)
uri = 'postgresql://sfdbo:sfdbo0@localhost:5432/ara'
app.config.update(SQLALCHEMY_DATABASE_URI=uri, SQLALCHEMY_TRACK_MODIFICATIONS=False)
pDB = SQLAlchemy(app)

@app.route('/', methods=['POST', 'GET'])
def indexpage():
	rqargs = request.args.to_dict()
	pqrint('**RUNNING: @ indexpage()', rqargs, file=sys.stderr)
	errMsg = ''
	try:
		if rqargs :
			name,lang,videoYN = (rqargs['name'],rqargs['lang'],'0')
			errMsg = "==TAKE: {} @{}".format((name,lang,videoYN),'as inputs.')
		else:
			name,lang,videoYN = ('AAPL','cn','0')
			errMsg = "==USE: {} @{}".format((name,lang,videoYN),'as initials.')
	except Exception, e:
		name = 'AAPL'
		lang = 'cn'
		videoYN = '0'
		errMsg = "**ERROR: {} @{}".format(str(e),'indexpage()')
	pqrint("\terrMsg:{}\n\tArgs:{}".format(errMsg, request.args.to_dict()), file=sys.stderr)
	d = qt_result(name, lang, videoYN, errMsg)
	pqrint("\tVideoYN:{}, Keys passed to aicaasi.html: {}".format(videoYN,d.keys()), file=sys.stderr)
	return render_template('aicaas.html', **d)

@app.route('/system', methods=['POST', 'GET'])
def system_call():
	try:
		xcmd = 'python '+request.args.get('cmd')
		htmlOut, _ = popenCall(xcmd)
	except Exception, e:
		htmlOut = "**ERROR: {}".format(str(e))
	return htmlOut

@app.route('/edit', methods=['POST', 'GET'])
def ace_editor():
	rqd = request.args.to_dict()
	jobj = request.get_json()
	if jobj is not None:
		jobj.update(rqd)
	else:
		pqrint("**WARNINGS: {}".format("request.get_json() has no value"), file=sys.stderr)
		jobj = rqd
	ret = ''
	editPage =  'ace_editor.html'
	try:
		if 'j2name' in jobj:
			fname= "{}/{}".format("/apps/fafa/pyx/flask/rmc/templates",jobj['j2name'])
			ret = open(fname).read()
		elif 'tmplrpt' in jobj:
			ret= jobj['tmplrpt']
		if 'editor' in jobj:
			editPage = jobj['editor']
	except Exception as e:
		sys.stderr.write("**ERROR:{}".format(str(e)))
		ret = str(e)
	jobj['j2ts'] = ret
	d = {}
	if len(jobj)>0:
		d.update(jobj)
	return render_template(editPage, **d)

@app.route('/_anaz', methods=['GET'])
def anaz():
	rqargs = request.args.to_dict()
	pqrint('**RUNNING @ anaz()', rqargs, file=sys.stderr)
	errMsg = ''
	try:
		if rqargs :
			name,lang,videoYN = (rqargs['name'],rqargs['lang'],rqargs['videoYN'])
			errMsg = "==TAKE: {} {}".format((name,lang,videoYN),'as inputs.')
		else:
			name,lang,videoYN = ('AAPL','cn','0')
			errMsg = "==USE: {} {}".format((name,lang,videoYN),'as initials.')
	except Exception, e:
		name,lang,videoYN = ('AAPL','cn','0')
		errMsg = '**ERROR:{} use {} @ {}'.format(str(e),(name,lang,videoYN),'anaz()')
	pqrint("\terrMsg:{}\n\tArgs:{}".format(errMsg,rqargs), file=sys.stderr)
	d = qt_result(name, lang, videoYN, errMsg)
	dd = {k:v for k,v in d.items() if k in ['quote','company','financials','earnings','name','lang','mp4path','error_message']}
	pqrint("\tVideoYN:{}, Keys passed to ajax : {}".format(videoYN,dd.keys()), file=sys.stderr)
	return jsonify(**dd)

@app.route('/alanapi/', methods=['GET','POST'])
def alanapi():
	dcGet = request.args.to_dict()
	dcPost = request.get_json()
	return run_alanapi(dcGet,dcPost)

@app.route('/api/', methods=['GET', 'POST'])
def page_api():
	rqd = request.args.to_dict()
	jobj = request.get_json()
	if jobj is not None:
		jobj.update(rqd)
	else:
		pqrint("**WARNINGS: {}".format("request.get_json() has no value"), file=sys.stderr)
		jobj = rqd
	dd = run_api(jobj)
	xlst = (unicode, str) if hasattr(__builtins__,"unicode") else str
	if isinstance(dd,xlst):
		return dd
	return jsonify(dd)


@app.route('/rmc', methods=['GET', 'POST'])
def page_rmc():
	rqd = request.args.to_dict()
	jobj = request.get_json()
	if jobj is not None:
		jobj.update(rqd)
	else:
		pqrint("**WARNINGS: {}".format("request.get_json() has no value"), file=sys.stderr)
		jobj = rqd
	ticker='^GSPC'
	username='ted'
	category='stock'
	tmplname='daily_briefing'
	lang='cn'
	d = dict(ticker=ticker,username=username,category=category,tmplname=tmplname,lang=lang)
	tmplstr=open('templates/{}_{}.j2'.format(tmplname,lang)).read()
	d.update(tmplstr=tmplstr)
	if jobj:
		d.update(jobj)
	try:
		xlst = list(find_lsi2nlg_list(findDct={'username':d.get('username')}))
		xlst.append('other')
		d['tmplnameLst'] = json.dumps(xlst)
	except Exception as e:
		sys.stderr.write("**ERROR:{}\n".format(str(e)))
		d['tmplnameLst'] = find_lsi2nlg_list(findDct={'username':username})
	return render_template('rmc.html', **d)

@app.route('/_wrap_rmc', methods=['GET', 'POST'])
def wrap_rmc():
	jg = request.args
	jKst = ['tmplstr', 'argstr', 'ticker', 'tmplrpt', 'username', 'category', 'tmplname', 'action']
	jobj = request.get_json()
	jobj.update(jg)
	jini=jobj.copy()
	pqrint("@ wrap_rmc(): {}\ntype: {}".format(jobj, type(jobj)), file=sys.stderr)
	try:
		jstr = run_rmc(jobj)
	except Exception, e:
		pqrint("**ERROR:{} @ {} ".format(str(e),"Json Serialization"), file=sys.stderr)
		jini["retcode"]=str(e) 
		jstr=json.dumps(jini)
	return jstr

@app.route('/lsi2nlg', methods=['GET', 'POST'])
def page_lsi2nlg():
	d = {}
	return render_template('lsi2nlg.html', **d)

@app.route('/_wrap_lsi2nlg', methods=['GET', 'POST'])
def wrap_lsi2nlg():
	jg = request.args
	jKst = ['tmplstr', 'argstr', 'prepstr', 'tmplrpt', 'username', 'category', 'tmplname', 'action']
	jLst = request.get_json()
	jobj = dict(zip(jKst, jLst))
	jobj.update(jg)
	pqrint(('@ wrap_lsi2nlg(): {}\ntype: {}').format(jobj, type(jobj)), file=sys.stderr)
	jobj = lsi2nlg_calc(jobj)
	pqrint(jobj, stream = sys.stderr)
	return json.dumps(jobj)

from geventwebsocket import WebSocketError
from geventwebsocket.handler import WebSocketHandler
if __name__ == '__main__':
	app.run(host='127.0.0.1', debug=True, threaded=True)
	#server = WSGIServer(('', 5000), app, handler_class=WebSocketHandler)
	#server.serve_forever()
	#app.debug = True
	#app.jinja_env.globals.update(__builtins__)
