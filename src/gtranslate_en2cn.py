#!/usr/bin/env python3
'''


'''
import sys
import requests
import re
import html

def part_of_text(s='',ptn='',thd=2500,debugTF=False):
	'''
	like Part Of Speech to break part of string 's' based on pattern 'ptn'
	and re-join them with 'thd' character threashold
	'''
	if len(s)<1:
		return []
	elif len(s)<thd:
		return [s]
	if len(ptn)<1:
		ptn=r'(\. )|(\n)'
	jt=1
	jb=b=0
	vs=[]
	for x in re.finditer(ptn,s):
		e =  x.end()
		sys.stderr.write('{},{},{}\n'.format(b,e,e-b))
		sys.stderr.write(s[b:e]+'\n')
		if e>jt*thd:
			je = b
			vs.append(s[jb:je])
			if debugTF:
				sys.stderr.write('{},{},{}\n'.format(jb,je,je-jb))
				sys.stderr.write("===\n"+s[jb:je]+'\n')
			jb = je
			jt = jt+1
		b = e
	je = e
	if je>jb:
		vs.append(s[jb:je])
		if debugTF:
			sys.stderr.write('{},{},{}\n'.format(jb,je,je-jb))
			sys.stderr.write("===\n"+s[jb:je]+'\n')
	return vs

def wrt_db(dd={}, dbname='med',tablename='daily_med',zpk={'setid'}):
	mobj,_,_ = write2mdb(dd,dbname=dbname,tablename=tablename,zpk=zpk)
	return mobj

def chk_db(setid='', dbname='med',tablename='daily_med'):
	if len(setid)<1:
		return {}
	xg,_,_ = find_mdb(dbname=dbname,tablename=tablename,jobj={"setid":setid},field={"drug_name","setid","title","title_cn","sec_cn"})
	return xg

def en2cnX(dscr='',src='en',dest='zh-TW'):
	from googletrans import Translator
	import copy
	dscr =html.unescape(dscr)
	dscr = str(dscr.strip()+"\n")
	if len(dscr)<1:
		return dscr
	try:
		ret = Translator().translate(dscr,src=src,dest=dest)
		newd = copy.deepcopy(ret.text)
		return newd
	except Exception as e:
		sys.stderr.write("**ERROR: @{} of {}\nINPUT:\n{}\n".format("en2cn",str(e),dscr))
		return ''

def en2cn(dscr='',ptn='',thd=1500,src='en',dest='zh-TW'):
	vs = part_of_text(s=dscr,ptn=ptn,thd=thd)
	sc=''
	for s in vs:
		sc += en2cnX(s,src=src,dest=dest)
	return sc

#--------------------------------------------------
if __name__ == '__main__':
	args=sys.argv[1:]
	if len(args)<1 or args[0]=='-':
		dscr =sys.stdin.read()
	elif len(args)>1 and args[0].upper()=="URL":
		import xml.etree.ElementTree as ET
		text = requests.get(args[1]).text
		dscr = ''.join(ET.fromstring(text).itertext())
	else:
		dscr = args[0]
	strCN = en2cn(dscr=dscr,ptn='',thd=1500)
	sys.stdout.write(strCN+"\n")
