#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" description:Create srt file based on srt prefix name
"""
import sys
import re
from mutagen.mp3 import MP3
import datetime
from optparse import OptionParser
if sys.version_info.major == 2:
        reload(sys)
        sys.setdefaultencoding('utf8')

def srtConverter(dirname,prfname,lang='cn',dlm='',strTF=False):
	if len(dlm)<1:
		dlm = '，|；|。|。\n|：|\n' if lang=='cn' else ', |\. |\.\n|:|\n'
	textFile="{}/{}.{}".format(dirname,pfxname,"txt")
	audioFile="{}/{}.{}".format(dirname,pfxname,"mp3")
	srtFile="{}/{}.{}".format(dirname,pfxname,"srt")
	return generateSubtitle(textFile=textFile,audioFile=audioFile,srtFile=srtFile,lang=lang,dlm=dlm,strTF=strTF)

def generateSubtitle(textFile='',audioFile='',srtFile='',lang='cn',dlm='',strTF=False):
	if len(dlm)<1:
		dlm = '，|；|。|。\n|：|\n' if lang=='cn' else ', |\. |\.\n|:|\n'
	if any([textFile,audioFile,srtFile]) is False:
		return 0
	audio = MP3(audioFile)
	audioTime=audio.info.length
	if strTF is True:
		txt = textFile
	elif textFile=='-':
		txt=sys.stdin.read()
	else:
		txt=open(textFile,'r').read()
	olen=0;lst=[];nst=[]
	if sys.version_info.major == 2:
		txt = str(txt)
	ltmp=re.split(dlm,txt)
	for j,lx in enumerate(ltmp):
		xtmp=lx.strip()
		xlen=len(xtmp)+1
		if xlen>1:
			olen=olen+xlen
			lst.append(xtmp)
			nst.append(xlen)
	final=[]
	endTime='00:00:00,000'
	current=0
	for num,lx in enumerate(lst):
		startTime=endTime
		seconds=(nst[num])/float(olen)*audioTime
		current+=seconds
		endTime='0'+str(datetime.timedelta(seconds=current))[:11].replace('.',',')
		if ',' not in endTime:
			endTime += ',000'
		final.append((num+1,startTime+' --> '+endTime,lst[num]))
	sys.stderr.write("{}".format(lst))
	sys.stderr.write("{}".format(final))
	if srtFile=='-':
		fp=sys.stdout
	else:
		fp=open(srtFile,'w')
	xstr = ''
	for line in final:
		xstr += "{}\n{}\n{}\n\n".format(line[0],line[1],line[2])
		#fp.write('%d\n%s\n%s\n\n' % (line[0],line[1],line[2]))
	fp.write(xstr)
	fp.flush()
	if srtFile != '-':
		fp.close()
	return xstr

def opt_srt_convert(argv,retParser=False):
	""" command-line options initial setup
	    Arguments:
		argv:   list arguments, usually passed from sys.argv
		retParser:      OptionParser class return flag, default to False
	    Return: (options, args) tuple if retParser is False else OptionParser class
	"""
	parser = OptionParser(usage="usage: %prog [option] prefix", version="%prog 0.65",
		description="Create srt file based on srt prefix name" )
	parser.add_option("-d","--dirname",action="store",dest="dirname",
		help="dirname to save srt file(default: None)")
	parser.add_option("-l","--lang",action="store",dest="lang",default="cn",
		help="db language mode [cn|en] (default: cn)")
	(options, args) = parser.parse_args(argv[1:])
	if retParser is True:
		return parser
	return (vars(options), args)

if __name__ == '__main__':
	(ops, args)=opt_srt_convert(sys.argv)
	if len(args)>0:
		pfxname=args[0]
	else:
		pfxname="mktCmt_US_cn_1"
	if ops['dirname'] is None:
		dirname="US/mp4"
	else:
		dirname=ops['dirname']
	dlm='，|。|。\n|：|\n' if ops['lang']=='cn' else ', |\. |\.\n|:|\n'
	srtConverter(dirname,pfxname,dlm=dlm)
