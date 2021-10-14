#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Retrieve/Update/Delete the authenticated user's uploaded videos.
(u2_list.py DEPRECATED)
Note, Client secret and credential json files are based on the user:bbface 
  (facebook@beyondbond.com) for youtube api
Last mod., Wed Aug 22 22:54:48 EDT 2018
	
"""
import httplib2
import os
import sys
import pandas as pd
import datetime
if sys.version_info.major == 2:
	reload(sys)
	sys.setdefaultencoding('utf8')

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
#from oauth2client.tools import argparser, run_flow
from oauth2client.tools import run_flow


# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret. You can acquire an OAuth 2.0 client ID and client secret from
# the {{ Google Cloud Console }} at
# {{ https://cloud.google.com/console }}.
# Please ensure that you have enabled the YouTube Data API for your project.
# For more information about using OAuth2 to access the YouTube Data API, see:
#	 https://developers.google.com/youtube/v3/guides/authentication
# For more information about the client_secrets.json file format, see:
#	 https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
CLIENT_SECRETS_FILE = "/home/rstudio/.client_secrets.json"

# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account.
YOUTUBE_READ_WRITE_SCOPE = "https://www.googleapis.com/auth/youtube"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# This variable defines a message to display if the CLIENT_SECRETS_FILE is
# missing.
MISSING_CLIENT_SECRETS_MESSAGE = """
WARNING: Please configure OAuth 2.0

To make this sample run you will need to populate the client_secrets.json file
found at:

	 %s

with information from the {{ Cloud Console }}
{{ https://cloud.google.com/console }}

For more information about the client_secrets.json file format, please visit:
https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
""" % os.path.abspath(os.path.join(os.path.dirname(__file__),
																	 CLIENT_SECRETS_FILE))

def get_authenticated_service(args):
	flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE,
		scope=YOUTUBE_READ_WRITE_SCOPE,
		message=MISSING_CLIENT_SECRETS_MESSAGE)

	#storage = Storage("%s-oauth2.json" % sys.argv[0])
	storage = Storage("/home/rstudio/.youtube-upload-credentials.json")
	credentials = storage.get()

	if credentials is None or credentials.invalid:
		credentials = run_flow(flow, storage, args)

	return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
		http=credentials.authorize(httplib2.Http()))

def update_video(clnt, options):
	# Call the API's videos.list method to retrieve the video resource.
	videos_list_response = clnt.videos().list(
		id=options.video_id,
		part='snippet'
	).execute()

	# If the response does not contain an array of "items" then the video was
	# not found.
	if not videos_list_response["items"]:
		print("Video '%s' was not found." % options.video_id)
		sys.exit(1)

	# Since the request specified a video ID, the response only contains one
	# video resource. This code extracts the snippet from that resource.
	videos_list_snippet = videos_list_response["items"][0]["snippet"]

	# Preserve any tags already associated with the video. If the video does
	# not have any tags, create a new array. Append the provided tag to the
	# list of tags associated with the video.
	if "tags" not in	videos_list_snippet:
		videos_list_snippet["tags"] = []
	videos_list_snippet["tags"].append(options.tag)

	# Update the video resource by calling the videos.update() method.
	videos_update_response = clnt.videos().update(
		part='snippet',
		body=dict(
			snippet=videos_list_snippet,
			id=options.video_id
		)).execute()

def get_my_uploads_list(clnt):
	# Retrieve the contentDetails part of the channel resource for the
	# authenticated user's channel.
	channels_response = clnt.channels().list(
		mine=True,
		part='contentDetails'
	).execute()

	for channel in channels_response['items']:
		# From the API response, extract the playlist ID that identifies the list
		# of videos uploaded to the authenticated user's channel.
		return channel['contentDetails']['relatedPlaylists']['uploads']

	return None

def list_my_uploaded_videos(clnt,uploads_playlist_id,opts):
	# Retrieve the list of videos uploaded to the authenticated user's channel.
	playlistitems_list_request = clnt.playlistItems().list(
		playlistId=uploads_playlist_id,
		part='snippet',
		maxResults=5
	)

	#print('Videos in list %s' % uploads_playlist_id)
	d=[]
	while playlistitems_list_request:
		playlistitems_list_response = playlistitems_list_request.execute()

		# Print information about each video.
		hdr = ["playlist_id","title","video_id","publish_at","description","kind","epochs"]
		dTitle = opts['title']
		dVideoId = opts['video_id']
		for j,playlist_item in enumerate(playlistitems_list_response['items']):
			if dTitle and dTitle not in playlist_item['snippet']['title']:
				continue
			if dVideoId and dVideoId != playlist_item['snippet']['resourceId']['videoId']:
				continue
			#print(playlist_item.keys())
			#print(playlist_item['snippet']['resourceId'].keys())
			#print(playlist_item['snippet'].keys();exit(1))
			kind = playlist_item['snippet']['resourceId']['kind']
			playlist_id = playlist_item['snippet']['playlistId']
			title = playlist_item['snippet']['title']
			video_id = playlist_item['snippet']['resourceId']['videoId']
			publish_at = playlist_item['snippet']['publishedAt']
			description = playlist_item['snippet']['description']
			epochs = int(datetime.datetime.strptime(publish_at,'%Y-%m-%dT%H:%M:%S.000Z').strftime('%s'))
			cln = (playlist_id,title,video_id,publish_at,description,kind,epochs)
			#print('{}'.format(cln))
			d.append(dict(zip(hdr,cln)))

		playlistitems_list_request = clnt.playlistItems().list_next(
			playlistitems_list_request, playlistitems_list_response)
	if len(d) < 1:
		return None
	df=pd.DataFrame(d)[hdr]
	return df

# Sample python code for videos.delete
def delete_video(clnt, **kwargs):
	# See full sample for function
	response = clnt.videos().delete( **kwargs).execute()
	return response

from argparse import ArgumentParser
def parse_args(version="0.1",description="Retrieve/Update/Delete the authenticated user's uploaded videos"):
	""" command-line options initial setup
	    Note, nargs: [?|+|*] for [0 or 1 | 1+ array | 0+ array ]
	"""
	parser = ArgumentParser(version=version,description=description)
	parser.add_argument("--title",action="store",dest="title",
		help="partial title (default: None)")
	parser.add_argument("--video_id",action="store",dest="video_id",
		help="video ID (default: None)")
	parser.add_argument("command",action="store", nargs='?',default="retrieve",
		help="COMMAND to [retrieve|delete] (default: retrieve)")
	ns_args = parser.parse_args()
	return(dict(ns_args._get_kwargs()), ns_args)

if __name__ == "__main__":
	#argparser.add_argument("--video-id", help="ID of video to update.")#,required=True)
	#argparser.add_argument("--tag", default="ai-caas", help="Additional tag to add to video.")
	#argparser.add_argument("--title", help="TITLE of video to retrieve")
	#args = argparser.parse_args()
	opts,ns_args = parse_args()
	clnt = get_authenticated_service(None)
	try:
		uploads_playlist_id = get_my_uploads_list(clnt)
		if uploads_playlist_id:
			vdf = list_my_uploaded_videos(clnt,uploads_playlist_id,opts)
			if vdf is None:
				print('No selected videos')
				exit(1)
			#dTitle=opts['title']
			#dLst=vdf.query("title=='{}'".format(dTitle))
			#OR
			#dLst = vdf[vdf.title.str.contains(dTitle.decode('utf-8'))]
			dLst = vdf.loc[vdf.epochs.sort_values(ascending=True).index[:]].reset_index(drop=True)
			if opts['command'] == 'delete':
				for idx,xlst in dLst.T.items():
					#print >> sys.stderr, idx,xlst
					try:
						print("==Deleting video [{title}] {video_id}".format(**xlst))
						delete_video(clnt, id=xlst['video_id'])
					except HttpError as e:
						print("**HTTP error %d occurred:\n%s" % (e.resp.status, e.content))
			else:
				print(dLst[['title','video_id','publish_at']].to_csv(sep="\t",index=False))
		else:
			print('There is no uploaded videos playlist for this user.')
		#update_video(clnt, args)
	except HttpError as e:
		print("**HTTP error %d occurred:\n%s" % (e.resp.status, e.content))
	#else:
	#	print("Tag '%s' was added to video id '%s'." % (args.tag, args.video_id))
