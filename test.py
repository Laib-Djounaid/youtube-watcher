import sys
import logging
import requests
from config import config
import json
import inspect



def fetch_playlist_items_page(google_api_key , playlist_id, page_token=None) :
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems" , params={
        'key' : google_api_key , 
        "playlistId": playlist_id ,
        "part" : "content_details" ,
        "maxResults" : 70 , 
        "pageToken" : page_token , 
     })
    payload = json.loads(response.text)
    logging.debug("Got %s",json.dumps(payload,indent=4))

    return payload

def fetch_playlist_items(google_api_key , playlist_id, page_token = None) :
    payload = fetch_playlist_items_page(google_api_key, playlist_id, page_token)
    yield from payload["items"]
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None : 
        yield fetch_playlist_items(google_api_key , playlist_id , next_page_token)
        
def fetch_videos_page(google_api_key , video_id, page_token=None) :
    response = requests.get("https://www.googleapis.com/youtube/v3/videos" , params={
        'key' : google_api_key , 
        "id": video_id ,
        "part" : "snippet,statistics" ,
        #"maxResults" : 70 , 
        "pageToken" : page_token , 
     })
    payload = json.loads(response.text)
    logging.debug("Got %s",json.dumps(payload,indent=4))

    return payload



def fetch_videos(google_api_key , video_id, page_token = None) :
    payload = fetch_videos_page(google_api_key, video_id, page_token)
    #print(payload)
    yield payload["items"]
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None: 
        yield fetch_videos(google_api_key , video_id , next_page_token)


video_id = 'UwtLjqLhsYA'
google_api_key = config['google_api_key']
for v in fetch_videos(google_api_key , video_id) :
    print(v)
#print(fetch_videos(google_api_key , video_id))