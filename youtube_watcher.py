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
        "maxResults" : 70 , 
        "pageToken" : page_token , 
     })
    payload = json.loads(response.text)
    logging.debug("Got %s",json.dumps(payload,indent=4))

    return payload



def fetch_videos(google_api_key , video_id, page_token = None) :
    payload = fetch_videos_page(google_api_key, video_id, page_token)
    #print(payload)
    yield from payload["items"]
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None: 
        yield fetch_videos(google_api_key , video_id , next_page_token)



def main() :
    logging.info("START")
    google_api_key = config['google_api_key']
    playlist_id = config['youtube_playlist_id']
    for video_item in fetch_playlist_items(google_api_key , playlist_id) :
        if inspect.isgenerator(video_item) == False:
            print(video_item)
            video_id = video_item["contentDetails"]["videoId"]
            print(video_id)
            for video_item in fetch_videos(google_api_key , video_id) :
                print(json.dumps(video_item,indent=4))


if __name__ == "__main__" :
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())