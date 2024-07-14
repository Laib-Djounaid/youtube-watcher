import sys
import logging
import requests
from config import config
import json
import inspect

def fetch_channel_playlists(google_api_key , channel_id , page_token= None): 
    response = requests.get("https://www.googleapis.com/youtube/v3/playlists" , params={
        "key" : google_api_key , 
        "part":"snippet,contentDetails",
        "channelId": channel_id ,
        "maxResults" : 50 , 
        "pageToken" : page_token , 
     })

    if response.status_code == 200:
        payload = response.json()
        items = payload.get("items")
        playlists_list = []
        for item in items : 
            playlist_id= item["id"]
            playlist_title= item["snippet"]["title"]
            playlist_publication_date= item["snippet"]["publishedAt"]
            playlist_thumbnail_high = item["snippet"]["thumbnails"]["high"]
            infos = {
                "playlist_id": playlist_id,
                "playlist_title": playlist_title ,
                "playlist_publication_date": playlist_publication_date ,
                "playlist_thumbnail_high": playlist_thumbnail_high ,
            }
            playlists_list.append(infos)
        
        return playlists_list
        #return json.dumps(payload , indent=4 ) #This one if we want all the infos from the api
    else:
        print(f"Failed to fetch playlists: {response.status_code}")
        return None

def get_playlists_ids(playlists_list) :
    ids_list = []
    for item in playlists_list :
        ids_list.append(item.get("playlist_id"))
    return ids_list

def fetch_channel_videos(google_api_key , channel_id , page_token= None):
    pass




# google_api_key = config['google_api_key']
# channel_id = config['youtube_channel_id']
# x=fetch_channel_playlists(google_api_key , channel_id)
# print(get_playlists_ids(x))