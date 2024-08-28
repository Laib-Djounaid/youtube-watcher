from pprint import pformat
import sys
import logging
import requests
from config import config
import json
import inspect
from confluent_kafka.serializing_producer import SerializingProducer
from confluent_kafka.serialization import StringSerializer


from confluent_kafka.serialization import Serializer


from confluent_kafka.schema_registry import SchemaRegistryClient
#from confluent_kafka.schema_registry.avro import AvroSerializer


def fetch_channel_data(google_api_key, channel_id):
    response = requests.get("https://www.googleapis.com/youtube/v3/channels" , params={
        'key' : google_api_key , 
        "id": channel_id ,
        "part" : "contentDetails,snippet,statistics",
        "maxResults" : 70 , 
     })
    payload = json.loads(response.text)
    logging.debug("Got %s",json.dumps(payload,indent=4))
    return payload

def fetch_playlist_items_page(google_api_key , playlist_id, page_token=None) :
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems" , params={
        'key' : google_api_key , 
        "playlistId": playlist_id ,
        "part" : "content_details",
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
        "maxResults" : 10 , 
        "pageToken" : page_token , 
     })
    payload = json.loads(response.text)
    logging.debug("Got %s",json.dumps(payload,indent=4))

    return payload

def fetch_videos(google_api_key , video_id, page_token = None) :
    payload = fetch_videos_page(google_api_key, video_id, page_token)
    #print(payload)
    yield from payload["items"]
    next_page_token = payload.get("nextPageToken") # Why did we use get instead of []
    if next_page_token is not None: 
        yield fetch_videos(google_api_key , video_id , next_page_token)

def on_delivery(err, record):
    if err is not None:
        logging.error('Message delivery failed: %s', err)
    else:
        logging.info('Message delivered to %s [%d] @ %d', record.topic(), record.partition(), record.offset())

def summarize_video(video) :
    return {
        "video_id": video["id"] ,
        "title": video["snippet"]["title"] ,
        "views": int(video["statistics"].get("viewCount",0)) ,
        "likes": int(video["statistics"].get("likeCount",0)) ,
        "comments": int(video["statistics"].get("commentCount",0)) ,
    }

def main() :
    logging.info("START")
    #Kafka configuration :
    #schema_registry_client = SchemaRegistryClient(config["schema_registry"])
    #youtube_video_value_schema = schema_registry_client.get_latest_version("watcher")
        
    #Using the StringSerialize function (default utf-8)
    kafka_config = {
        "key.serializer" : StringSerializer() ,
        "value.serializer" : StringSerializer() ,  #AvroSerializer(schema_registry_client,youtube_video_value_schema.schema.schema_str), #hna hbesst
        'bootstrap.servers': 'localhost:9092'
    }

    #Getting API key from config file 
    google_api_key = config['google_api_key']
    list_of_channels=config["list_of_channels"]


    #Initializing the producer :
    producer0 = SerializingProducer(kafka_config)
    producer = SerializingProducer(kafka_config)
    
    cmpt = 0
    for channel_id in list_of_channels :
        #Get the main data for that channel : 
        channel_data= fetch_channel_data(google_api_key,channel_id)
        #Get the id for the main playlist of that channel :
        upload_playlist_id= channel_data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        print("L'id de la playlist principale est : ",upload_playlist_id)
        #Produce main data of the channel : 
        producer0.produce(
                #topic= "topic_for_channel_"+channel_data["items"][0]["snippet"]["customUrl"] ,
                topic = "new_channel" ,
                key= channel_id, 
                value= json.dumps(
                    {
                    "channel_id" : channel_id , # hadi normalement troh direct lfo9
                    "title" : channel_data["items"][0]["snippet"]["title"],
                    "description" : channel_data["items"][0]["snippet"]["description"], 
                    "publication_date" : channel_data["items"][0]["snippet"]["publishedAt"],
                    "thumbnail" : channel_data["items"][0]["snippet"]["thumbnails"]["high"]["url"],
                    "country" : channel_data["items"][0]["snippet"].get("country",None),
                    "views": int(channel_data["items"][0]["statistics"].get("viewCount",0)) ,
                    "subscribers": int(channel_data["items"][0]["statistics"].get("subscriberCount",0)) ,
                    "nb_videos": int(channel_data["items"][0]["statistics"].get("videoCount",0)) 
                    }
                )
                , 
                on_delivery=on_delivery
                )
        print("le nombre de videos est : ",channel_data["items"][0]["statistics"].get("videoCount",0))
        
        #Produce data relative to the videos of that channel :

        #playlist_id = config['youtube_playlist_id']
        playlist_id = upload_playlist_id

        for video_item in fetch_playlist_items(google_api_key , playlist_id) :
            if inspect.isgenerator(video_item) == False:
                #print(video_item)
                video_id = video_item["contentDetails"]["videoId"]
                #print(video_id)
                for video in fetch_videos(google_api_key , video_id) :
                    #print(pformat(summarize_video(video)))
                    print(json.dumps(video,indent=4))

                producer.produce(
                    #topic= "topic_for_video_"+video_id+"channel_"+channel_data["items"][0]["snippet"]["customUrl"],
                    topic ='new_videos',
                    key=video_id, 
                    value= json.dumps(
                        {
                        "video_id" : video_id ,
                        "publication_date" : video["snippet"]["publishedAt"],
                        "title": video["snippet"]["title"] ,
                        "thumbnail" : video["snippet"]["thumbnails"]["high"]["url"],
                        "category_id" : video["snippet"]["categoryId"],
                        "tags": video["snippet"].get("tags",None) ,
                        "views": int(video["statistics"].get("viewCount",0)) ,
                        "likes": int(video["statistics"].get("likeCount",0)) ,
                        "comments": int(video["statistics"].get("commentCount",0)) 
                        }
                    )
                    , 
                    on_delivery=on_delivery
                )

                #print(f"Ceci est la #{cmpt} video")
                #cmpt+=1

    
    producer.flush()
    producer0.flush()

if __name__ == "__main__" :
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
