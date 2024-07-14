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
    
    # in python 3.9 <, the | is used for dictionnary union
    

    #Using the StringSerialize function (default utf-8)

    kafka_config = config["kafka"] | {
        "key.serializer" : StringSerializer() ,
        "value.serializer" : StringSerializer()  #AvroSerializer(schema_registry_client,youtube_video_value_schema.schema.schema_str), #hna hbesst

    }
     
    #Initialization of the producer
    producer = SerializingProducer(kafka_config)

    #Getting API key from config file 
    google_api_key = config['google_api_key']
    playlist_id = config['youtube_playlist_id']
    for video_item in fetch_playlist_items(google_api_key , playlist_id) :
        if inspect.isgenerator(video_item) == False:
            print(video_item)
            video_id = video_item["contentDetails"]["videoId"]
            print(video_id)
            for video in fetch_videos(google_api_key , video_id) :
                #print(json.dumps(video_item,indent=4))
                print(pformat(summarize_video(video)))

            producer.produce(
                topic= "youtube_videos" ,
                key=video_id, 
                value= json.dumps(
                    {
                    "TITLE": video["snippet"]["title"] ,
                    "VIEWS": int(video["statistics"].get("viewCount",0)) ,
                    "LIKES": int(video["statistics"].get("likeCount",0)) ,
                    "COMMENTS": int(video["statistics"].get("commentCount",0)) 
                    }
                )
                , 
                on_delivery=on_delivery
            )
    
    producer.flush()

if __name__ == "__main__" :
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())



# -- Create a stream named youtube_videos
# CREATE STREAM youtube_videos (
#  video_id VARCHAR KEY, -- Primary key for the stream
#  title VARCHAR,        -- Title of the video
#  views INTEGER,        -- Number of views
#  comments INTEGER,     -- Number of comments
#  likes INTEGER         -- Number of likes
# )
# WITH (
#   KAFKA_TOPIC = 'youtube_videos', -- Kafka topic to read from
#  PARTITIONS = 1,
#  VALUE_FORMAT = 'avro'           -- Format of the data in the topic
# );


# CREATE TABLE youtube_changes WITH (KAFKA_TOPIC = 'youtube_changes' ) AS
# SELECT (
# video_id ,
# latest_by_offset(title) AS title ,
# latest_by_offset(comments,2)[1] AS comments_previous ,
# latest_by_offset(comments,2)[2] AS comments_current ,
# latest_by_offset(views,2)[1] AS views_previous ,
# latest_by_offset(views,2)[2] AS views_current ,
# latest_by_offset(likes,2)[1] AS likes_previous ,
# latest_by_offset(likes,2)[2] AS likes_current
# FROM  YOUTUBE_VIDEOS 
# GROUP BY video_id );

# CREATE TABLE TEST 
# WITH (KAFKA_TOPIC='test',
# PARTITIONS=1,
# REPLICAS=3,
# VALUE_FORMAT='JSON') AS
# SELECT YOUTUBE_VIDEOS.VIDEO_ID VIDEO_ID 
# FROM YOUTUBE_VIDEOS YOUTUBE_VIDEOS
# EMIT CHANGES;