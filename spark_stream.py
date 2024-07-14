import logging
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import ExecutionProfile

#from cassandra.io.asyncioreactor import AsyncioConnection
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
                            WTIH REPLICATION = {
                                'class':'SimpleStrategy' ,
                                'replication_factor': 1
                            }
    """)

    logging.log("................................Kayspace created successfull................................y")

def create_table(session) :
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.videos (
                    video_id TEXT PRIMARY KEY ,
                    VIEWS int ,
                    LIKES int ,
                    COMMENTS TEXT
                    )
    """)
    logging.log("................................Table created successfullyv................................")


def insert_data(session ,**kwargs):
    print("Inserting data ...")

    video_id = kwargs.get('video_id')
    views = kwargs.get('views')
    likes = kwargs.get('likes')
    comments = kwargs.get('comments')

    try :
        session.execute("""
                        INSERT INTO spark_streams.videos (video_id , views , likes , comments)
                        VALUES (%s,%d,%d,%s)
                        """, (video_id, views, likes,comments))
        logging.log("Data inserted successfully in the table")

    except Exception as e :
        logging.error(f"................................Couldn't insert data due to the following error : {e}................................")



def create_spark_connection():
    try :
        conn = ( SparkSession.builder \
        .appName("SparkDataStreaming")\
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1")
            .config('spark.cassandra.connection.host', 'localhost')
            .config('spark.master', 'spark://spark-master:7077')
        .getOrCreate() )

        conn.sparkContext.setLogLevel("DEBUG")
        logging.info(".................................Connection created successfully.................................")

        return conn

    except Exception as e :
        logging.error(f"................................Couldn't connect to spark due to the following error : {e}................................")
        return None



def create_cassandra_connection():    
    try :
        # Cassandra connection parameters
        hostname = 'localhost'  # or 'cassandra' if you're connecting from another Docker container
        #That means that if we deployed this application on another docker container
        #madam les containers yemcho m3a ba3dahom then we use the hostname ta3 container cassandra_db
        #li houwa cassandra
        port = 9042
        username = 'cassandra'
        password = 'cassandra'
    
        auth_provider = PlainTextAuthProvider( username=username, password=password)
        
        #Learn how to use Execution profiles
        #For exemple a po=rfile for dev and another for production in order to use load_balancing policy
        load_balancing_policy= DCAwareRoundRobinPolicy(local_dc="datacenter1")
        cluster = Cluster(  #load_balancing_policy=load_balancing_policy ,
                            contact_points=[hostname] ,
                            port= port ,
                            protocol_version= 5 ,
                            auth_provider=auth_provider ,
                            # Adjust the protocol version number as per your cluster compatibility
                          ) # , connection_class= AsyncioConnection)
        session = cluster.connect()
        logging.info("................................Cassandra session created successfully................................")

        return session

    except Exception as e :
        logging.error(f"................................Couldn't create cassandra connection due to the following error : {e}................................")
        return None


def connect_to_kafka(spark_conn):
    logging.setLoggerClass()
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'youtube_videos') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("................................kafka dataframe created successfully................................")

        return spark_df

    except Exception as e:
        logging.debug(f"................................kafka dataframe could not be created because: {e}")

        return None

def select_connection_df_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("videos_id",StringType(),False),
            StructField("views",StringType(),True),
            StructField("likes",StringType(),True),
            StructField("comments",StringType(),True)
        ]
    )
    sel = spark_df.selectExpr("CAST(value AS STRING)").select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)
    return sel





if __name__ == "__main__" :
    #spark_conn = create_spark_connection()
    # if spark_conn is not None :
    #     cassandra_session = create_cassandra_connection()
    #     spark_df= connect_to_kafka(spark_conn)
    #     selection_df=select_connection_df_from_kafka(spark_df)

    #     if cassandra_session is not None :
    #         key_space = create_keyspace(cassandra_session)
    #         table = create_table(cassandra_session)

    #         logging.info("Streaming is being started...")


    #         streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
    #                            .option('checkpointLocation', '/tmp/checkpoint')
    #                            .option('keyspace', 'spark_streams')
    #                            .option('table', 'videos')
    #                            .start())

    #         streaming_query.awaitTermination()
    cassandra_session = create_cassandra_connection()
