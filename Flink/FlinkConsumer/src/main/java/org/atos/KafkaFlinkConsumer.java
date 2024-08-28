package org.atos;
import Deserializer.JSONValueDeserializationSchema;
import Deserializer.JSONValueDeserializationSchema0;
import Dto.YoutubeChannel;
import Dto.YoutubeVideo;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

// import javax.lang.model.type.ArrayType;
// import java.util.Properties;


public class KafkaFlinkConsumer {

    public static void main(String[] args) throws Exception {


    //Retrieve credentials from snowflake_credentials.txt

    List<String> cred = new ArrayList<>();

    try {
      File myObj = new File("Flink\\FlinkConsumer\\src\\main\\java\\org\\atos\\snowflake_credentials.txt");
      Scanner myReader = new Scanner(myObj);
      while (myReader.hasNextLine()) {
        String data = myReader.nextLine();
        cred.add(data) ;
      }
      myReader.close();
    } catch (FileNotFoundException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
    }



        String account_name = cred.get(0) ;
        String snowflake_username = cred.get(1) ;
        String snowflake_password = cred.get(2) ;
        String database_name = "FlinkSink" ;
        String schema_name = "Public" ;
        String jdbcUrl = String.format("jdbc:snowflake://%s.snowflakecomputing.com/?db=%s&schema=%s",account_name,database_name,schema_name) ;

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic0= "new_channel";
        String topic= "new_videos";

        // Kafka properties

        // If using a remote Flink cluster, set the job manager address
        //env.setParallelism(1); // Optional: Adjust parallelism if needed

        KafkaSource<YoutubeChannel> kafkaConsumer0 = KafkaSource.<YoutubeChannel>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic0)
                .setGroupId("flink-group")
                .setProperty("request.timeout.ms", "30000")  // Increase timeout
                .setProperty("metadata.max.age.ms", "10000")  // Adjust metadata refresh rate
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema0())
                .build();

        
        KafkaSource<YoutubeVideo> kafkaConsumer = KafkaSource.<YoutubeVideo>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("flink-group")
                .setProperty("request.timeout.ms", "30000")  // Increase timeout
                .setProperty("metadata.max.age.ms", "10000")  // Adjust metadata refresh rate
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        // Add the Kafka source to the execution environment
        DataStream<YoutubeChannel> channel_stream = env.fromSource(kafkaConsumer0, WatermarkStrategy.noWatermarks() , "Kafka Source") ;
        DataStream<YoutubeVideo> video_stream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks() , "Kafka Source") ;

        //Addd sink for Snowflake : 

        JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(String.format("jdbc:snowflake://%s.snowflakecomputing.com/?db=%s&schema=%s",account_name,database_name,schema_name))
                .withDriverName("net.snowflake.client.jdbc.SnowflakeDriver")
                .withUsername(snowflake_username)
                .withPassword(snowflake_password)
                .build();


        //CREATING OUR OLAP CUBE 
        //-----------------------



        //CREATING THE CHANNELS DIMENSION :

        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            Connection connection = DriverManager.getConnection(String.format("jdbc:snowflake://%s.snowflakecomputing.com/?db=%s&schema=%s",account_name,database_name,schema_name), snowflake_username, snowflake_password);

            Statement statement = connection.createStatement();

            String createTableSQL = //"USE WAREHOUSE DATALOAD_WH; " +
                "CREATE OR REPLACE TABLE CHANNELS (" +
                "CHANNEL_ID STRING, " +
                "TITLE STRING, " +
                "PUBLICATION_DATE TIMESTAMP, " +
                "COUNTRY STRING, " +
                "THUMBNAIL STRING, " +
                "VIEWS INTEGER, " +
                "SUBSCRIBER INTEGER, " +
                "NB_VIDEOS INTEGER, " +
                "DESCRIPTION STRING)";

            statement.executeUpdate(createTableSQL);

            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // CREATING SINK AND UPLOADING DATA TO SNOWFLAKE CHANNELS TABLE

        channel_stream.addSink(JdbcSink.sink(
                "INSERT INTO CHANNELS (CHANNEL_ID, TITLE, PUBLICATION_DATE, COUNTRY, THUMBNAIL, VIEWS, SUBSCRIBER, NB_VIDEOS, DESCRIPTION)"+
                "VALUES (?,?,?,?,?,?,?,?,?) ; " ,
                 (ps, t) -> {
                        ps.setString(1, t.getChannel_id());
                        ps.setString(2, t.getTitle());
                        ps.setTimestamp(3, t.getPublication_date());
                        ps.setString(4, t.getCountry());
                        ps.setString(5, t.getThumbnail());
                        ps.setInt(6, t.getViews());
                        ps.setInt(7, t.getSubscribers());
                        ps.setInt(8, t.getNb_videos());
                        ps.setString(9, t.getDescription());

                        
                },
                execOptions,
                connOptions
                ));
        
        
        //CREATING VIDEOS DIMENSION

        try {
                Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
                Connection connection = DriverManager.getConnection(String.format("jdbc:snowflake://%s.snowflakecomputing.com/?db=%s&schema=%s",account_name,database_name,schema_name), snowflake_username, snowflake_password);
    
                Statement statement = connection.createStatement();
    
                String createTableSQL = //"USE WAREHOUSE DATALOAD_WH; " +
                    "CREATE OR REPLACE TABLE VIDEOS (" +
                    "VIDEO_ID STRING, " +
                    "TITLE STRING, " +
                    "PUBLICATION_DATE TIMESTAMP, " +
                    "CATEGORY_ID STRING, " +
                    "THUMBNAIL STRING, " +
                    "TAGS STRING, " +
                    "VIEWS INTEGER, " +
                    "LIKES INTEGER, " +
                    "COMMENTS INTEGER)";
    
                statement.executeUpdate(createTableSQL);
    
                statement.close();
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        
        // CREATING SINK AND UPLOADING DATA TO SNOWFLAKE CHANNELS TABLE
        
        
        video_stream.addSink(JdbcSink.sink(
                "INSERT INTO VIDEOS (VIDEO_ID, TITLE, PUBLICATION_DATE, CATEGORY_ID, THUMBNAIL, TAGS, VIEWS, LIKES, COMMENTS)"+
                "VALUES (?,?,?,?,?,?,?,?,?) ; " ,
                 (ps, t) -> {
                        ps.setString(1, t.getVideo_id());
                        ps.setString(2, t.getTitle());
                        ps.setTimestamp(3, t.getPublication_date());
                        ps.setString(4, t.getCategory_id());
                        ps.setString(5, t.getThumbnail());
                        ps.setString(6, Arrays.toString(t.getTags()));
                        ps.setInt(7, t.getViews());
                        ps.setInt(8, t.getLikes());
                        ps.setInt(9, t.getComments());

                        
                },
                execOptions,
                connOptions
                ));

        
       

        // FILLING THE TIME DIMENSION
        //If is unfilled uncomment
        //fill_time_table(account_name, snowflake_username, snowflake_password, database_name, schema_name);
        // -- Insert data into the FLINKSINK.PUBLIC.TIME table
        // INSERT INTO FLINKSINK.PUBLIC.TIME (TIME_ID, YEAR, MONTH, DAY, HOUR, IS_WEEKEND)
        // WITH RECURSIVE DateSeries AS (
        // SELECT CAST('2006-01-01 00:00:00' AS TIMESTAMP) AS DateTime
        // UNION ALL
        // SELECT DateTime + INTERVAL '1 hour'
        // FROM DateSeries
        // WHERE DateTime < '2030-12-31 23:00:00'
        // )
        // SELECT CONCAT(
        //         EXTRACT(YEAR FROM DateTime), '-', 
        //         LPAD(EXTRACT(MONTH FROM DateTime)::TEXT, 2, '0'), '-', 
        //         LPAD(EXTRACT(DAY FROM DateTime)::TEXT, 2, '0'), '-', 
        //         LPAD(EXTRACT(HOUR FROM DateTime)::TEXT, 2, '0')
        // ) AS TIME_ID,
        // EXTRACT(YEAR FROM DateTime) AS YEAR,
        // EXTRACT(MONTH FROM DateTime) AS MONTH,
        // EXTRACT(DAY FROM DateTime) AS DAY,
        // EXTRACT(HOUR FROM DateTime) AS HOUR,
        // CASE WHEN EXTRACT(DOW FROM DateTime) IN (0, 6) THEN TRUE ELSE FALSE END AS IS_WEEKEND
        // FROM DateSeries;


        //Printing records pour verifier : 

        System.out.println("Stream channel : ");
        channel_stream.print();
        // System.out.println("Stream videos : ");
        // video_stream.print();

        // Execute the Flink job
        env.execute("Flink Kafka Consumer Example");
    }


    private static boolean isLeapYear(int year) {
        return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    }

    private static void fill_time_table(String account_name ,String snowflake_username ,String snowflake_password , String database_name , String schema_name ) {
        
        // CREATING AND FILLING THE TIME DIMENSION

         try {
                Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
                Connection connection = DriverManager.getConnection(String.format("jdbc:snowflake://%s.snowflakecomputing.com/?db=%s&schema=%s",account_name,database_name,schema_name), snowflake_username, snowflake_password);
    
                Statement statement = connection.createStatement();
    
                String createTableSQL = //"USE WAREHOUSE DATALOAD_WH; " +
                    "CREATE OR REPLACE TABLE TIME (" +
                    "TIME_ID STRING, " +
                    "YEAR INTEGER, " +
                    "MONTH INTEGER, " +
                    "DAY INTEGER, " +
                    "HOUR INTEGER)";

                    statement.executeUpdate(createTableSQL);


                    List<Integer> thirtyDayMonths = Arrays.asList(4, 6, 9, 11);
            
                    for (int y = 2006; y <= 2024; y++) {
                        for (int m = 1; m <= 12; m++) {
                            int maxDay = 31;
            
                            if (thirtyDayMonths.contains(m)) {
                                maxDay = 30;
                            } else if (m == 2) {
                                if (isLeapYear(y)) {
                                    maxDay = 29;
                                } else {
                                    maxDay = 28;
                                }
                            }
            
                            for (int d = 1; d <= maxDay; d++) {
                                for (int h = 0; h < 24; h++) {
                                    String time_id = Integer.toString(y)+Integer.toString(m)+Integer.toString(d)+Integer.toString(h) ;
                                    statement.executeUpdate(String.format("INSERT INTO TIME VALUES (%s, %d, %d, %d, %d)",time_id, y, m, d, h));
                                    System.out.println(String.format("(%s, %d, %d, %d, %d)",time_id, y, m, d, h));
                                }
                            }
                        }
                
                
    
                statement.close();
                connection.close();
            }} catch (Exception e) {
                e.printStackTrace();
            }
    }

}