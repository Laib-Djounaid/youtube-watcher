package Dto;
import java.sql.Timestamp;
import java.util.Arrays;

public class YoutubeChannel {

    private String channel_id ;
    private String title ;
    private String description ;
    private Timestamp publication_date;
    private String thumbnail;
    private String country ;
    private Integer views ;
    private Integer subscribers ;
    private Integer nb_videos;


    public YoutubeChannel() {} ;
    public YoutubeChannel( String channel_id ,String title , String description , Timestamp publication_date, String thumbnail,String country ,Integer views ,Integer subscribers ,Integer nb_videos) {
        this.channel_id = channel_id;
        this.title = title;
        this.description =description ;
        this.publication_date= publication_date;
        this.thumbnail = thumbnail;
        this.country = country ;
        this.views= views ;
        this.subscribers = subscribers ;
        this.nb_videos=nb_videos ;
    } ;

    public String getChannel_id() {
        return channel_id ;
    }
    public String getTitle() {
        return title ;
    }
    public String getDescription() {
        return description;
    }
    public Timestamp getPublication_date() {
        return publication_date ;
    }
    public String getThumbnail() {
        return thumbnail ;
    }
    public String getCountry() {
        return country;
    }
    
    public Integer getViews() {
        return views;
    }
    public Integer getSubscribers() {
        return subscribers;
    }
    public Integer getNb_videos() {
        return nb_videos;
    }


    @Override
    public String toString() {
        return "YoutubeChannel{" +
                "channel_id='" + channel_id + "', " +
                "title='" + title + "', " +
                "publication_date='" + publication_date + "', " +
                "description='" + description + "', " +
                "thumbnail='" + thumbnail + "', " +
                "country='" + country + "', " +
                "views=" + views + ", " +
                "subscribers= " + subscribers + ", " +
                "nb_videos=" + nb_videos +
                '}';
    }


}
