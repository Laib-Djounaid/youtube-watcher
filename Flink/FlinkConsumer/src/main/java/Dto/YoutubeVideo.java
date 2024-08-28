package Dto;

import java.sql.Timestamp;
import java.util.Arrays;

public class YoutubeVideo {
    private String video_id ;
    private String title ;
    private Timestamp publication_date ;
    private String thumbnail ;
    private String category_id ;
    private String[] tags ;
    private Integer views ;
    private Integer likes ;
    private Integer comments;

    public YoutubeVideo() {} ;

    public YoutubeVideo(String video_id, String title , Timestamp publication_date , String thumbnail, String category_id , String[] tags, int views , int likes , int comments) {
        this.video_id = video_id;
        this.publication_date = publication_date ;
        this.thumbnail = thumbnail ;
        this.category_id = category_id ;
        this.tags = tags ;
        this.title = title;
        this.views = views;
        this.likes = likes;
        this.comments = comments ;
    }

    public String getVideo_id() {
        return video_id;
    }
    public Timestamp getPublication_date() {
        return publication_date;
    }

    public String getThumbnail() {
        return thumbnail;
    }

    public String[] getTags() {
        return tags;
    }

    public String getCategory_id() {
        return category_id;
    }

    public String getTitle() {
        return title;
    }

    public Integer getViews() {
        return views;
    }

    public Integer getLikes() {
        return likes;
    }

    public Integer getComments() {
        return comments;
    }

    @Override
    public String toString() {
        return "YoutubeVideo{" +
                "video_id='" + video_id + "', " +
                "title='" + title + "', " +
                "publication_date='" + publication_date + "', " +
                "tags='" + Arrays.toString(tags) + "', " +
                "thumbnail='" + thumbnail + "', " +
                "category_id='" + category_id + "', " +
                "views=" + views + ", " +
                "likes= " + likes + ", " +
                "comments=" + comments +
                '}';
    }
}
