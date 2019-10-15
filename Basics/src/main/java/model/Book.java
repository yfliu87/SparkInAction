package model;

import java.util.Date;

/**
 * Created by yifeiliu on 9/15/19.
 * Description:
 */
public class Book {
    int id;
    int authorId;
    String title;
    String link;
    Date releaseDate;

    public int getId() { return this.id; }

    public void setId(int id) { this.id = id; }

    public int getAuthorId() { return this.authorId; }

    public void setAuthorId(int authorId) { this.authorId = authorId; }

    public String getTitle() { return this.title; }

    public void setTitle(String title) { this.title = title; }

    public String getLink() { return this.link; }

    public void setLink(String link) { this.link = link; }

    public Date getReleaseDate() { return this.releaseDate; }

    public void setReleaseDate(Date date) { this.releaseDate = date; }
}
