package model;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.text.SimpleDateFormat;

/**
 * Created by yifeiliu on 9/15/19.
 * Description:
 */
public class BookMapper implements MapFunction<Row,Book> {
    @Override
    public Book call(Row row) throws Exception {
        Book b = new Book();
        b.setId(row.getAs("id"));
        b.setAuthorId(row.getAs("authorId"));
        b.setLink(row.getAs("link"));
        b.setTitle(row.getAs("title"));

        String dateAsString = row.getAs("releaseDate");
        if (dateAsString != null) {
            SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
            b.setReleaseDate(parser.parse(dateAsString));
        }
        return b;
    }
}
