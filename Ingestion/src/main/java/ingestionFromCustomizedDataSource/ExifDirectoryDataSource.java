package ingestionFromCustomizedDataSource;

import ingestionFromCustomizedDataSource.utils.Keys;
import ingestionFromCustomizedDataSource.utils.RecursiveExtensionFilteredLister;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;

/**
 * Created by yifeiliu on 9/22/19.
 * Description:
 */
public class ExifDirectoryDataSource implements RelationProvider {

    private static Logger log = LoggerFactory.getLogger(ExifDirectoryDataSource.class);

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        java.util.Map<String,String> optionsAsJavaMap =
                (java.util.Map<String,String>) mapAsJavaMapConverter(parameters).asJava();

        ExifDirectoryRelation br = new ExifDirectoryRelation();
        br.setSqlContext(sqlContext);

        RecursiveExtensionFilteredLister photoLister = new RecursiveExtensionFilteredLister();

        for (java.util.Map.Entry<String, String> entry : optionsAsJavaMap
                .entrySet()) {
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue();
            log.debug("[{}] --> [{}]", key, value);

            switch (key) {
                case Keys.PATH:
                    photoLister.setPath(value);
                    break;

                case Keys.RECURSIVE:
                    if (value.toLowerCase().charAt(0) == 't') {
                        photoLister.setRecursive(true);
                    } else {
                        photoLister.setRecursive(false);
                    }
                    break;

                case Keys.LIMIT:
                    int limit;
                    try {
                        limit = Integer.valueOf(value);
                    } catch (NumberFormatException e) {
                        log.error(
                                "Illegal value for limit, expecting a number, got: {}. {}. Ignoring parameter.",
                                value, e.getMessage());
                        limit = -1;
                    }
                    photoLister.setLimit(limit);
                    break;

                case Keys.EXTENSIONS:
                    String[] extensions = value.split(",");
                    for (int i = 0; i < extensions.length; i++) {
                        photoLister.addExtension(extensions[i]);
                    }
                    break;

                default:
                    log.warn("Unrecognized parameter: [{}].", key);
                    break;
            }
        }

        br.setPhotoLister(photoLister);
        return br;
    }
}
