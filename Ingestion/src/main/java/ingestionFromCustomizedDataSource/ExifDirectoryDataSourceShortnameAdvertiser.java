package ingestionFromCustomizedDataSource;

import org.apache.spark.sql.sources.DataSourceRegister;

/**
 * Created by yifeiliu on 9/22/19.
 * Description:
 */
public class ExifDirectoryDataSourceShortnameAdvertiser
        extends ExifDirectoryDataSource
        implements DataSourceRegister {

    @Override
    public String shortName() {
        return "exif";
    }
}
