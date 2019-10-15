package ingestionFromCustomizedDataSource;

import ingestionFromCustomizedDataSource.utils.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by yifeiliu on 9/22/19.
 * Description:
 */
public class ExifDirectoryRelation
        extends BaseRelation
        implements Serializable, TableScan{

    private static final long serialVersionUID = 4598175080399877334L;
    private static transient Logger log = LoggerFactory.getLogger(ExifDirectoryRelation.class);
    private SQLContext sqlContext;
    private Schema schema = null;
    private RecursiveExtensionFilteredLister photoLister;

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    public void setSqlContext(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    @Override
    public StructType schema() {
        if (schema == null) {
            schema = SparkBeanUtils.getSchemaFromBean(PhotoMetadata.class);
        }
        return schema.getStructSchema();
    }

    @Override
    public RDD<Row> buildScan() {
        log.debug("-> buildScan()");
        schema();

        List<PhotoMetadata> table = collectData();

        @SuppressWarnings("resource")
        JavaSparkContext sparkContext = new JavaSparkContext(sqlContext.sparkContext());
        return sparkContext.parallelize(table)
                .map(photo -> SparkBeanUtils.getRowFromBean(schema, photo))
                .rdd();
    }

    private List<PhotoMetadata> collectData() {
        return this.photoLister.getFiles()
                .stream()
                .map(photo -> ExifUtils.processFromFilename(photo.getAbsolutePath()))
                .collect(Collectors.toList());
    }

    public void setPhotoLister(RecursiveExtensionFilteredLister photoLister) {
        this.photoLister = photoLister;
    }
}
