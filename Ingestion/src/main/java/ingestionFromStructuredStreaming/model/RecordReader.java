package ingestionFromStructuredStreaming.model;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yifeiliu on 9/28/19.
 * Description:
 */
public class RecordReader extends ForeachWriter<Row> {

    private int streamId = 0;
    private static Logger logger = LoggerFactory.getLogger(RecordReader.class);

    public RecordReader(int streamId) {
        this.streamId = streamId;
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        return true;
    }

    @Override
    public void process(Row value) {
        if (value.length() != 5) {
            return;
        }
        int age = value.getInt(3);
        if (age < 13) {
            logger.info("On stream #{}: {} is a kid, they are {} yrs old.", streamId, value.getString(0), age);
        } else if (age > 12 && age < 20) {
            logger.info("On stream #{}: {} is a teen, they are {} yrs old.", streamId, value.getString(0), age);
        } else if (age > 64) {
            logger.info("On stream #{}: {} is a senior, they are {} yrs old.", streamId, value.getString(0), age);
        }
    }

    @Override
    public void close(Throwable errorOrNull) {

    }
}
