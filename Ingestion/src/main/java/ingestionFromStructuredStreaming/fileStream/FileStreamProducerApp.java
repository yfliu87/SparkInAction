package ingestionFromStructuredStreaming.fileStream;

import ingestionFromStructuredStreaming.model.FieldType;
import ingestionFromStructuredStreaming.model.RecordStructure;
import ingestionFromStructuredStreaming.utils.Config;
import ingestionFromStructuredStreaming.utils.RecordGeneratorUtils;
import ingestionFromStructuredStreaming.utils.RecordWriterUtils;

/**
 * Created by yifeiliu on 9/22/19.
 * Description:
 */
public class FileStreamProducerApp {
    private int streamDuration = 60;
    private int batchSize = 10;
    private int waitTime = 5;

    public static void main(String[] args) {
        RecordStructure rs = new RecordStructure("contact")
                .add("fname", FieldType.FIRST_NAME)
                .add("mname", FieldType.FIRST_NAME)
                .add("lname", FieldType.LAST_NAME)
                .add("age", FieldType.AGE)
                .add("ssn", FieldType.SSN);

        FileStreamProducerApp app = new FileStreamProducerApp();
        //app.start(rs, Config.StreamMode.SingleStream);
        app.start(rs, Config.StreamMode.MultipleStreams);
    }

    private void start(RecordStructure rs, Config.StreamMode streamMode) {
        long start = System.currentTimeMillis();

        while (start + streamDuration * 1000 > System.currentTimeMillis()) {
            int maxRecord = RecordGeneratorUtils.getRandomInt(batchSize) + 1;
            RecordWriterUtils.write(rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
                    rs.getRecords(maxRecord, false), streamMode);
            try {
                Thread.sleep(RecordGeneratorUtils.getRandomInt(waitTime * 1000) + waitTime * 1000 / 2);
            } catch (InterruptedException e) {

            }
        }
    }
}
