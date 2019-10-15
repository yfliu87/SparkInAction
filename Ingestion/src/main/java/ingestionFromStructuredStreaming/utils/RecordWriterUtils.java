package ingestionFromStructuredStreaming.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Created by yifeiliu on 9/22/19.
 * Description:
 */
public class RecordWriterUtils {
    private static final Logger logger = LoggerFactory.getLogger(RecordWriterUtils.class);

    public static void write(String filename, StringBuilder record, Config.StreamMode streamMode) {
        switch(streamMode) {
            case SingleStream:
                write(filename, record, StreamingUtils.getInputDirectory());
                break;
            case MultipleStreams:
                String directory = new Random().nextBoolean() ? StreamingUtils.getInputSubDirectory1() : StreamingUtils.getInputSubDirectory2();
                write(filename, record, directory);
                break;
            default:
                logger.error("Unrecognized stream mode: " + streamMode);
                break;
        }
    }

    public static void write(String filename, StringBuilder record, String directory) {
        if (!directory.endsWith(File.separator)) {
            directory += File.separator;
        }

        String fullFilename = directory + filename;
        logger.info("Writing in: {}", fullFilename);

        BufferedWriter out = null;
        try {
            FileWriter fstream = new FileWriter(fullFilename, true);
            out = new BufferedWriter(fstream);
        } catch (IOException e) {
            logger.error("Error while opening file: {}", e.getMessage());
        }

        try {
            out.write(record.toString());
        } catch (IOException e) {
            logger.error("Error while writing: {}", e.getMessage());
        }

        try {
            out.close();
        } catch (IOException e) {
            logger.error("Error while closing the file: {}", e.getMessage());
        }
    }
}
