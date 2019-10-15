package ingestionFromStructuredStreaming.utils;

/**
 * Created by yifeiliu on 9/28/19.
 * Description:
 */
public class Config {
    public enum StreamMode {
        SingleStream,
        MultipleStreams
    }

    public enum SchemaMode {
        WithSchema,
        WithoutSchema
    }
}
