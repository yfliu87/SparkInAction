package dataTransformation.lowLevelTransformation;

import org.apache.spark.api.java.function.ReduceFunction;

/**
 * Created by yifeiliu on 10/20/19.
 * Description:
 */
public final class CountyStateConcatenator implements ReduceFunction<String> {
    private static final long serialVersionUID = 12859L;

    @Override
    public String call(String v1, String v2) throws Exception {
        return v1 + ", " + v2;
    }
}
