package ingestionFromCustomizedDataSource.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by yifeiliu on 9/22/19.
 * Description:
 */

@Retention(RetentionPolicy.RUNTIME)
public @interface SparkColumn {

    String name() default "";

    String type() default "";

    boolean nullable() default true;
}
