package ingestionFromCustomizedDataSource.utils;

import java.io.Serializable;

/**
 * Created by yifeiliu on 9/22/19.
 * Description:
 */
public class SchemaColumn implements Serializable{
    private static final long serialVersionUID = 9113201899451270469L;
    private String methodName;
    private String columnName;

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
