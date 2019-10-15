package ingestionFromCustomizedDataSource.utils;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yifeiliu on 9/22/19.
 * Description:
 */
public class Schema implements Serializable{
    private static final long serialVersionUID = 2376325490075130182L;

    private StructType structSchema;
    private Map<String, SchemaColumn> columns;

    public Schema() {
        columns = new HashMap<>();
    }


    public StructType getStructSchema() {
        return structSchema;
    }

    public void setStructSchema(StructType structSchema) {
        this.structSchema = structSchema;
    }

    public void add(SchemaColumn col) {
        this.columns.put(col.getColumnName(), col);
    }

    public String getMethodName(String columnName) {
        return this.columns.get(columnName).getMethodName();
    }
}
