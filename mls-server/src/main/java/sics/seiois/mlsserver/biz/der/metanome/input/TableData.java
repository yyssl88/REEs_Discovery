package sics.seiois.mlsserver.biz.der.metanome.input;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
public class TableData {
    private String tableName;
    private List<String> fkList = new ArrayList<>();
    private List<Integer> colLocation = new ArrayList<>();
    private List<Row> rows;
    private StructType schema;

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("TableName:" + tableName + "\n");
        for(String name : schema.fieldNames()){
            sb.append(name + "\n");
        }
        if(rows != null) {
            for (Row row : rows) {
                sb.append(row.toString() + "\n");
            }
        }
        return sb.toString();
    }
}
