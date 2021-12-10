package sics.seiois.mlsserver.biz.der.metanome.input;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import sics.seiois.mlsserver.biz.der.metanome.helpers.ParserHelper;


public class Column {

    public enum Type {
        STRING, NUMERIC, LONG
    }

    private final String tableName;
    private final String name;
    @Deprecated
    private HashSet<String> valueSet = new HashSet<>();
    private List<String> values = new ArrayList<String>();
    private Type type = Type.LONG;

    public Type getType() {
        if (name.contains("String")) {
            return Type.STRING;
        }
        if (name.contains("Double")) {
            return Type.NUMERIC;
        }
        if (name.contains("Integer")) {
            return Type.LONG;
        }
        return type;
    }

    public Column(String tableName, String name) {
        this.tableName = tableName;
        this.name = name;
    }

    // set type
    public void setType(String columnType) {
        if ("STRING".equals(columnType.toUpperCase())
                || "VARCHAR".equals(columnType.toUpperCase())
                || "CHAR".equals(columnType.toUpperCase())
                || "TEXT".equals(columnType.toUpperCase())
                || columnType.toLowerCase().matches("^varchar\\([1-9][0-9]*\\)")    //varchar(n)
                || columnType.toLowerCase().matches("^char\\([1-9][0-9]*\\)")){
            this.type = Type.STRING;
        } else if ("DOUBLE".equals(columnType.toUpperCase())
                || "FLOAT".equals(columnType.toUpperCase())
                || "DECIMAL".equals(columnType.toUpperCase())
                || columnType.toLowerCase().matches("^float[48]")) {      //float4 float8
            this.type = Type.NUMERIC;
        } else if ("INTEGER".equals(columnType.toUpperCase())
                || "INT".equals(columnType.toUpperCase())
                || "TINYINT".equals(columnType.toUpperCase())
                || "SMALLINT".equals(columnType.toUpperCase())
                || "MEDIUMINT".equals(columnType.toUpperCase())
                || "BIGINT".equals(columnType.toUpperCase())
                || columnType.toLowerCase().matches("^int[248]")             //int2 int4 int8
                || columnType.toLowerCase().matches("^serial[248]")) {      //serial2 serial4 serial8
            this.type = Type.LONG;
        } else {
            this.type = Type.STRING;
        }
    }

    // check String
    public boolean checkStringType() {
        if (this.type == Type.STRING) {
            return true;
        }
        return false;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return tableName + "." + name;
    }

    public String getTableName() {
        return tableName;
    }

    public void addLine(String string) {
        if (type == Type.LONG && !ParserHelper.isInteger(string)) {
            type = Type.NUMERIC;
        }
        if (type == Type.NUMERIC && !ParserHelper.isDouble(string)) {
            type = Type.STRING;
        }
//        valueSet.add(string);
        values.add(string);
    }

    public Comparable<?> getValue(int line) {
        switch (type) {
            case LONG:
                return new Long(Long.parseLong(values.get(line)));
            case NUMERIC:
                return new Double(Double.parseDouble(values.get(line)));
            case STRING:
            default:
                return values.get(line);

        }
    }

    public Long getLong(int line) {
        Long l = Long.valueOf(values.get(line));
        return l == null ? Long.valueOf(Long.MIN_VALUE) : l;

    }

    public Double getDouble(int line) {
        Double d = Double.valueOf(values.get(line));
//		return d == null ? Double.valueOf(Double.NaN) : d;
        return d == null ? Double.valueOf(Double.MIN_VALUE) : d;
    }

    public String getString(int line) {
        return values.get(line) == null ? "" : values.get(line);
    }
}
