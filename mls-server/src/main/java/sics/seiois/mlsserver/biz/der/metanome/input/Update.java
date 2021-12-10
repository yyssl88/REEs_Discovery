package sics.seiois.mlsserver.biz.der.metanome.input;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class Update {
    private int tid;
    private ArrayList<String> relations;
    private ArrayList<String> columns;
    private ArrayList<Integer> replacedValues;

    public Update(int tid) {
        this.tid = tid;
        relations = new ArrayList<>();
        columns = new ArrayList<>();
        replacedValues = new ArrayList<>();
    }

    public void add(String relation_name, String column_name, Integer replacedValue) {
        this.relations.add(relation_name);
        this.columns.add(column_name);
        this.replacedValues.add(replacedValue);
    }

    public int getTid() {
        return tid;
    }

    public int getUpdateNum() {
        return relations.size();
    }

    public String fetchRelation(int sc) {
        return relations.get(sc);
    }

    public String fetchColumns(int sc) {
        return columns.get(sc);
    }

    public Integer fetchValue(int sc) {
        return replacedValues.get(sc);
    }

}
