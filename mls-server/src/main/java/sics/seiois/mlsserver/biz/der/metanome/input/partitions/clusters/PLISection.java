package sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class PLISection implements Serializable {
    @Getter
    @Setter
    // 这一片 PLI 的起始行 id（include），用局部行 id 表示
    private int tidStart;

    @Getter
    @Setter
    // 这一片 PLI 的末尾行 id（exclude），用局部行 id 表示。一般 tidEnd = tidStart + interval
    private int tidEnd;

    @Getter
    @Setter
    private int section;

    @Getter
    @Setter
    Map<String, PLI> PLIMap;

    @Getter
    @Setter
    private Map<String, Map<Integer, Integer>> tupleValues = new HashMap<>();

    public PLISection(){
        PLIMap = new HashMap<>();
    }

    public PLISection(int section){
        PLIMap = new HashMap<>();
        this.section = section;
    }

    public void putPLI(String colName, PLI pli) {
        PLIMap.put(colName, pli);
    }

    public PLI getPLI(String colName) {
        return PLIMap.get(colName);
    }

    public void putTupleValues(String colName, Map<Integer, Integer> tuples) {
        tupleValues.put(colName, tuples);
    }

    public Map<Integer, Integer> getTupleValues(String colName) {
        return tupleValues.get(colName);
    }


}
