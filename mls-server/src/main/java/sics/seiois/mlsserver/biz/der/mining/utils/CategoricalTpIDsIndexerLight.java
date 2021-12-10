package sics.seiois.mlsserver.biz.der.mining.utils;

import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.CategoricalTpIDsIndexer;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.ITPIDsIndexer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CategoricalTpIDsIndexerLight implements ITPIDsIndexer, Serializable {

    private static final long serialVersionUID = 971087336068687097L;
    private static final Logger logger = LoggerFactory.getLogger(CategoricalTpIDsIndexerLight.class);

    Map<Integer, List<Integer>> valueToTpIDsMap;
    int scriptNull = -1;

    public CategoricalTpIDsIndexerLight() {
        this.scriptNull = -1;
    }


//    public CategoricalTpIDsIndexerLight(CategoricalTpIDsIndexer index) {
//        this.valueToTpIDsMap = index.getValueToTpIDsMap();
//    }

    // based on hashing values
    public CategoricalTpIDsIndexerLight(List<List<Integer>> tpIDs, int[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, List<Integer>>();

        int count = 0;
        for (List<Integer> ids : tpIDs) {
            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, ids);
            if (value == -1) {
                this.scriptNull = count;
            }
            count++;
        }

    }

    public CategoricalTpIDsIndexerLight(Map<Integer, List<Integer>> tpIDs, Integer[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, List<Integer>>();

        int count = 0;
        for (List<Integer> ids : tpIDs.values()) {
            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, ids);
            if (value == -1) {
                this.scriptNull = count;
            }
            count++;
        }

    }

    // based on hashing values
    public CategoricalTpIDsIndexerLight(List<List<Integer>> tpIDs, int[] values, int tuple_id_start) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, List<Integer>>();

        int count = 0;
        for (List<Integer> ids : tpIDs) {
            Integer value = values[ids.get(0) - tuple_id_start];
            valueToTpIDsMap.put(value, ids);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;
        }

    }

    // based on hashing values
    public CategoricalTpIDsIndexerLight(Map<Integer, List<Integer>> tpIDs, Integer[] values, int tuple_id_start) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, List<Integer>>();

        int count = 0;
        for (List<Integer> ids : tpIDs.values()) {
            Integer value = values[ids.get(0) - tuple_id_start];
            valueToTpIDsMap.put(value, ids);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;
        }

    }

    public int getScriptNull(){
        return this.scriptNull;
    }

    public Set<Integer> getValues() {
        return valueToTpIDsMap.keySet();
    }

    public List<Integer> getTpIDsForValue(Integer value) {
        return valueToTpIDsMap.get(value);
    }

    public int getIndexForValueThatIsLessThan(int value) {
        return -1; // not applied to categorical
    }

    public void setValueToTpIDsMap(Map<Integer, List<Integer>> valueToTpIDsMap) {
        this.valueToTpIDsMap = valueToTpIDsMap;
    }
    public Map<Integer, List<Integer>> getValueToTpIDsMap() {
        return valueToTpIDsMap;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        valueToTpIDsMap.forEach((k, v) -> {
            sb.append(k + ":" + v + "\n");
        });
        sb.append("\n");

        return sb.toString();
    }

}