package sics.seiois.mlsserver.biz.der.mining.utils;

import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.ITPIDsIndexer;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.NumericalTpIDsIndexer;

import java.io.Serializable;
import java.util.*;

public class NumericalTpIDsIndexerLight implements ITPIDsIndexer, Serializable {

    private static final long serialVersionUID = 571067436167627057L;
    private static final Logger logger = LoggerFactory.getLogger(NumericalTpIDsIndexerLight.class);

    Map<Integer, List<Integer>> valueToTpIDsMap;
    int scriptNull = -1; // index of empty value

    public NumericalTpIDsIndexerLight() {
        scriptNull = -1;
    }


//    public NumericalTpIDsIndexerLight(NumericalTpIDsIndexer index) {
//        this.valueToTpIDsMap = index.getValueToTpIDsMap();
//    }


    // based on hashing values
    public NumericalTpIDsIndexerLight(List<List<Integer>> tpIDs, int[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, List<Integer>>();

        int count = 0;
        for (List<Integer> ids : tpIDs) {
            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, ids);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;
        }

    }

    public NumericalTpIDsIndexerLight(Map<Integer, List<Integer>> tpIDs, int[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, List<Integer>>();
        int count = 0;
        for (List<Integer> ids : tpIDs.values()) {
            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, ids);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;
        }

    }

    // based on hashing values
    public NumericalTpIDsIndexerLight(List<List<Integer>> tpIDs, int[] values, int tuple_id_start) {

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
    public NumericalTpIDsIndexerLight(Map<Integer, List<Integer>> tpIDs, Integer[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, List<Integer>>();

        int count = 0;
        for (List<Integer> ids : tpIDs.values()) {

            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, ids);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;

        }

    }

    public int getScriptNull() {
        return this.scriptNull;
    }

    @Override
    public Collection<Integer> getValues() {
        return null;
    }

    public List<Integer> getTpIDsForValue(Integer value) {

        return valueToTpIDsMap.get(value);

    }

    @Override
    public int getIndexForValueThatIsLessThan(int value) {
        return 0;
    }


    public Map<Integer, List<Integer>> getValueToTpIDsMap() {
        return valueToTpIDsMap;
    }

    public void setValueToTpIDsMap(Map<Integer, List<Integer>> valueToTpIDsMap) {
        this.valueToTpIDsMap = valueToTpIDsMap;
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
