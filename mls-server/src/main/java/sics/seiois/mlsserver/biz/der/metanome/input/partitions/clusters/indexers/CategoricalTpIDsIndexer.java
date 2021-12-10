package sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers;

import gnu.trove.list.array.TIntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class CategoricalTpIDsIndexer implements ITPIDsIndexer, Serializable {

    private static final long serialVersionUID = 961087336068687061L;
    private static final Logger logger = LoggerFactory.getLogger(CategoricalTpIDsIndexer.class);

    Map<Integer, TIntArrayList> valueToTpIDsMap;
    int scriptNull = -1;

    public CategoricalTpIDsIndexer() {
        this.scriptNull = -1;
    }

    // based on hashing values
    public CategoricalTpIDsIndexer(List<List<Integer>> tpIDs, int[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, TIntArrayList>();

        int count = 0;
        for (List<Integer> ids : tpIDs) {
            TIntArrayList idsInts = new TIntArrayList(ids.size());
            idsInts.addAll(ids);
            idsInts.trimToSize();

            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, idsInts);
            if (value == -1) {
                this.scriptNull = count;
            }
            count++;
        }

    }

    public CategoricalTpIDsIndexer(Map<Integer, List<Integer>> tpIDs, Integer[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, TIntArrayList>();

        int count = 0;
        for (List<Integer> ids : tpIDs.values()) {
            TIntArrayList idsInts = new TIntArrayList(ids.size());
            idsInts.addAll(ids);
            idsInts.trimToSize();


            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, idsInts);
            if (value == -1) {
                this.scriptNull = count;
            }
            count++;
        }

    }

    // based on hashing values
    public CategoricalTpIDsIndexer(List<List<Integer>> tpIDs, int[] values, int tuple_id_start) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, TIntArrayList>();

        int count = 0;
        for (List<Integer> ids : tpIDs) {
            TIntArrayList idsInts = new TIntArrayList(ids.size());
            idsInts.addAll(ids);
            idsInts.trimToSize();

            Integer value = values[ids.get(0) - tuple_id_start];
            valueToTpIDsMap.put(value, idsInts);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;
        }

    }

    // based on hashing values
    public CategoricalTpIDsIndexer(Map<Integer, List<Integer>> tpIDs, Integer[] values, int tuple_id_start) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, TIntArrayList>();

        int count = 0;
        for (List<Integer> ids : tpIDs.values()) {
            TIntArrayList idsInts = new TIntArrayList(ids.size());
            idsInts.addAll(ids);
            idsInts.trimToSize();


            Integer value = values[ids.get(0) - tuple_id_start];
            valueToTpIDsMap.put(value, idsInts);
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
        TIntArrayList tIntArrayList = valueToTpIDsMap.get(value);
        if (tIntArrayList ==null) return null;
        List<Integer> r = new ArrayList<>(tIntArrayList.size());
        for (int i = 0; i < tIntArrayList.size(); i++) {
            int i1 = tIntArrayList.get(i);
            r.add(i1);
        }
        return r;
    }

    public int getIndexForValueThatIsLessThan(int value) {
        return -1; // not applied to categorical
    }

    public void setValueToTpIDsMap(Map<Integer, TIntArrayList> valueToTpIDsMap) {
        this.valueToTpIDsMap = valueToTpIDsMap;
    }
    public Map<Integer, TIntArrayList> getValueToTpIDsMap() {
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