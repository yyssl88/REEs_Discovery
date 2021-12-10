package sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers;

import gnu.trove.list.array.TIntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class NumericalTpIDsIndexer implements ITPIDsIndexer, Serializable {

    private static final long serialVersionUID = 961087436068687061L;
    private static final Logger logger = LoggerFactory.getLogger(NumericalTpIDsIndexer.class);

    Map<Integer, TIntArrayList> valueToTpIDsMap;

    List<Integer> orderedValues;
    int scriptNull = -1; // index of empty value

    public NumericalTpIDsIndexer() {
        scriptNull = -1;
    }

    // based on hashing values
    public NumericalTpIDsIndexer(List<List<Integer>> tpIDs, int[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, TIntArrayList>();

        orderedValues = new ArrayList<Integer>(tpIDs.size());

        int count = 0;
        for (List<Integer> ids : tpIDs) {
            TIntArrayList idsInts = new TIntArrayList(ids.size());
            idsInts.addAll(ids);
            idsInts.trimToSize();

            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, idsInts);
            orderedValues.add(value);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;
        }

    }

    public NumericalTpIDsIndexer(Map<Integer, List<Integer>> tpIDs, int[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer,TIntArrayList>();

        orderedValues = new ArrayList<Integer>(tpIDs.size());

        int count = 0;
        for (List<Integer> ids : tpIDs.values()) {
            TIntArrayList idsInts = new TIntArrayList(ids.size());
            idsInts.addAll(ids);
            idsInts.trimToSize();

            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, idsInts);
            orderedValues.add(value);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;
        }

    }

    // based on hashing values
    public NumericalTpIDsIndexer(List<List<Integer>> tpIDs, int[] values, int tuple_id_start) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, TIntArrayList>();

        orderedValues = new ArrayList<Integer>(tpIDs.size());

        int count = 0;
        for (List<Integer> ids : tpIDs) {
            TIntArrayList idsInts = new TIntArrayList(ids.size());
            idsInts.addAll(ids);
            idsInts.trimToSize();

            Integer value = values[ids.get(0) - tuple_id_start];
            valueToTpIDsMap.put(value, idsInts);
            orderedValues.add(value);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;

        }

    }

    // based on hashing values
    public NumericalTpIDsIndexer(Map<Integer, List<Integer>> tpIDs, Integer[] values) {

        this.scriptNull = -1;
        valueToTpIDsMap = new HashMap<Integer, TIntArrayList>();

        orderedValues = new ArrayList<Integer>(tpIDs.size());

        int count = 0;
        for (List<Integer> ids : tpIDs.values()) {
            TIntArrayList idsInts = new TIntArrayList(ids.size());
            idsInts.addAll(ids);
            idsInts.trimToSize();

            Integer value = values[ids.get(0)];
            valueToTpIDsMap.put(value, idsInts);
            orderedValues.add(value);
            if (value == -1) {
                this.scriptNull = count;
            }
            count ++;

        }

    }

    public int getScriptNull() {
        return this.scriptNull;
    }

    public List<Integer> getValues() {
        return orderedValues;
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

    public int getIndexForValueThatIsLessThanLinear(int value) {

        int ans = -1;

        for (int i = 0; i < orderedValues.size(); i++) {
            if (orderedValues.get(i) < value) {
                return i;
            }
        }

        return ans;
    }

    // using BS
    public int getIndexForValueThatIsLessThan(int value) {
        int start = 0, end = orderedValues.size() - 1;

        int ans = -1;
        while (start <= end) {
            int mid = (start + end) / 2;

            // Move to right side if target is
            // greater.
            if (orderedValues.get(mid) >= value) {
                start = mid + 1;
            }

            // Move left side.
            else {
                ans = mid;
                end = mid - 1;
            }
        }
        return ans;
    }


    public Map<Integer, TIntArrayList> getValueToTpIDsMap() {
        return valueToTpIDsMap;
    }

//    public void setValueToTpIDsMap(Map<Integer, List<Integer>> valueToTpIDsMap) {
////        this.valueToTpIDsMap = valueToTpIDsMap;
////    }


    public List<Integer> getOrderedValues() {
        return orderedValues;
    }

    public void setOrderedValues(List<Integer> orderedValues) {
        this.orderedValues = orderedValues;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        valueToTpIDsMap.forEach((k, v) -> {

            sb.append(k + ":" + v + "\n");

        });

        sb.append("\n");

        sb.append(orderedValues);

        return sb.toString();
    }

}
