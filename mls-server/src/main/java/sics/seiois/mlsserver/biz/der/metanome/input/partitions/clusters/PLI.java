package sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters;

import java.io.Serializable;
import java.util.*;


import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.CategoricalTpIDsIndexer;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.ITPIDsIndexer;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.NumericalTpIDsIndexer;

public class PLI implements Serializable {

    private static final long serialVersionUID = -6632843229256239029L;

    List<List<Integer>> tpIDs;
    LinkedHashMap<Integer, List<Integer>> tpIDMap;
    ITPIDsIndexer tpIDsIndexer;
    long relSize;
    boolean numerical;

    int scriptNull = -1;

    public PLI() {
        scriptNull = -1;//后续生成es时用到这个值
    }
    // descending order for numerical
    public PLI(List<Set<Integer>> setPlis, long relSize, boolean numerical) {
        tpIDs = new ArrayList<>(setPlis.size());
        this.relSize = relSize;
        this.numerical = numerical;

        for (Set<Integer> set : setPlis) {
            List<Integer> tidsList = new ArrayList<>(set);
            tpIDs.add(tidsList);
        }
        Collections.reverse(tpIDs);
        this.scriptNull = -1;
    }

    public PLI(List<Set<Integer>> setPlis, long relSize, boolean numerical, int[] values) {
        tpIDs = new ArrayList<>(setPlis.size());
        this.relSize = relSize;
        this.numerical = numerical;

        for (Set<Integer> set : setPlis) {
            List<Integer> tidsList = new ArrayList<>(set);
            tpIDs.add(tidsList);
        }
        Collections.reverse(tpIDs);
        if (numerical) {
            tpIDsIndexer = new NumericalTpIDsIndexer(tpIDs, values);
            this.scriptNull = tpIDsIndexer.getScriptNull();
        } else {
            tpIDsIndexer = new CategoricalTpIDsIndexer(tpIDs, values);
            this.scriptNull = tpIDsIndexer.getScriptNull();
        }
    }

    public PLI(List<Set<Integer>> setPlis, long relSize, boolean numerical, int[] values, int tuple_id_start) {
        tpIDs = new ArrayList<>(setPlis.size());
        this.relSize = relSize;
        this.numerical = numerical;

        for (Set<Integer> set : setPlis) {
            List<Integer> tidsList = new ArrayList<>(set);
            tpIDs.add(tidsList);
        }
        Collections.reverse(tpIDs);
        if (numerical) {
            tpIDsIndexer = new NumericalTpIDsIndexer(tpIDs, values, tuple_id_start);
            this.scriptNull = tpIDsIndexer.getScriptNull();
        } else {
            tpIDsIndexer = new CategoricalTpIDsIndexer(tpIDs, values, tuple_id_start);
            this.scriptNull = tpIDsIndexer.getScriptNull();
        }
    }

    public PLI(Map<Integer, Set<Integer>> setPlis, long relSize, boolean numerical, Integer[] values) {
        tpIDMap = new LinkedHashMap<>(setPlis.size());
        this.relSize = relSize;
        this.numerical = numerical;

        List<Integer> keySet = new ArrayList<>(setPlis.keySet());
        Collections.sort(keySet);
        for (Integer key : keySet) {
            List<Integer> tidsList = new ArrayList<>(setPlis.get(key));
            tpIDMap.put(key, tidsList);
        }

        if (numerical) {
            tpIDsIndexer = new NumericalTpIDsIndexer(tpIDMap, values);
            this.scriptNull = tpIDsIndexer.getScriptNull();
        } else {
            tpIDsIndexer = new CategoricalTpIDsIndexer(tpIDMap, values);
            this.scriptNull = tpIDsIndexer.getScriptNull();
        }

        tpIDs = null;
//        List<List<Integer>> list = new ArrayList<>();
//        for (Map.Entry<Integer, List<Integer>> entry : tpIDMap.entrySet()) {
//            list.add(entry.getValue());
//        }
//        tpIDs = list;
    }

    public PLI(Map<Integer, Set<Integer>> setPlis, long relSize, boolean numerical, Collection<Integer> values) {
        tpIDMap = new LinkedHashMap<>(setPlis.size());
        this.relSize = relSize;
        this.numerical = numerical;

        List<Integer> keySet = new ArrayList<>(setPlis.keySet());
        Collections.sort(keySet);
        for (Integer key : keySet) {
            List<Integer> tidsList = new ArrayList<>(setPlis.get(key));
            tpIDMap.put(key, tidsList);
        }

        if (numerical) {
            tpIDsIndexer = new NumericalTpIDsIndexer(tpIDMap, values.toArray(new Integer[]{}));
            this.scriptNull = tpIDsIndexer.getScriptNull();
        } else {
            tpIDsIndexer = new CategoricalTpIDsIndexer(tpIDMap, values.toArray(new Integer[]{}));
            this.scriptNull = tpIDsIndexer.getScriptNull();
        }

        tpIDs = null;
//        List<List<Integer>> list = new ArrayList<>();
//        for (Map.Entry<Integer, List<Integer>> entry : tpIDMap.entrySet()) {
//            list.add(entry.getValue());
//        }
//        tpIDs = list;
    }

    public int getScriptNull() {
        return this.scriptNull;
    }

    public Collection<Integer> getValues() {
        return tpIDsIndexer.getValues();
    }

    public boolean isNumerical() {
        return numerical;
    }

    public void setNumerical(boolean numerical) {
        this.numerical = numerical;
    }


    public long getRelSize() {
        return relSize;
    }

    public void setRelSize(long relSize) {
        this.relSize = relSize;
    }

    public List<List<Integer>> getPlis() {
        return tpIDs;
    }

    public void setPlis(List<List<Integer>> plis) {
        this.tpIDs = plis;
    }

    public LinkedHashMap<Integer, List<Integer>> getPlisByMap() {
        return tpIDMap;
    }

    public void setPliMap(LinkedHashMap<Integer, List<Integer>> plis) {
        this.tpIDMap = plis;
    }

    public List<Integer> getTpIDsForValue(Integer value) {
        return tpIDsIndexer.getTpIDsForValue(value);
    }

    public int getIndexForValueThatIsLessThan(int value) {
        return tpIDsIndexer.getIndexForValueThatIsLessThan(value);
    }


    public ITPIDsIndexer getTpIDsIndexer() {
        return tpIDsIndexer;
    }

    public boolean getNumerical() {
        return this.numerical;
    }

    public void setTpIDsIndexer(ITPIDsIndexer tpIDsIndexer) {
        this.tpIDsIndexer = tpIDsIndexer;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Integer key : tpIDMap.keySet()) {
            List<Integer> list = (List<Integer>) tpIDMap.get(key);
            sb.append(list + "\n");

        }
        // sb.append("\n");
        // if (numerical)
        // sb.append((NumericalTpIDsIndexer) tpIDsIndexer);
        // else
        // sb.append((CategoricalTpIDsIndexer) tpIDsIndexer);

        return sb.toString();
    }

}
