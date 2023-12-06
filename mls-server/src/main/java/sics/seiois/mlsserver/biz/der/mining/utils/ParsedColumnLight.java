package sics.seiois.mlsserver.biz.der.mining.utils;

import com.google.common.collect.HashMultiset;
import de.metanome.algorithm_integration.ColumnIdentifier;
import gnu.trove.list.array.TIntArrayList;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import sics.seiois.mlsserver.biz.der.metanome.input.ParsedColumn;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLI;
import sics.seiois.mlsserver.model.PredicateConfig;

import java.io.Serializable;
import java.util.*;


public class ParsedColumnLight<T extends Comparable<T>> implements Serializable {

    private static final long serialVersionUID = -377843229256777037L;

    private String tableName;
    private String name;

    private long uniqueConstantNumber;

    private Class<T> type;

    //transient private List<Integer> valuesInt = new ArrayList<>();
    // transient private PLILight pliLight;

    // key : partition ID
    private HashMap<Integer, TIntArrayList> valuesIntPartitions;
    transient private HashMap<Integer, PLILight> pliLightListPartition;
    transient private HashMap<Integer, ImmutablePair<Integer, Integer>> tidsIntervals;

    public HashMap<Integer, TIntArrayList> getValuesIntPartitions() {
        return this.valuesIntPartitions;
    }

    public HashMap<Integer, PLILight> getPliLightListPartition() {
        return this.pliLightListPartition;
    }

    public HashMap<Integer, ImmutablePair<Integer, Integer>> getTidsIntervals() {
        return this.tidsIntervals;
    }

    public TIntArrayList getValuesInt(int pid) {
        return this.valuesIntPartitions.get(pid);
    }

    public String toStringData() {
        return this.tableName + "." + this.name;
    }

    public long getUniqueConstantNumber() {
        return this.uniqueConstantNumber;
    }

    public Class<T> getType() {
        return this.type;
    }

    public ParsedColumnLight(ParsedColumn<?> col, Class<T> type) {
        ArrayList<Integer> usedPIDs = new ArrayList<>();
        for (int pid = 0; pid < col.getPliSections().size(); pid++) {
            usedPIDs.add(pid);
        }
        this.tableName = col.getTableName();
        this.name = col.getName();
        this.type = type;
        // valuesInt = new ArrayList<>();
        HashSet<Integer> uniques = new HashSet<>();

//        for (int i = 0; i < col.getValueIntSize(); i++) {
//            valuesInt.add(col.getValueInt(i));
//            uniques.add(col.getValueInt(i));
//        }
//        pliLight = new PLILight(col.getPli());
//        this.uniqueConstantNumber = uniques.size();

        this.pliLightListPartition = new HashMap<>();
        this.valuesIntPartitions = new HashMap<>();
        this.tidsIntervals = new HashMap<>();
        String key =col.getTableName() + PredicateConfig.TBL_COL_DELIMITER + col.getName();
        // for (int pid = 0; pid < col.getPliSections().size(); pid++) {
        for (Integer pid : usedPIDs) {
            PLILight pliLight =
                    new PLILight(col.getPliSections().get(pid).getPLI(key));
            this.pliLightListPartition.put(pid, pliLight);
            int begin = col.getPliSections().get(pid).getTidStart();
            int end = col.getPliSections().get(pid).getTidEnd();
            TIntArrayList partitionValueInt = new TIntArrayList(end-begin);
            for (int t = begin; t < end; t++) {
                partitionValueInt.add(col.getValueInt(t));
                uniques.add(col.getValueInt(t));
            }
            partitionValueInt.trimToSize();
            this.valuesIntPartitions.put(pid, partitionValueInt);
            this.tidsIntervals.put(pid, new ImmutablePair<>(begin, end));
        }
        // clear value int data for col
        col.cleanValueIntBeforeBroadCast();
        this.uniqueConstantNumber = uniques.size();

    }

    public Integer getValueInt(int pid, int line) {
        return valuesIntPartitions.get(pid).get(line);
    }

    public TIntArrayList getValueIntList(int pid) {
        return valuesIntPartitions.get(pid);
    }

    public String getTableName() {
        return tableName;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        // return tableName + "." + name;
        return name;
    }

    public String toStringWithTbl(){
        return tableName + "." + name;
    }

    public void setPliLight(int pid, PLILight pli) {
        if (this.pliLightListPartition == null) {
            this.pliLightListPartition = new HashMap<>();
        }
        this.pliLightListPartition.put(pid, pli);

    }

    public void setValuesInt(int pid, TIntArrayList valuesInt) {
        if (this.valuesIntPartitions == null) {
            this.valuesIntPartitions = new HashMap<>();
        }
        this.valuesIntPartitions.put(pid, valuesInt);
    }

    public PLILight getPliLight(int pid) {
        return pliLightListPartition.get(pid);
    }

    public ImmutablePair<Integer, Integer> getTidsInterval(int pid) {
        return this.tidsIntervals.get(pid);
    }

    public void setTidsInterval(int pid, ImmutablePair<Integer, Integer> interval) {
        if (this.tidsIntervals == null) {
            this.tidsIntervals = new HashMap<>();
        }
        this.tidsIntervals.put(pid, interval);
    }

    public void clearData() {
        valuesIntPartitions = new HashMap<>();
    }


    public int getValueIntLength(int pid) {
        return valuesIntPartitions.get(pid).size();
    }
}
