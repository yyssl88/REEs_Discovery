package sics.seiois.mlsserver.biz.der.metanome.input;
import com.google.common.collect.HashMultiset;
import de.metanome.algorithm_integration.ColumnIdentifier;

import java.io.Serializable;
import java.util.*;

import gnu.trove.list.array.TIntArrayList;
import sics.seiois.mlsserver.biz.der.metanome.helpers.IndexProvider;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLI;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLISection;


public class ParsedColumn<T extends Comparable<T>> implements Serializable {

    private static final long serialVersionUID = -5632843229256239029L;

    private final String tableName;
    private final String name;
    transient private final HashMultiset<T> valueSet = HashMultiset.create();
    transient private final List<T> values = new ArrayList<>();
    private final Class<T> type;
    private final int index;

    transient private Map<Integer, Integer> tupleValues = new HashMap<>();//tid2value
    transient private TIntArrayList valuesInt = new TIntArrayList();
    transient private PLI pli;

    transient private List<PLISection> pliSections;

    public void setPliSections(List<PLISection> pliSections) {
        this.pliSections = pliSections;
    }

    public List<PLISection> getPliSections() {
        return this.pliSections;
    }

    public String toStringData() {
        return this.tableName + "." + this.name;
    }

    // set encoded integer values of data
    public void setValuesInt(IndexProvider<String> providerS, IndexProvider<Double> providerD, IndexProvider<Long> providerL) {
        if (this.type == String.class) {
            for (int i = 0; i < values.size(); i++) {
                this.valuesInt.add(providerS.getIndex((String)values.get(i)));
            }
        } else if (this.type == Double.class) {
            for (int i = 0; i < values.size(); i++) {
                this.valuesInt.add(providerD.getIndex((Double)values.get(i)));
            }
        } else if (this.type == Long.class) {

            for (int i = 0; i < values.size(); i++) {
                this.valuesInt.add(providerL.getIndex((Long)values.get(i)));
            }
        }
    }

    public void addTuple(Integer tid, Integer value) {
        tupleValues.put(tid, value);
    }

    public Integer getTupleValue(int tid) {
        if (tupleValues.containsKey(tid)) {
            return tupleValues.get(tid);
        }
        return null;
    }

    public void setTupleValues(Map<Integer, Integer> tupleValues) {
        this.tupleValues = tupleValues;
    }

    public Map<Integer, Integer> getTupleValues() {
        return tupleValues;
    }

    public void replaceTupleValue(Integer tid_old, Collection<Integer> tids_new) {
        if (!tupleValues.containsKey(tid_old)) return;
        Integer value = tupleValues.get(tid_old);
        tupleValues.remove(tid_old);
        for (Integer tt : tids_new) {
            tupleValues.put(tt, value);
        }
    }

    public ParsedColumn(String tableName, String name, Class<T> type, int index) {
        this.tableName = tableName;
        this.name = name;
        this.type = type;
        this.index = index;
    }

    public void cleanBeforeBoradCast() {
        valueSet.clear();
        values.clear();
    }

    public void cleanValueIntBeforeBroadCast() {
        valuesInt.clear();
    }

    public void addLine(T value) {
        valueSet.add(value);
        values.add(value);
    }

    public T getValue(int line) {
        return values.get(line);
    }

    public Integer getValueInt(int line) {
        return valuesInt.get(line);
    }

    public String getTableName() {
        return tableName;
    }

    public String getName() {
        return name;
    }

    public ColumnIdentifier getColumnIdentifier() {
        return new ColumnIdentifier(tableName, name);
    }

    public int getIndex() {
        return index;
    }

    public Class<T> getType() {
        return type;
    }

    @Override
    public String toString() {
        // return tableName + "." + name;
        return name;
    }

    public String toStringWithTbl(){
        return tableName + "." + name;
    }

    public boolean isComparableType() {
        return getType().equals(Double.class) || getType().equals(Long.class);
    }

    public double getAverage() {
        double avg = 0.0d;
        int size = values.size();
        if (type.equals(Double.class)) {
            for (int i = 0; i < size; i++) {
                Double l = (Double) values.get(i);
                double tmp = l.doubleValue() / size;
                avg += tmp;
            }
        } else if (type.equals(Long.class)) {
            for (int i = 0; i < size; i++) {
                Long l = (Long) values.get(i);
                double tmp = l.doubleValue() / size;
                avg += tmp;
            }
        }

        return avg;
    }

    // calculate the sigma
    public double getSigma(double average) {
        double sigma = 0.0d;
        int size = values.size();
        if (type.equals(Double.class)) {
            for (int i = 0; i < size; i++) {
                Double l = (Double) values.get(i);
                double tmp = (l.doubleValue() - average) * (l.doubleValue() - average);
                sigma += tmp;
            }
        } else if (type.equals(Long.class)) {
            for (int i = 0; i < size; i++) {
                Long l = (Long) values.get(i);
                double tmp = (l.doubleValue() - average) * (l.doubleValue() - average);
                sigma += tmp;
            }
        }
        return Math.sqrt(sigma / size);
    }

    public Collection<String> getConstantValues(int countThreshold) {
        ArrayList<String> res = new ArrayList<>();
        for (T s : valueSet.elementSet()) {
            int count = valueSet.count(s);
            if (count >= countThreshold) {
                res.add(s.toString());
            }
        }
        return res;
    }

    public double getSharedPercentage(ParsedColumn<?> c2) {
        int totalCount = 0;
        int sharedCount = 0;
        for (T s : valueSet.elementSet()) {
            int thisCount = valueSet.count(s);
            int otherCount = c2.valueSet.count(s);
            sharedCount += Math.min(thisCount, otherCount);
            totalCount += Math.max(thisCount, otherCount);
        }
        return totalCount == 0 ? 0 : ((double) sharedCount) / ((double) totalCount);
    }

    public long getSharedCardinality(ParsedColumn<?> c2) {
        long card = 0;
        for (T s : valueSet.elementSet()) {
            int thisCount = valueSet.count(s);
            int otherCount = valueSet.count(s);
            card += (long)thisCount * (long)otherCount;
        }
        if (this.equals(c2)) {
            card = (long)(card / 2);
        }
        return card;
    }

    public int Cardinality() {
        return valueSet.elementSet().size();
    }

    public int getValueSize() {
        return this.values.size();
    }

    public int getValueIntSize() {
        return this.valuesInt.size();
    }

    public boolean checkHighCard() {
        return valueSet.size() <= 5;
    }

    public void setPLI(PLI pli) {
        this.pli = pli;

    }

    public PLI getPli() {
        return pli;
    }

    public void setPli(PLI pli) {
        this.pli = pli;
    }

    public void clearData() {
        valuesInt = new TIntArrayList();
        tupleValues = new HashMap<>();
        pliSections = new ArrayList<>();
    }

    public List<T> getValues() {
        return values;
    }

    public TIntArrayList getValuesInt() {
        return valuesInt;
    }
}
