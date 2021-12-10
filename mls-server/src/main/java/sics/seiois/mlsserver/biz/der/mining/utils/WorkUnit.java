package sics.seiois.mlsserver.biz.der.mining.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import gnu.trove.list.array.TIntArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class WorkUnit implements KryoSerializable {
    private PredicateSet currrent;
    private List<Integer> currrentIndexs;
    private PredicateSet rhss;
    private List<Integer> rhsIndexs;
    private long allCount;

    // store required data, key: use Function toStringData()
    private HashMap<String, HashMap<Integer, TIntArrayList>> columnsValueInt;
    private HashMap<String, HashMap<Integer, PLILight>> columnsPLI;
    private HashMap<String, HashMap<Integer, ImmutablePair<Integer, Integer>>> tidsIntervals;
    // for ML
//    transient
    private HashMap<String, ArrayList<ImmutablePair<Integer, Integer>>> predicatesMLData;

    private long minPredicateSupportT0T1;

    // private long tidT0Start;
    // private long tidT0End;
    // tids for each parititions
    private int[] pids;

    public void resetRHSs(PredicateSet rhss) {
        this.rhss = rhss;
    }

    // data transfer from column to work unit, and then ship from work unit to columnLight
    private void setPredicatesMLData(Predicate ml) {
        if (! ml.isML()) {
            return;
        }
        String key = ml.getOperand1().getColumnLight().toStringData() + "-" + ml.getOperand2().getColumnLight().toStringData();
        if (!predicatesMLData.containsKey(key)) {
            predicatesMLData.put(key, ml.getMlOnePredicate());
        }
        // add interval
        key = ml.getOperand1().getColumnLight().toStringData();
        if (! this.tidsIntervals.containsKey(key)) {
            this.tidsIntervals.put(key, ml.getOperand1().getColumnLight().getTidsIntervals());
        }
        key = ml.getOperand2().getColumnLight().toStringData();
        if (! this.tidsIntervals.containsKey(key)) {
            this.tidsIntervals.put(key, ml.getOperand2().getColumnLight().getTidsIntervals());
        }

    }

    private void retrievePRedicatesMLData(Predicate ml) {
        String key = ml.getOperand1().getColumnLight().toStringData() + "-" + ml.getOperand2().getColumnLight().toStringData();
        if (predicatesMLData.containsKey(key)) {
            ml.setMLList(predicatesMLData.get(key));
        }

        int pid = this.pids[ml.getIndex1()];
        String k = ml.getOperand1().getColumnLight().toStringData();
        ml.getOperand1().getColumnLight().setTidsInterval(pid, this.tidsIntervals.get(k).get(pid));
        pid = this.pids[ml.getIndex2()];
        k = ml.getOperand2().getColumnLight().toStringData();
        ml.getOperand2().getColumnLight().setTidsInterval(pid, this.tidsIntervals.get(k).get(pid));
    }

    public HashMap<String, ArrayList<ImmutablePair<Integer, Integer>>> getPredicatesMLData() {
        return this.predicatesMLData;
    }

    public WorkUnit() {
        this.currrent = new PredicateSet();
        this.rhss = new PredicateSet();
        this.currrentIndexs = new ArrayList<>();
        this.rhsIndexs = new ArrayList<>();
        this.columnsValueInt = new HashMap<>();
        this.columnsPLI = new HashMap<>();
        this.tidsIntervals = new HashMap<>();
        this.predicatesMLData = new HashMap<>();
        this.pids = null;
    }

    public WorkUnit(WorkUnit task, int[] pidCombination) {
        this.currrent = task.getCurrrent();
        this.currrentIndexs = task.getCurrentIndexs();
        this.rhss = task.getRHSs();
        this.rhsIndexs = task.getRHSIndexs();
        this.allCount = task.getAllCount();
        this.columnsValueInt = new HashMap<>();
        this.columnsPLI = new HashMap<>();
        this.tidsIntervals = task.getTidsIntervals();
        for (Predicate p : this.currrent) {
            if (p.isML()) {
                continue;
            }
            // valueint
            this.addColumnsValueInt(task.getColumnsValueInt(), p, pidCombination);
            // PLILight
            this.addColumnsPLI(task.getColumnsPLI(), p, pidCombination);
        }
        for (Predicate rhs : this.rhss) {
            if (rhs.isML()) {
                continue;
            }
            // only consider valueint
            this.addColumnsValueInt(task.getColumnsValueInt(), rhs, pidCombination);
        }

        this.predicatesMLData = task.getPredicatesMLData();
        this.minPredicateSupportT0T1 = task.getMinPredicateSupportT0T1();
        this.pids = pidCombination;
    }

    // copy all member data of task into the new one
    public WorkUnit(WorkUnit task) {
        this.currrent = task.getCurrrent();
        this.rhss = task.getRHSs();
        this.currrentIndexs = task.getCurrentIndexs();
        this.rhsIndexs = task.getRHSIndexs();
        this.allCount = task.getAllCount();
        this.columnsValueInt = task.getColumnsValueInt();
        this.columnsPLI = task.getColumnsPLI();
        this.tidsIntervals = task.getTidsIntervals();
        this.predicatesMLData = task.getPredicatesMLData();
        this.minPredicateSupportT0T1 = task.getMinPredicateSupportT0T1();
        this.pids = task.getPids();
    }

    private void addTidsInterval(HashMap<String, HashMap<Integer, ImmutablePair<Integer, Integer>>> tidsIntervals, Predicate p, int[] pidCombination) {

    }

    private void addColumnsPLI(HashMap<String, HashMap<Integer, PLILight>> columnsPLI, Predicate p, int[] pidCombination) {
        String key = p.getOperand1().getColumnLight().toStringData();
        if (! this.columnsPLI.containsKey(key)) {
            HashMap<Integer, PLILight> temp = new HashMap<>();
            int pid = pidCombination[p.getIndex1()];
            temp.put(pid, columnsPLI.get(key).get(pid));
            this.columnsPLI.put(key, temp);
        } else {
            int pid = pidCombination[p.getIndex1()];
            if (! this.columnsPLI.containsKey(p.getIndex1())) {
                this.columnsPLI.get(key).put(pid, columnsPLI.get(key).get(pid));
            }
        }
        if (p.getIndex1() == p.getIndex2()) return;

        key = p.getOperand2().getColumnLight().toStringData();
        if (! this.columnsPLI.containsKey(key)) {
            HashMap<Integer, PLILight> temp = new HashMap<>();
            int pid = pidCombination[p.getIndex2()];
            temp.put(pid, columnsPLI.get(key).get(pid));
            this.columnsPLI.put(key, temp);
        } else {
            int pid = pidCombination[p.getIndex2()];
            if (! this.columnsPLI.containsKey(p.getIndex2())) {
                this.columnsPLI.get(key).put(pid, columnsPLI.get(key).get(pid));
            }
        }
    }

    private void addColumnsValueInt(HashMap<String, HashMap<Integer, TIntArrayList>> columnsVI, Predicate p, int[] pidCombination) {
        String key = p.getOperand1().getColumnLight().toStringData();
        if (! this.columnsValueInt.containsKey(key)) {
            HashMap<Integer, TIntArrayList> temp = new HashMap<>();
            int pid = pidCombination[p.getIndex1()];
            if(columnsVI.get(key) != null) {
                temp.put(pid, columnsVI.get(key).get(pid));
                this.columnsValueInt.put(key, temp);
            }
        } else {
            int pid = pidCombination[p.getIndex1()];
            if (! this.columnsValueInt.get(key).containsKey(pid)) {
                this.columnsValueInt.get(key).put(pid, columnsVI.get(key).get(pid));
            }
        }
        if (p.getIndex1() == p.getIndex2()) return;

        key = p.getOperand2().getColumnLight().toStringData();
        if (! this.columnsValueInt.containsKey(key)) {
            HashMap<Integer, TIntArrayList> temp = new HashMap<>();
            int pid = pidCombination[p.getIndex2()];
            if(columnsVI.get(key) != null) {
                temp.put(pid, columnsVI.get(key).get(pid));
                this.columnsValueInt.put(key, temp);
            }
        } else {
            int pid = pidCombination[p.getIndex2()];
            if (! this.columnsValueInt.get(key).containsKey(pid)) {
                this.columnsValueInt.get(key).put(pid, columnsVI.get(key).get(pid));
            }
        }
    }

    public HashMap<String, HashMap<Integer, ImmutablePair<Integer, Integer>>> getTidsIntervals() {
        return this.tidsIntervals;
    }

    public HashMap<String, HashMap<Integer, TIntArrayList>> getColumnsValueInt() {
        return this.columnsValueInt;
    }

    public HashMap<String, HashMap<Integer, PLILight>> getColumnsPLI() {
        return this.columnsPLI;
    }

    public int[] getPids() {
        return this.pids;
    }

    public String getPidString() {
        StringBuffer str = new StringBuffer();
        for(int i : pids) {
            str.append(i).append(":");
        }
        return str.toString();
    }

    public void setMinPredicateSupportT0T1(long minPredicateSupportT0T1) {
        this.minPredicateSupportT0T1 = minPredicateSupportT0T1;
    }

    public long getMinPredicateSupportT0T1() {
        return this.minPredicateSupportT0T1;
    }


    public void setMinPredicateSupportT0T1(HashMap<String, Long> numOfTuplesRelations) {
        this.minPredicateSupportT0T1 = 0L;
        boolean first = true;
        for (Predicate p : this.currrent) {
            if (p.isConstant()) {
                continue;
            }
            if (p.getIndex1() == 0 && p.getIndex2() == 1) {
                if (! first) {
                    this.minPredicateSupportT0T1 = Math.min(this.minPredicateSupportT0T1, p.getSupport());
                } else {
                    this.minPredicateSupportT0T1 = p.getSupport();
                    first = false;
                }
            }
        }
    }

    public void setAllCount(long allCount) {
        this.allCount = allCount;
    }

    public long getAllCount() {
        return this.allCount;
    }


    // tids: t0.col, t1.col => [t0, t1]
    public void setTransferData(ParsedColumnLight<?> columnLight, Integer tid, boolean ifRHS) {
        String key = columnLight.toStringData();
//        log.info(">>>add key {} into column int : {}", key, columnLight.getValuesIntPartitions());
        if (! this.columnsValueInt.containsKey(key)) {
            this.columnsValueInt.put(key, columnLight.getValuesIntPartitions());
        }

        if (! ifRHS) {
            if (!this.columnsPLI.containsKey(key)) {
                this.columnsPLI.put(key, columnLight.getPliLightListPartition());
            }
        }

        if (! this.tidsIntervals.containsKey(key)) {
            this.tidsIntervals.put(key, columnLight.getTidsIntervals());
        }

//        String key = columnLight.toStringData();
//        int pid = this.pids[tid];
//        // set value int
//        List<Integer> vi = columnLight.getValuesInt(pid);
//        if (! this.columnsValueInt.containsKey(key)) {
//            HashMap<Integer, List<Integer>> temp = new HashMap<>();
//            temp.put(pid, vi);
//            this.columnsValueInt.put(key, temp);
//        } else {
//            if (! this.columnsValueInt.get(key).containsKey(pid)) {
//                this.columnsValueInt.get(key).put(pid, vi);
//            }
//        }
//        // set tids intervals
//        ImmutablePair<Integer, Integer> interval = columnLight.getTidsInterval(pid);
//        if (! this.tidsIntervals.containsKey(key)) {
//            HashMap<Integer, ImmutablePair<Integer, Integer>> temp = new HashMap<>();
//            temp.put(pid, interval);
//            this.tidsIntervals.put(key, temp);
//        } else {
//            if (! this.tidsIntervals.get(key).containsKey(pid)) {
//                this.tidsIntervals.get(key).put(pid, interval);
//            }
//        }
//
//        if (! ifRHS) {
//            // set PLILight
//            PLILight pliLight = columnLight.getPliLight(pid);
//            if (! this.columnsPLI.containsKey(key)) {
//                HashMap<Integer, PLILight> temp = new HashMap<>();
//                temp.put(pid, pliLight);
//                this.columnsPLI.put(key, temp);
//            } else {
//                if (! this.columnsPLI.get(key).containsKey(pid)) {
//                    this.columnsPLI.get(key).put(pid, pliLight);
//                }
//            }
//        }
    }

    public void setPredicateTransferData(Predicate p, boolean ifRHS) {
        ParsedColumnLight<?> column1 = p.getOperand1().getColumnLight();
        this.setTransferData(column1, p.getIndex1(), ifRHS);
        ParsedColumnLight<?> column2 = p.getOperand2().getColumnLight();
        this.setTransferData(column2, p.getIndex2(), ifRHS);
    }

    public void setTransferData(List<Predicate> allPredicate) {
        for (int pIndex : this.currrentIndexs) {
            Predicate p = allPredicate.get(pIndex);
//            p.initialPredicateDataTransfer();
            log.info(">>>>set transfer data:{} | {}", pIndex, p.toInnerString());
            currrent.add(p);
            if (p.isML()) {
                setPredicatesMLData(p);
            } else {
                this.setPredicateTransferData(p, false);
            }
        }
        for (int pIndex : this.rhsIndexs) {
            Predicate p = allPredicate.get(pIndex);
//            p.initialPredicateDataTransfer();
            log.info(">>>>set transfer data:{} | {}", pIndex, p.toInnerString());
            rhss.add(p);
            this.setPredicateTransferData(p, true);
        }
        log.info(">>>>columns key:{}", columnsValueInt.keySet());
//        for (Predicate p : this.currrent) {
//            if (p.isML()) {
//                continue;
//            }
//
//            log.info(">>>>add columns:{} | {}", p.getOperand1().getColumnLight().toStringData(), p.getOperand2().getColumnLight().toStringData() );
//            // valueint
//            this.addColumnsValueInt(this.columnsValueInt, p, pids);
//            // PLILight
//            this.addColumnsPLI(this.columnsPLI, p, pids);
//        }
//        for (Predicate rhs : this.rhss) {
//            if (rhs.isML()) {
//                continue;
//            }
//            // only consider valueint
//            this.addColumnsValueInt(this.columnsValueInt, rhs, pids);
//        }
    }

    public void initIndex(List<Predicate> allPredicate, boolean isCleanPredicate) {
        int index = 0;
        for(Predicate p : allPredicate) {
            if(this.currrent.containsPredicate(p)) {
                this.currrentIndexs.add(index);
            }
            if(this.rhss.containsPredicate(p)) {
                this.rhsIndexs.add(index);
            }

            index++;
        }
        if (isCleanPredicate) {
            this.rhss = new PredicateSet();
            this.currrent = new PredicateSet();
        }
        columnsValueInt = new HashMap<>() ;
        columnsPLI = new HashMap<>() ;
        tidsIntervals = new HashMap<>() ;
        // for ML
        predicatesMLData = new HashMap<>() ;
    }

    public void clearData() {
//        for(Predicate p : this.currrent) {
//            p.getOperand1().clearData();
//            p.getOperand2().clearData();
//        }
//
//        for(Predicate p : this.rhss) {
//            p.getOperand1().clearData();
//            p.getOperand2().clearData();
//        }

        columnsValueInt = new HashMap<>() ;
        columnsPLI = new HashMap<>() ;
        tidsIntervals = new HashMap<>() ;
        // for ML
        predicatesMLData = new HashMap<>() ;
    }


    public void setTransferData() {
        for (Predicate p : this.currrent) {
            if (p.isML()) {
                setPredicatesMLData(p);
            } else {
                this.setPredicateTransferData(p, false);
            }
        }
        for (Predicate p : this.rhss) {
            this.setPredicateTransferData(p, true);
        }
    }

    public void retrievePredicateTransferData(Predicate p, boolean ifRHS) {
        if (p.isML() && ifRHS == false) {
            this.retrievePRedicatesMLData(p);
            return;
        }
        // set data for columns in p;
        String key1 = p.getOperand1().getColumnLight().toStringData();
        int pid = this.pids[p.getIndex1()];
        if (!ifRHS) {
            p.getOperand1().getColumnLight().setPliLight(pid, this.columnsPLI.get(key1).get(pid));
        }
        p.getOperand1().getColumnLight().setValuesInt(pid, this.columnsValueInt.get(key1).get(pid));
        p.getOperand1().getColumnLight().setTidsInterval(pid, this.tidsIntervals.get(key1).get(pid));

        pid = this.pids[p.getIndex2()];
        String key2 = p.getOperand2().getColumnLight().toStringData();
        if (!ifRHS) {
            p.getOperand2().getColumnLight().setPliLight(pid, this.columnsPLI.get(key2).get(pid));
        }
        p.getOperand2().getColumnLight().setValuesInt(pid, this.columnsValueInt.get(key2).get(pid));
        p.getOperand2().getColumnLight().setTidsInterval(pid, this.tidsIntervals.get(key2).get(pid));
    }

    public void retrieveTransferData() {
//        log.info("### Work unit current : {}", this.currrent);
//        log.info("### Work unit rhss : {}", this.rhss);
//        log.info("### Work unit valueInt : {}", this.columnsValueInt);
//        log.info("### Work unit PLI : {}", this.columnsPLI);

        for (Predicate p : this.currrent) {
            this.retrievePredicateTransferData(p, false);
        }
        for (Predicate p :this.rhss) {
            this.retrievePredicateTransferData(p, true);
        }
    }

    public String toString() {
        return "[ " + this.currrent.toString() + " ] -> { " + this.rhss.toString() + " }";
    }

    public void addCurrent(Predicate p) {
        this.currrent.add(p);
    }

    public void addCurrentIndex(Integer p) {
        this.currrentIndexs.add(p);
    }

    public void addRHS(Predicate p) {
        this.rhss.add(p);
    }

    public void addRHSIndex(Integer p) {
        this.rhsIndexs.add(p);
    }

    public PredicateSet getRHSs() {
        return this.rhss;
    }

    public List<Integer> getRHSIndexs() {
        return this.rhsIndexs;
    }

    public PredicateSet getCurrrent() {
        return this.currrent;
    }

    public List<Integer> getCurrentIndexs() {
        return this.currrentIndexs;
    }

    public void unionRHSs(PredicateSet rhss_new) {
//        for (Predicate p : rhss_new) {
//            this.rhss.add(p);
//        }
        this.rhss.union(rhss_new.getBitset());
    }

    public IBitSet getKey() {
        return currrent.getBitset();
    }

//    public void dataTransfer() {
//        currrent.prepareDataTransfer();
//        rhss.prepareDataTransfer();
//    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, currrent);
        kryo.writeObject(output, rhss);
        kryo.writeObject(output, currrentIndexs);
        kryo.writeObject(output, rhsIndexs);
        kryo.writeObject(output, columnsPLI);
        kryo.writeObject(output, columnsValueInt);
        kryo.writeObject(output, tidsIntervals);
        kryo.writeObject(output, allCount);
        kryo.writeObject(output, minPredicateSupportT0T1);
        kryo.writeObject(output, predicatesMLData);
        kryo.writeObject(output, pids);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.currrent = kryo.readObject(input, PredicateSet.class);
        this.rhss = kryo.readObject(input, PredicateSet.class);
        this.currrentIndexs = kryo.readObject(input, ArrayList.class);
        this.rhsIndexs = kryo.readObject(input, ArrayList.class);
        this.columnsPLI = kryo.readObject(input, HashMap.class);
        this.columnsValueInt = kryo.readObject(input, HashMap.class);
        this.tidsIntervals = kryo.readObject(input, HashMap.class);
        this.allCount = kryo.readObject(input, Long.class);
        this.minPredicateSupportT0T1 = kryo.readObject(input, Long.class);
        this.predicatesMLData = kryo.readObject(input, HashMap.class);
        this.pids = kryo.readObject(input, int[].class);
    }

    private static Logger log = LoggerFactory.getLogger(WorkUnit.class);
}
