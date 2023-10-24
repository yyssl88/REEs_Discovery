package sics.seiois.mlsserver.biz.der.mining.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;

import java.util.*;

@Setter
@Getter
public class WorkUnits implements KryoSerializable {
    private PredicateSet currrent;
    private PredicateSet rhss;
    private Long allCount;
    private PredicateSet sameSet;
    private HashSet<String> sameXConstSet;
    private List<WorkUnit> units;
    private LinkedHashMap<String, List<Integer>> predicateMap;
    private int[] pids;

    public WorkUnits() {
        this.currrent = new PredicateSet();
        this.rhss = new PredicateSet();
        this.sameSet = new PredicateSet();
        this.allCount = 0l;
        this.units = new ArrayList<>();
        this.predicateMap = new LinkedHashMap<>();
        this.pids = null;
        this.sameXConstSet = new HashSet<>();
    }


    public void addUnit(WorkUnit unit) {
        this.units.add(unit);
        int index = units.size() - 1;
        for (Predicate p : unit.getCurrrent()) {
            //p为常数谓词时，增加一个类来统计t0.A = _的总数
            if (p.isConstant()) {
//                String key = p.getOperand1().toString_(0);
                String key = p.getOperand1().toString_(p.getIndex1());
                predicateMap.putIfAbsent(key, new ArrayList<>());
                predicateMap.get(key).add(index);
            } else {
                predicateMap.putIfAbsent(p.toString(), new ArrayList<>());
                predicateMap.get(p.toString()).add(index);
            }
            this.currrent.add(p);
        }
        for (Predicate p : this.currrent) {
            if (p.isConstant()) {
                //p为常数谓词时, t0.A = _都认为是相同进行聚合
//                String key = p.getOperand1().toString_(0);
                String key = p.getOperand1().toString_(p.getIndex1());
                if (predicateMap.get(key).size() == units.size()) {
                    sameXConstSet.add(key);
                } else if (sameXConstSet.contains(key)) {
                    sameXConstSet.remove(key);
                }
            } else {
                String key = p.toString();
                if (predicateMap.get(key).size() == units.size()) {
                    sameSet.add(p);
                } else {
                    if (sameSet.containsPredicate(p)) {
                        sameSet.remove(p);
                    }
                }
            }
        }

        this.pids = unit.getPids();
        this.rhss.addAll(unit.getRHSs());
    }

    public int calDistance(WorkUnit unit) {
        if (!this.getPids().equals(unit.getPids())) {
            return 0;
        }

//        if (this.units.size() > 90) {
//            return 0;
//        }

        int count = 0;
        for(Predicate p : unit.getCurrrent()) {
            if (p.isConstant()) {
                String key = p.getOperand1().toString_(0);
                if(sameXConstSet.contains(key)) {
                    count++;
                }
            } else {
                if (this.sameSet.containsPredicate(p)) {
                    count = count + 2; //非常数谓词认为权重更高,优先聚合
                }
            }
        }

        return count;
    }

    public Map<String, List<Integer>> getPredicateMap() {
        return this.predicateMap;
    }

    public PredicateSet getSameSet() {
        return this.sameSet;
    }

    public void setAllCount(long allCount) {
        this.allCount = allCount;
    }

    public long getAllCount() {
        return this.allCount;
    }

    public List<WorkUnit> getUnits() {
        return this.units;
    }

    public String toString() {
        return "[ " + this.currrent.toString() + " ] -> { " + this.rhss.toString() + " }"
                + " | { " + sameSet.toString() + "} , pid: " + pids.length + " predicateMap ： "
                + predicateMap.keySet().size() + " unit: " + units.size();
    }

    public void addSameSet(Predicate p) {
        this.sameSet.add(p);
    }

    public void addRHS(Predicate p) {
        this.rhss.add(p);
    }

    public PredicateSet getRHSs() {
        return this.rhss;
    }

    public PredicateSet getCurrrent() {
        return this.currrent;
    }

    public IBitSet getKey() {
        return currrent.getBitset();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, currrent);
        kryo.writeObject(output, rhss);
        kryo.writeObject(output, allCount);
        kryo.writeObject(output, sameSet);
        kryo.writeObject(output, units);
        kryo.writeObject(output, predicateMap);
        kryo.writeObject(output, pids);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.currrent = kryo.readObject(input, PredicateSet.class);
        this.rhss = kryo.readObject(input, PredicateSet.class);
        this.allCount = kryo.readObject(input, Long.class);
        this.sameSet = kryo.readObject(input, PredicateSet.class);
        this.units = kryo.readObject(input, ArrayList.class);
        this.predicateMap = kryo.readObject(input, LinkedHashMap.class);
        this.pids = kryo.readObject(input, int[].class);
    }

    private static Logger log = LoggerFactory.getLogger(WorkUnits.class);
}
