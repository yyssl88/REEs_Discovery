package sics.seiois.mlsserver.biz.der.mining.utils;

import com.jcraft.jsch.HASH;
import gnu.trove.list.array.TIntArrayList;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;

import java.util.*;

/*
    Now only consider REEs that only contain t_0.A = t_1.A predicates
 */


public class HyperCube {

    private HashMap<TIntArrayList, HashMap<Integer, Cube>> cubeInstances;

    // Ys
    private ArrayList<Predicate> rhss;

    // the support of <t0, t1>
    private long supportX;
    // the support of t0
    private long supportXCP0;
    // the support of t1
    private long supportXCP1;

    // length = this.rhss.size()
    private Long[] suppRHSs;

    private Map<TIntArrayList, Integer> lhsSupportMap0;
    private Map<TIntArrayList, Integer> lhsSupportMap1;

//    private Map<Integer, Map<TIntArrayList, Integer>> lhsSupportMap;


    public HyperCube(ArrayList<Predicate> rhss_arr) {
        this.cubeInstances = new HashMap<>();
        lhsSupportMap0 = new HashMap<>();
        lhsSupportMap1 = new HashMap<>();
        /*
        this.rhss = new HashMap<>();
        for (Predicate rhs : rhss_arr) {
            if (! this.rhss.containsKey(rhs.toString())) {
                this.rhss.put(rhs.toString(), rhs);
            }
        }
         */
        this.rhss = rhss_arr;
        this.supportX = 0;
        this.supportXCP0 = 0;
        this.supportXCP1 = 0;
        this.suppRHSs = new Long[rhss_arr.size()];
        for (int i = 0; i < this.suppRHSs.length; i++) {
            this.suppRHSs[i] = 0L;
        }
    }

//    public void addxValue(TIntArrayList xValue, Integer index){
//        Map<TIntArrayList, Integer> lhsIndexSupportMap = lhsSupportMap.get(index);
//
//        Integer supp = lhsIndexSupportMap.getOrDefault(xValue, 0);
//        lhsIndexSupportMap.put(xValue,supp+1);
//    }

    public void addxValue0(TIntArrayList xValue){
        Integer supp = this.lhsSupportMap0.getOrDefault(xValue, 0);
        this.lhsSupportMap0.put(xValue,supp+1);
    }

    public void setxValue0(TIntArrayList xValue, int supp){
        this.lhsSupportMap0.put(xValue,supp);
    }

    public void setxValue1(TIntArrayList xValue, int supp){
        this.lhsSupportMap1.put(xValue,supp);
    }

    public void addxValue1(TIntArrayList xValue){
        Integer supp = this.lhsSupportMap1.getOrDefault(xValue, 0);
        this.lhsSupportMap1.put(xValue,supp+1);
    }

//    public void addTuple(TIntArrayList xValue, Integer yValue, int rhsID, int tid, int index) {
//        if (!this.cubeInstances.containsKey(xValue)) {
//            HashMap<Integer, Cube> tt = new HashMap<>();
//            Cube cube = new Cube();
//            cube.addTupleID(tid, rhsID, index);
//            tt.put(yValue, cube);
//            this.cubeInstances.put(xValue, tt);
//        } else {
//            HashMap<Integer, Cube> tt = this.cubeInstances.get(xValue);
//            if (!tt.containsKey(yValue)) {
//                Cube cube = new Cube();
//                cube.addTupleID(tid, rhsID, index);
//                tt.put(yValue, cube);
//            } else {
//                tt.get(yValue).addTupleID(tid, rhsID, index);
//            }
//        }
//    }

    public void addTupleT0(TIntArrayList xValue, Integer yValue, int rhsID, int tid) {
        if (!this.cubeInstances.containsKey(xValue)) {
            HashMap<Integer, Cube> tt = new HashMap<>();
            Cube cube = new Cube(suppRHSs.length);
            cube.addTupleIDT0(rhsID);
            tt.put(yValue, cube);
            this.cubeInstances.put(xValue, tt);
        } else {
            HashMap<Integer, Cube> tt = this.cubeInstances.get(xValue);
            if (!tt.containsKey(yValue)) {
                Cube cube = new Cube(suppRHSs.length);
                cube.addTupleIDT0(rhsID);
                tt.put(yValue, cube);
            } else {
                tt.get(yValue).addTupleIDT0(rhsID);
            }
        }
    }


    public void addTupleT1(TIntArrayList xValue, Integer yValue, int rhsID, int tid) {
        if (!this.cubeInstances.containsKey(xValue)) {
            HashMap<Integer, Cube> tt = new HashMap<>();
            Cube cube = new Cube(suppRHSs.length);
            cube.addTupleIDT1(rhsID);
            tt.put(yValue, cube);
            this.cubeInstances.put(xValue, tt);
        } else {
            HashMap<Integer, Cube> tt = this.cubeInstances.get(xValue);
            if (!tt.containsKey(yValue)) {
                Cube cube = new Cube(suppRHSs.length);
                cube.addTupleIDT1(rhsID);
                tt.put(yValue, cube);
            } else {
                tt.get(yValue).addTupleIDT1(rhsID);
            }
        }
    }

    public void getStatistic(boolean ifSame) {
        if (ifSame) {
            for (Integer value : this.lhsSupportMap0.values()) {
                long count = value;
                this.supportX += (long) (count * (count - 1));
            }
        } else {
            for (Map.Entry<TIntArrayList, Integer> entry : this.lhsSupportMap0.entrySet()) {
                if (this.lhsSupportMap1.containsKey(entry.getKey())) {
                    long count = entry.getValue();
                    long count1 = lhsSupportMap1.get(entry.getKey());
                    this.supportX += (long) (count * count1);
                }
            }
        }

        for (Map.Entry<TIntArrayList, HashMap<Integer, Cube>> entry : this.cubeInstances.entrySet()) {
            HashMap<Integer, Cube> tt = entry.getValue();
            for (Map.Entry<Integer, Cube> entry_ : tt.entrySet()) {
                Cube cube = entry_.getValue();
                if (!ifSame) {
                    for (int rhsId = 0; rhsId < this.suppRHSs.length; rhsId++) {
                        this.suppRHSs[rhsId] += (long) (cube.getTupleNumT0(rhsId) * cube.getTupleNumT1(rhsId));
                    }
                } else {
                    for (int rhsId = 0; rhsId < this.suppRHSs.length; rhsId++) {
//                        this.suppRHSs[cube.getRhsID()] += (long) (cube.getTupleNumT0() * (cube.getTupleNumT0() - 1) / 2);
                        this.suppRHSs[rhsId] += (long) (cube.getTupleNumT0(rhsId) * (cube.getTupleNumT0(rhsId) - 1));
                    }
                }
            }
        }
    }

    public void getStatistic_new(boolean ifSame) {
        HashSet<TIntArrayList> commonList = new HashSet<>();
        // update support information of LHS
        for (Map.Entry<TIntArrayList, Integer> entry : this.lhsSupportMap0.entrySet()) {
            if (!this.lhsSupportMap1.containsKey(entry.getKey())) {
                continue;
            }
            commonList.add(entry.getKey());
            long count = entry.getValue();
            long count1 = lhsSupportMap1.get(entry.getKey());
            this.supportX += (long) (count * count1);
            this.supportXCP0 += (long)(count);
            this.supportXCP1 += (long) (count1);
        }

        // update support and tuple coverage information of rules
        for (Map.Entry<TIntArrayList, HashMap<Integer, Cube>> entry : this.cubeInstances.entrySet()) {
            TIntArrayList xValue = entry.getKey();
            if (!commonList.contains(entry.getKey())) {
                continue;
            }
            HashMap<Integer, Cube> tt = entry.getValue();
            for (Map.Entry<Integer, Cube> entry_ : tt.entrySet()) {
                Cube cube = entry_.getValue();
                for (int rhsID = 0; rhsID < this.suppRHSs.length; rhsID++) {
                    Predicate p = this.rhss.get(rhsID);
                    // handle constant predicates as RHSs
                    if (p.isConstant()) {
                        // evaluate constant predicates and check whether these values equal to the constant
                        if (p.getConstantInt().intValue() == entry_.getKey().intValue()) {
                            if (p.getIndex1() == 0) {
                                this.suppRHSs[rhsID] += (long) (cube.getTupleNumT0(rhsID) * this.lhsSupportMap1.get(xValue));
                            } else if (p.getIndex1() == 1) {
                                this.suppRHSs[rhsID] += (long) (cube.getTupleNumT1(rhsID) * this.lhsSupportMap0.get(xValue));
                            }
                        }
                    }
                    // handle non-constant predicates as RHSs
                    else {
                        this.suppRHSs[rhsID] += (long) (cube.getTupleNumT0(rhsID) * cube.getTupleNumT1(rhsID));
                    }
                }
            }
        }
    }

    public long getSupportX() {
        return this.supportX;
    }

    public long getSupportXCP0() {
        return this.supportXCP0;
    }

    public long getSupportXCP1() {
        return this.supportXCP1;
    }

    public Long[] getSuppRHSs() {
        return this.suppRHSs;
    }

    public String toSize() {
        return "cubeInstances size:" + cubeInstances.size() + " | suppRHSs size:" + suppRHSs.length;
    }

}