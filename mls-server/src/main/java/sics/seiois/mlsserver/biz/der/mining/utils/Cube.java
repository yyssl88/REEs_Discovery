package sics.seiois.mlsserver.biz.der.mining.utils;

/*
    Now only consider REEs that only contain t_0.A = t_1.A predicates
 */

import java.util.HashMap;
import java.util.Map;

public class Cube {
//    private Map<Integer, Map<Integer, Integer>> tupleIDs;
    private long[] tupleIDs_t0;
    private long[] tupleIDs_t1;

    public Cube(int size) {
//        this.tupleIDs = new HashMap<>();
        this.tupleIDs_t0 = new long[size];
        this.tupleIDs_t1 = new long[size];
        for (int index = 0; index < size; index++) {
            this.tupleIDs_t0[index] = 0;
            this.tupleIDs_t1[index] = 0;
        }
    }

//    public void addTupleID(int rhsId, int index) {
//        Map<Integer, Integer> indexTupleIDs = tupleIDs.get(index);
//        Integer t = indexTupleIDs.getOrDefault(rhsId, 0);
//        indexTupleIDs.put(rhsId, t + 1);
//    }
//
//    public long getTupleNum(int rhsId, int index) {
//        Map<Integer, Integer> indexTupleIDs = this.tupleIDs.get(index);
//        return indexTupleIDs.getOrDefault(rhsId, 0);
//    }

    public void addTupleIDT0(int rhsId) {
//        Integer t = tupleIDs_t0.getOrDefault(rhsId, 0);
//        tupleIDs_t0.put(rhsId, t + 1);
        tupleIDs_t0[rhsId] = tupleIDs_t0[rhsId] + 1;
    }

    public void addTupleIDT1(int rhsId) {
//        Integer t = tupleIDs_t1.getOrDefault(rhsId, 0);
//        tupleIDs_t1.put(rhsId, t + 1);
        tupleIDs_t1[rhsId] = tupleIDs_t1[rhsId] + 1;
    }

    public long getTupleNumT0(int rhsId) {
//        return this.tupleIDs_t0.getOrDefault(rhsId, 0);
        return tupleIDs_t0[rhsId];
    }

    public long getTupleNumT1(int rhsId) {
//        return this.tupleIDs_t1.getOrDefault(rhsId, 0);
        return tupleIDs_t1[rhsId];
    }

}