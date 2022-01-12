package sics.seiois.mlsserver.biz.der.mining.sample;

import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;

import java.util.HashSet;
import java.util.List;

public class RandomWalk {
    private HashSet<Integer> selectedTids;
    private long sampleNum;

    /*
        predicates: only consider equality and ML predicates
     */
    public RandomWalk(List<Predicate> predicates, double sampleRatio, long dataNum, Input input) {
        this.selectedTids = new HashSet<>();
        this.sampleNum = (long)(sampleRatio * dataNum);

    }
}
