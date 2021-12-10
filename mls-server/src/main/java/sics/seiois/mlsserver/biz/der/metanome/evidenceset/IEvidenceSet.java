package sics.seiois.mlsserver.biz.der.metanome.evidenceset;

import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import sics.seiois.mlsserver.biz.der.bitset.IBitSet;

public interface IEvidenceSet extends Iterable<PredicateSet> {

    boolean add(PredicateSet predicateSet);

    boolean add(PredicateSet create, long count);

    void add(IBitSet create, List<ImmutablePair<Integer, Integer>> list);

    void add(IBitSet create, ImmutablePair<Integer, Integer> pair);

    long getCount(PredicateSet predicateSet);

    long getAllCount();

    boolean adjustCount(PredicateSet predicateSet, long amount);

    List<ImmutablePair<Integer, Integer>> getList(IBitSet iBitSet);

    Iterator<PredicateSet> iterator();

    Set<PredicateSet> getSetOfPredicateSets();

    void generateEvidenceSet(ArrayList<HashSet<String>> predicateStrings,
                             HashMap<String, Predicate> predicateStringsMap,
                             ArrayList<Long> evidCounts);

    void remove(PredicateSet create);

    int size();

    void addFKMap(Map<IBitSet, List<ImmutablePair<Integer, Integer>>> map);
    Map<IBitSet, List<ImmutablePair<Integer, Integer>>> getFKMap();

    boolean isEmpty();

    void refine();

    void print();

    int getIndex(PredicateSet set);

    PredicateSet getPredicateSet(Integer index);
}
