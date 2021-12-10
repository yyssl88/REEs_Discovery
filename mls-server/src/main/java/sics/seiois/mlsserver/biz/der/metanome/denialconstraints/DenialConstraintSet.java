package sics.seiois.mlsserver.biz.der.metanome.denialconstraints;

import sics.seiois.mlsserver.biz.der.metanome.evidenceset.IEvidenceSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.Closure;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSetFactory;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.bitset.search.NTreeSearch;

public class DenialConstraintSet implements Iterable<DenialConstraint> {

    private Set<DenialConstraint> constraints = new HashSet<>();

    private long totalTpCount;

    public boolean contains(DenialConstraint dc) {
        return constraints.contains(dc);
    }

    public void removeRHS() {
        Set<DenialConstraint> constraints_bak = new HashSet<>();
        for(DenialConstraint ree : constraints) {
            ree.removeRHS();
            constraints_bak.add(ree);
        }
        constraints = new HashSet<>();
        for(DenialConstraint ree: constraints_bak) {
            constraints.add(ree);
        }

    }

    public void add(DenialConstraint dc) {
        constraints.add(dc);
    }

    @Override
    public Iterator<DenialConstraint> iterator() {
        return constraints.iterator();
    }

    public int size() {
        return constraints.size();
    }

    public long getTotalTpCount() {
        return totalTpCount;
    }

    public void setTotalTpCount(long totalTpCount) {
        this.totalTpCount = totalTpCount;
    }

    private static Logger log = LoggerFactory.getLogger(DenialConstraintSet.class);
}
