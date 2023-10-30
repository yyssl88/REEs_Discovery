package sics.seiois.mlsserver.biz.der.mining.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shapeless.ops.nat;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.ParallelRuleDiscovery;
import sics.seiois.mlsserver.biz.der.mining.ParallelRuleDiscoverySampling;
import sics.seiois.mlsserver.biz.der.mining.model.DQNMLP;
import sics.seiois.mlsserver.biz.der.mining.model.MLPFilterClassifier;
import sics.seiois.mlsserver.biz.der.mining.model.MLPFilterRegressor;

import java.io.Serializable;
import java.util.*;

/*
    store the intermediate results of REE breadth-first search
 */
public class Lattice implements KryoSerializable {

    private static final long serialVersionUID = 349789485707775075L;

    private HashMap<IBitSet, LatticeVertex> latticeLevel = new HashMap<>();
    private int maxTupleNumPerRule = 0;

    private HashSet<IBitSet> allLatticeVertexBits = new HashSet<>();

    public HashMap<IBitSet, LatticeVertex> getLatticeLevel() {
        return this.latticeLevel;
    }


    // only for test
    public void test() {
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            PredicateSet ps = entry.getValue().getPredicates();
            if (ps.toString().contains("ncvoter.t0.voting_intention == ncvoter.t1.voting_intention") &&
                    (ps.toString().contains("ncvoter.t0.party == DEMOCRATIC") || ps.toString().contains("ncvoter.t1.party == DEMOCRATIC"))) {
                logger.info("Check ncvoters information ---> {}", entry.getValue().printCurrent());
            }
        }
    }


    public void setAllLatticeVertexBits(HashMap<IBitSet, LatticeVertex> ll) {
        this.allLatticeVertexBits = new HashSet<>();
        for (IBitSet key : ll.keySet()) {
            allLatticeVertexBits.add(key);
        }
    }

    public void setAllLatticeVertexBits(HashSet<IBitSet> keys) {
        this.allLatticeVertexBits = keys;
    }

    public ArrayList<Lattice> splitLattice(int numOfLattice) {
        ArrayList<Lattice> lattices = new ArrayList<>();
        ArrayList<IBitSet> keys = new ArrayList<>();
        HashSet<IBitSet> keysHash = new HashSet<>();
        for (IBitSet key : latticeLevel.keySet()) {
            keys.add(key);
            keysHash.add(key);
        }
        int num = keys.size();
        if (num <= numOfLattice) {
            for (IBitSet key : keys) {
                Lattice lTemp = new Lattice(this.maxTupleNumPerRule);
                lTemp.addLatticeVertex(this.latticeLevel.get(key));
                // add lattice
                lattices.add(lTemp);
            }
        } else {
            int step = (int) (num * 1.0 / numOfLattice);
            // deal with the first (numOfLattice - 1) ones
            for (int i = 0; i < numOfLattice - 1; i++) {
                int begin = i * step;
                int end = (i + 1) * step;
                Lattice lTemp = new Lattice(this.maxTupleNumPerRule);
                for (int j = begin; j < end; j++) {
                    lTemp.addLatticeVertex(this.latticeLevel.get(keys.get(j)));
                }
                lattices.add(lTemp);
            }
            // deal with the last one
            int begin = (numOfLattice - 1) * step;
            int end = num;
            Lattice lTemp = new Lattice(this.maxTupleNumPerRule);
            for (int j = begin; j < end; j++) {
                lTemp.addLatticeVertex(this.latticeLevel.get(keys.get(j)));
            }
            lattices.add(lTemp);
        }

        // set hash keys
        for (Lattice lattice : lattices) {
            lattice.setAllLatticeVertexBits(keysHash);
        }
        return lattices;
    }


    public Lattice() {
        // this.latticeLevel = new HashSet<>();
        this.latticeLevel = new HashMap<>();
    }

    public Lattice(int maxT) {
        this.latticeLevel = new HashMap<>();
        this.setMaxTupleNumPerRule(maxT);
    }

    public String printCurrents() {
        String res = "";
        for (LatticeVertex lv : latticeLevel.values()) {
            res += " [ " + lv.printCurrent() + " ] && ";
        }
        return res;
    }

    public int size() {
        return this.latticeLevel.size();
    }

    public void setMaxTupleNumPerRule(int maxT) {
        this.maxTupleNumPerRule = maxT;
    }

    public int getMaxTupleNumPerRule() {
        return this.maxTupleNumPerRule;
    }

    public void addLatticeVertex(LatticeVertex lv) {
        IBitSet key = lv.getPredicates().getBitset();
        if (this.latticeLevel.containsKey(key)) {
            this.latticeLevel.get(key).adjustRHSs(lv.getRHSs());
        } else {
            this.latticeLevel.put(key, lv);
        }
////         prune node with empty RHSs
//        if (this.latticeLevel.get(key).getRHSs().size() == 0 || this.latticeLevel.get(key) == null ||
//        this.latticeLevel.get(key).getPredicates() == null || this.latticeLevel.get(key).getRHSs() == null) {
//            this.latticeLevel.remove(key);
//            this.allLatticeVertexBits.remove(key);
//        }
    }

    public void removeLatticeVertex(PredicateSet currPsel, Predicate newP) {
        // remove the LatticeVertex with X being {currPsel, newP} from current lattice.
        PredicateSet tmp_ps = new PredicateSet(currPsel);
        tmp_ps.add(newP);
        IBitSet key = tmp_ps.getBitset();
        if (this.latticeLevel.containsKey(key)) {
            this.latticeLevel.remove(key);
            this.allLatticeVertexBits.remove(key);
        }
    }

    public void removeInvalidLatticeAndRHSs(Lattice parent) {
        HashSet<IBitSet> removedKeys = new HashSet<>();
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            // first check empty RHSs
            LatticeVertex lv = entry.getValue();
            if (lv.getRHSs().size() <= 0) {
                removedKeys.add(entry.getKey());
                continue;
            }
            // second intersection of RHSs
            for (Predicate r : lv.getPredicates()) {
                PredicateSet temp = new PredicateSet(lv.getPredicates());
                temp.remove(r);
                if (parent.getLatticeLevel().containsKey(temp.getBitset())) {
                    lv.getRHSs().and(parent.getLatticeLevel().get(temp.getBitset()).getRHSs());
                }
            }
            if (lv.getRHSs().size() <= 0) {
                removedKeys.add(entry.getKey());
            }
        }
        // prune invalid lattice vertex
        for (IBitSet key : removedKeys) {
            this.latticeLevel.remove(key);
            this.allLatticeVertexBits.remove(key);
        }

    }

    public void prune(HashMap<IBitSet, ArrayList<Predicate>> prunedRHSs) {
        // prune invalid candidate rules
        for (IBitSet key : this.latticeLevel.keySet()) {
            this.latticeLevel.get(key).prune(prunedRHSs);
        }
    }


    /*
        initialize the lattice, i.e., 1st level
        // in the 1st level, do not consider constant predicates
     */
    public void initialize(ArrayList<Predicate> allPredicates, int maxTupleIDs) {
        for (Predicate p : allPredicates) {
            // skip constant predicates
            if (p.isConstant()) {
                continue;
            }
            LatticeVertex lv = new LatticeVertex(p, maxTupleIDs);
            // add latest tuple ID pair. In the beginning, use <t_0, t_1>;
            lv.updateLatestTupleIDs(0, 1);
            // add RHS
            ArrayList<Predicate> filteredRHSs = new ArrayList<>();
            String r_1 = p.getOperand1().getColumn().getTableName();
            String r_2 = p.getOperand2().getColumn().getTableName();
            // update TIDs' relation names
            lv.updateTIDRelationName(r_1, r_2);
            for (Predicate p_ : allPredicates) {
                if (p_.checkIfCorrectRelationName(r_1, r_2)) {
                    filteredRHSs.add(p_);
                }
            }
            lv.addRHSs(filteredRHSs);
            // add to 1st level
            this.addLatticeVertex(lv);
        }
    }

    /*
        only for application driven
     */
    public void initialize(List<Predicate> allPredicates, int maxTupleIDs, List<Predicate> applicationRHSs) {
        for (Predicate p : allPredicates) {
            // skip constant predicates
//            if (p.isConstant()) {
//                continue;
//            }
            LatticeVertex lv = new LatticeVertex(p, maxTupleIDs);
            // add latest tuple ID pair. In the beginning, use <t_0, t_1>;
            lv.updateLatestTupleIDs(p.getIndex1(), p.getIndex2());
            // add RHS
            ArrayList<Predicate> filteredRHSs = new ArrayList<>();
            String r_1 = p.getOperand1().getColumn().getTableName();
            String r_2 = p.getOperand2().getColumn().getTableName();
            // update TIDs' relation names
            lv.updateTIDRelationName(r_1, r_2);
            for (Predicate p_ : applicationRHSs) {
//                if (p_.checkIfCorrectRelationName(r_1, r_2)) {
//                    filteredRHSs.add(p_);
//                }
                filteredRHSs.add(p_);
            }
            lv.addRHSs(filteredRHSs);
            // add to 1st level
            this.addLatticeVertex(lv);
        }
    }

    public void pruneLattice(HashSet<IBitSet> invalidX, HashMap<IBitSet, ArrayList<Predicate>> validXRHSs) {
        HashSet<IBitSet> removedKeys = new HashSet<>();
        for (IBitSet key : this.latticeLevel.keySet()) {
            IBitSet t = this.latticeLevel.get(key).getPredicates().getBitset();
            if (invalidX.contains(t)) {
                removedKeys.add(key);
            }
            // remove valid rules
            if (validXRHSs.containsKey(t)) {
                ArrayList<Predicate> removedRHSs = validXRHSs.get(t);
                for (Predicate rhs : removedRHSs) {
                    this.latticeLevel.get(key).getRHSs().remove(rhs);
                }
                if (this.latticeLevel.get(key).getRHSs().size() <= 0) {
                    removedKeys.add(key);
                }
            }
        }
        // remove some keys
        for (IBitSet key : removedKeys) {
            this.latticeLevel.remove(key);
            this.allLatticeVertexBits.remove(key);
        }
    }

    public void pruneLattice(HashSet<IBitSet> invalidX, HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                             HashMap<IBitSet, ArrayList<Predicate>> validXRHSs, List<Predicate> allPredicates) {
        HashSet<IBitSet> removedKeys = new HashSet<>();
        for (IBitSet key : this.latticeLevel.keySet()) {
            IBitSet t = this.latticeLevel.get(key).getPredicates().getBitset();
            if (invalidX.contains(t)) {
                removedKeys.add(key);
                continue;
            }
            // check |len| - 1 predicateset -- new ADDED, need to re-test and re-think !!!
            for (Predicate r : this.latticeLevel.get(key).getPredicates()) {
                PredicateSet temp = new PredicateSet(this.latticeLevel.get(key).getPredicates());
                temp.remove(r);
                if (invalidX.contains(temp.getBitset())) {
                    removedKeys.add(key);
                    break;
                }
                // removed invalid RHSs
                if (invalidXRHSs.containsKey(temp.getBitset())) {
                    for (Predicate invalidRHS : invalidXRHSs.get(temp.getBitset())) {
                        this.latticeLevel.get(key).getRHSs().remove(invalidRHS);
                    }
                }
                // check 0 RHSs
                if (this.latticeLevel.get(key).getRHSs().size() <= 0) {
                    removedKeys.add(key);
                    break;
                }
            }

            // remove valid rules
            if (validXRHSs.containsKey(t)) {
                ArrayList<Predicate> removedRHSs = validXRHSs.get(t);
                for (Predicate rhs : removedRHSs) {
                    this.latticeLevel.get(key).getRHSs().remove(rhs);
                }
                if (this.latticeLevel.get(key).getRHSs().size() <= 0) {
                    removedKeys.add(key);
                    continue;
                }
                // remove all rhss predicate not in "t"
                for (Predicate cand : allPredicates) {
                    if (cand.getIndex1() != 0 || cand.getIndex2() != 1) {
                        continue;
                    }
                    if (!this.latticeLevel.get(key).getPredicates().containsPredicate(cand)) {
                        // check!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        if (this.latticeLevel.get(key).getRHSs().containsPredicate(cand)) {
                            this.latticeLevel.get(key).getRHSs().remove(cand);
                        }
                    }
                }
            }

            // further prune
            for (Predicate r : this.latticeLevel.get(key).getPredicates()) {
                PredicateSet temp = new PredicateSet(this.latticeLevel.get(key).getPredicates());
                temp.remove(r);
                if (validXRHSs.containsKey(temp.getBitset())) {
                    for (Predicate rhs : validXRHSs.get(temp.getBitset())) {
                        if (this.latticeLevel.get(key).getRHSs().containsPredicate(rhs)) {
                            this.latticeLevel.get(key).getRHSs().remove(rhs);
                        }
                    }
                }
                if (this.latticeLevel.get(key).getRHSs().size() <= 0) {
                    removedKeys.add(key);
                    break;
                }
            }
        }
        // remove some keys
        for (IBitSet key : removedKeys) {
            this.latticeLevel.remove(key);
            this.allLatticeVertexBits.remove(key);
        }
    }

    // early termination
    /*
        supp_ratios:  support ratios of last validation results
        current lattice node: L,  IBitSet of supp_ratios: L
     */
//    private void pruneInterestingnessUB(Interestingness interestingness, double KthScore, HashMap<IBitSet, Double> supp_ratios) {
//        for (IBitSet key : this.latticeLevel.keySet()) {
//            LatticeVertex lv = this.latticeLevel.get(key);
//            double ub = interestingness.computeUB(supp_ratios.get(lv.getPredicates().getBitset()), 1.0, lv.getPredicates(), null);
//            if (ub < KthScore) {
//                this.latticeLevel.remove(key);
//                this.allLatticeVertexBits.remove(key);
//            }
//        }
//    }

    // early termination
    /*
        supp_ratios:  support ratios of last current X
        current lattice node: L, IBitSet of supp_ratios: L - 1
     */
    public void pruneXInterestingnessUB(Interestingness interestingness, double KthScore, HashMap<PredicateSet, Double> supp_ratios, String topKOption) {
//        ArrayList<Double> ubScores = new ArrayList<>();
//        ArrayList<Double> nonZeroUBScores = new ArrayList<>();
        HashSet<IBitSet> removeKeys = new HashSet<>();
        for (IBitSet key : this.latticeLevel.keySet()) {
//            double curr_ub = 0.0;
            LatticeVertex lv = this.latticeLevel.get(key);
            PredicateSet current = lv.getPredicates();

            // first check all predicates in lv, prune all
//            if (supp_ratios.containsKey(lv.getPredicates())) {
//                PredicateSet tt = new PredicateSet(current);
//                double ub = interestingness.computeUB(supp_ratios.get(lv.getPredicates()), 1.0, tt, null, topKOption);
////                curr_ub = ub;
////                logger.info("#### KthScore: {}, ub: {}, curr_ub: {}", KthScore, ub, curr_ub);
//                if (ub <= KthScore) {
//                    removeKeys.add(key);
//                    continue;
//                }
//            }

            // loop all RHSs to check whether some of them can be pruned based on the interestingness UB
            for (Predicate rhs : lv.getRHSs()) {
                if (!current.containsPredicate(rhs)) {
                    continue;
                }
                PredicateSet tt = new PredicateSet(current);
                tt.remove(rhs);
                if (supp_ratios.containsKey(current)) {
                    double ub = interestingness.computeUB(supp_ratios.get(current) * interestingness.getAllCount(), 1.0, tt, rhs, topKOption);
                    if (ub <= KthScore) {
                        //removeKeys.add(key);
                        // cannot remove the key, but the rhs
                        lv.getRHSs().remove(rhs);
                        break;
                    }
                }
                if (supp_ratios.containsKey(tt)) {
                    double ub = interestingness.computeUB(supp_ratios.get(tt) * interestingness.getAllCount(), 1.0, tt, rhs, topKOption);
                    if (ub <= KthScore) {
                        // remove the RHS predicate (p_0) if it did not meet the value of UB
                        lv.getRHSs().remove(rhs);

                        // if at least one RHS UB does not satisfy, remove the lattice vertex, -- new ADDED, need to re-test and re-think
                        // removeKeys.add(key);
                        break;
                    }
                }
            }
//            ubScores.add(curr_ub);
//            if (curr_ub != 0.0) {
//                nonZeroUBScores.add(curr_ub);
//            }
            if (lv.getRHSs().size() <= 0) {
                removeKeys.add(key);
            }
        }
//        nonZeroUBScores.sort(Comparator.reverseOrder());
//        if (nonZeroUBScores.size() > 10 * 4) {
//            double new2KthScore = nonZeroUBScores.get(40);
//            logger.info("#### lv num: {}, removeKeys.size: {}, nonZeroUBScores.size: {}, new2KthScore: {}", this.latticeLevel.size(), removeKeys.size(), nonZeroUBScores.size(), new2KthScore);
//            int lv_idx = 0;
//            for (IBitSet key : this.latticeLevel.keySet()) {
//                if (ubScores.get(lv_idx) != 0.0 && ubScores.get(lv_idx) <= new2KthScore) {
//                    removeKeys.add(key);
//                }
//                lv_idx = lv_idx + 1;
//            }
//            logger.info("#### removeKeys.size: {}", removeKeys.size());
//        }

        // remove useless keys
        for (IBitSet key : removeKeys) {
            this.latticeLevel.remove(key);
            this.allLatticeVertexBits.remove(key);
        }
    }


    public void pruneXInterestingnessUB(Interestingness interestingness, double KthScore,
                                        HashMap<PredicateSet, Double> supp_ratios, ArrayList<LatticeVertex> partialRules) {
        HashSet<IBitSet> removeKeys = new HashSet<>();
        // record the interestingness UBs
        HashMap<Predicate, Double> removeRHSs = new HashMap<>();
        for (IBitSet key : this.latticeLevel.keySet()) {
            LatticeVertex lv = this.latticeLevel.get(key);
            removeRHSs.clear();
            // loop all RHSs to check whether some of them can be pruned based on the interestingness UB
            PredicateSet current = lv.getPredicates();
            for (Predicate rhs : lv.getRHSs()) {
                PredicateSet tt = new PredicateSet(current);
                tt.remove(rhs);
                if (supp_ratios.containsKey(tt)) {
//                    double ub = interestingness.computeUB(supp_ratios.get(tt), 1.0, tt, null);
//                    if (ub < KthScore) {
//                        // remove the RHS predicate (p_0) if it did not meet the value of UB
//                        lv.getRHSs().remove(rhs);
//                        removeRHSs.put(rhs, ub);
//                    }
                }
            }

            // anytime top-K, store unsatisfying cases
            if (removeRHSs.size() > 0) {
                LatticeVertex candidate = new LatticeVertex(lv.getMaxTupleID());
                // without copy RHSs
                candidate.copyWithOutRHSs(lv);
                for (Predicate rhs : removeRHSs.keySet()) {
                    candidate.addRHS(rhs);
                    candidate.setInterestingnessUB(rhs, removeRHSs.get(rhs));
                }
                // add into the candidate list
                partialRules.add(candidate);
            }

            if (lv.getRHSs().size() <= 0) {
                removeKeys.add(key);
            }
        }
        // remove useless keys
        for (IBitSet key : removeKeys) {
            this.latticeLevel.remove(key);
            this.allLatticeVertexBits.remove(key);
        }
    }


    private boolean checkValidSubSetX(LatticeVertex lv_child, HashSet<IBitSet> invalidX) {
        boolean valid = true;
        for (Predicate rp : lv_child.getPredicates()) {
            PredicateSet ps = new PredicateSet(lv_child.getPredicates());
            ps.remove(rp);
            if (invalidX.contains(ps.getBitset())) {
                valid = false;
                break;
            }
        }
        return valid;
    }

    // check: (1) whether there exists constant predicates with the same index and attribute but different constant values in {lv, newP};
    // (2) whether there exists constant predicates with the same attribute and constant but different index in {lv, newP}
    public boolean checkInvalidExtension(LatticeVertex lv, Predicate newP) {
//        if (!newP.isConstant()) {
//            return false;
//        }
        int index_check = newP.getIndex1();
        String attr_check = newP.getOperand1().getColumnLight().getName();
        String constant_check = newP.getConstant();
        for (Predicate p : lv.getPredicates()) {
            if (!p.isConstant()) {
                continue;
            }
            if (!p.getOperand1().getColumnLight().getName().equals(attr_check)) {
                continue;
            }
            if (p.getIndex1() == index_check) { // (1) eg: t0.A=a1, t0.A=a2, should not co-exist
                return true;
            }
            if (!p.getConstant().equals(constant_check)) {  // (2) eg: t0.A=a1, t1.A=a2, which equals to inequality predicate t0.A <> t1.A, meaningless
                return true;
            }
        }
        return false;
    }


    // for bi-variable rees with equality operator
    public boolean checkTrivialExtension(LatticeVertex lv, Predicate newP) {
        boolean t0_exist = false;
        boolean t1_exist = false;
        boolean t0_t1_exist = false;
        if (newP.isConstant()) {
            String attr = newP.getOperand1().getColumnLight().getName();
            int index = newP.getIndex1();
            if (index == 0) {
                t0_exist = true;
            } else {
                t1_exist = true;
            }
            for (Predicate p : lv.getPredicates()) {
                if (p.isConstant()) {
                    if (!p.getOperand1().getColumnLight().getName().equals(attr)) {
                        continue;
                    }
                    int index_p = p.getIndex1();
                    if (index_p == 0) {
                        t0_exist = true;
                    } else {
                        t1_exist = true;
                    }
                } else {
                    if (!p.getOperand1().getColumnLight().getName().equals(attr)) {
                        continue;
                    }
                    if (!p.getOperand2().getColumnLight().getName().equals(attr)) {
                        continue;
                    }
                    t0_t1_exist = true;
                }
            }
        } else {
            t0_t1_exist = true;
            String attr_1 = newP.getOperand1().getColumnLight().getName();
            String attr_2 = newP.getOperand2().getColumnLight().getName();
            for (Predicate p : lv.getPredicates()) {
                if (p.isConstant()) {
                    int index_p = p.getIndex1();
                    if (index_p == newP.getIndex1() && p.getOperand1().getColumnLight().getName().equals(attr_1)) {
                        if (index_p == 0) {
                            t0_exist = true;
                        } else {
                            t1_exist = true;
                        }
                    }
                    if (index_p == newP.getIndex2() && p.getOperand1().getColumnLight().getName().equals(attr_2)) {
                        if (index_p == 0) {
                            t0_exist = true;
                        } else {
                            t1_exist = true;
                        }
                    }
                }
            }
        }
        return t0_exist && t1_exist && t0_t1_exist;
    }


    /*
        For lattice combination, e.g., AB + BC -> ABC
     */
    private boolean checkValidExtension(LatticeVertex lv, Predicate newP) {
        for (Predicate rp : lv.getPredicates()) {
            PredicateSet ps = new PredicateSet(lv.getPredicates());
            ps.remove(rp);
            ps.add(newP);

            // if ps contains all constant predicates, continue
            boolean allConstants = true;
            for (Predicate p : ps) {
                if (!p.isConstant()) {
                    allConstants = false;
                }
            }
            if (allConstants) {
                continue;
            }


            if (!this.allLatticeVertexBits.contains(ps.getBitset())) {
                return false;
            }
//            if (this.latticeLevel.containsKey(ps.getBitset())) {
//                return true;
//            }
        }
        return true;
    }

    private boolean checkValidConstantPredicateExtension(LatticeVertex lv, Predicate cp) {
        if (lv.getPredicates().size() == 4) {
            //logger.info(".......");
        }
        for (Predicate p : lv.getPredicates()) {
            if (p.isConstant()) {
                if (p.getIndex1() == cp.getIndex1() && p.getOperand1().equals(cp.getOperand1())) {
                    return false;
                }
            }
        }
        return true;
    }

    private int checkNumberOfConstantPredicate(LatticeVertex lv) {
        int res = 0;
        for (Predicate p : lv.getPredicates()) {
            if (p.isConstant()) {
                res++;
            }
        }
        return res;
    }

    /*
        check whether current predicateset ONLY contains all constant predicates
     */
    private boolean ifAllConstantPredicates(PredicateSet ps) {
//        if (ps.size() <= 1) {
//            return false;
//        }
        for (Predicate p : ps) {
            if (!p.isConstant()) {
                return false;
            }
        }
        return true;

//        int countConstants = 0;
//        for (Predicate p : ps) {
//            if (p.isConstant()) {
//                countConstants ++;
//            }
//        }
//        if (countConstants > 1) {
//            return true;
//        } else {
//            return false;
//        }

    }

    /*
        invalidX: predicates of lattice vertex whose support < k
        option: "original" and "anytime"
        ifRL: 1-use RL; 0-not use RL
        ifOnlineTrainRL: 1-online; 0-offline
        ifOfflineTrainStage: 1-offline training stage-to get sequence for RL offline training; 0-offline prediction stage
     */

    public boolean testEntry(LatticeVertex lv) {
        // boolean f = true;
        PredicateSet p = lv.getPredicates();
            if (p.toString().trim().contains("ncvoter.t0.voting_intention == ncvoter.t1.voting_intention") &&
                    (p.toString().trim().contains("ncvoter.t0.party == DEMOCRATIC") || p.toString().trim().contains("ncvoter.t1.party == DEMOCRATIC"))) {
                return true;
            }
            return false;
    }

    public LatticeVertex expandLatticeByTopK(LatticeVertex lv, Predicate newP, double kth,
                                             Interestingness interestingness,
                                             HashMap<String, Integer> predicatesHashIDs, HashMap<PredicateSet, Double> suppRatios,
                                             String topKOption) {
        if (topKOption.equals("noFiltering")) {
            return new LatticeVertex(lv, newP);
        }
        ArrayList<Predicate> validRHSs = new ArrayList<>();
        int numPredicates = predicatesHashIDs.size();
        PredicateSet pSet = new PredicateSet();
        // add P_sel
        for (Predicate p : lv.getPredicates()) {
            pSet.add(p);
        }
        pSet.add(newP);
        for (Predicate rhs : lv.getRHSs()) {
            // remove RHS
            if (pSet.containsPredicate(rhs)) {
                pSet.remove(rhs);
            }
            boolean f1 = false, f2 = false;
            if (suppRatios.containsKey(pSet)) {
                f1 = true;
                double UBScore = interestingness.computeUB(suppRatios.get(pSet) * interestingness.getAllCount(), 1.0, pSet, rhs, topKOption);
                if (UBScore > kth) {
                    validRHSs.add(rhs);
                }
            }
            pSet.remove(newP);
            if (suppRatios.containsKey(pSet)) {
                f2 = true;
                double UBScore = interestingness.computeUB(suppRatios.get(pSet) * interestingness.getAllCount(), 1.0, pSet, rhs, topKOption);
                if (UBScore > kth) {
                    validRHSs.add(rhs);
                }
            }

            // add newP
            pSet.add(newP);
            if (f1 == false && f2 == false) {
                double UBScore = interestingness.computeUB(interestingness.getAllCount() * interestingness.getAllCount(), 1.0, pSet, rhs, topKOption);
                if (UBScore > kth) {
                    validRHSs.add(rhs);
                }
            }

            // add RHS
            pSet.add(rhs);
        }
        // adjust RHSs, such that only keep valid RHSs predicted by DQN
        if (validRHSs.size() == 0) {
            return null;
        }
        LatticeVertex lv_new = new LatticeVertex(lv, newP);
        lv_new.adjustRHSs(validRHSs);
        return  lv_new;
    }

    public LatticeVertex expandLatticeByDQN(LatticeVertex lv, Predicate newP, List<Predicate> allPredicates, MLPFilterClassifier mlpFilterClassifier, HashMap<String, Integer> predicatesHashIDs, boolean ifDQN) {
        if (ifDQN == false) {
            return new LatticeVertex(lv, newP);
        }
        ArrayList<Predicate> validRHSs = new ArrayList<>();
        int numPredicates = predicatesHashIDs.size();
        double[][] feature_vectors = new double[1][numPredicates * 2];
        // add P_sel
        for (Predicate p : lv.getPredicates()) {
            feature_vectors[0][predicatesHashIDs.get(p.toString().trim())] = 1.0;
        }
        // feature_vectors[0][predicatesHashIDs.get(newP.toString())] = 1.0;
        for (Predicate rhs : lv.getRHSs()) {
            // remove RHS
            feature_vectors[0][predicatesHashIDs.get(rhs.toString().trim())] = 0.0;
            // add RHS
            feature_vectors[0][numPredicates + predicatesHashIDs.get(rhs.toString().trim())] = 1.0;
            // add the new predicate
            feature_vectors[0][predicatesHashIDs.get(newP.toString().trim())] = 1.0;
            if (mlpFilterClassifier.run(feature_vectors)) {
                validRHSs.add(rhs);
            }
        }
        // adjust RHSs, such that only keep valid RHSs predicted by DQN
        if (validRHSs.size() == 0) {
            return null;
        }
        LatticeVertex lv_new = new LatticeVertex(lv, newP);
        lv_new.adjustRHSs(validRHSs);
        return  lv_new;
    }


    public Lattice generateNextLatticeLevel(List<Predicate> allPredicates, ArrayList<Predicate> allExistPredicates, HashSet<IBitSet> invalidX,
                                            HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                                            HashMap<IBitSet, ArrayList<Predicate>> validXRHSs,
                                            Interestingness interestingness, double KthScore, HashMap<PredicateSet, Double> suppRatios,
                                            PredicateProviderIndex predicateProviderIndex,
                                            String option, ArrayList<LatticeVertex> partialRules,
                                            int ifRL, int ifOnlineTrainRL, int ifOfflineTrainStage, boolean ifExistModel,
                                            String python_path, String RL_code_path,
                                            float lr, float rd, float eg, int rtr, int ms, int bs,
                                            String table_name, int N_num, HashMap<String, Integer> predicatesHashIDs, String topKOption) {
        logger.info("#####generate Next Lattice level!");
        // for RL
        ArrayList<PredicateSet> currPsel = new ArrayList<>();
        ArrayList<Predicate> newPredicates = new ArrayList<>();
//        ArrayList<LatticeVertex> newLatticeVertices = new ArrayList<>();

        Lattice nextLevel = new Lattice(this.maxTupleNumPerRule);

//        logger.info("beginning lattice : {} with option {}", this.printCurrents(), option);
//        logger.info("All predicates : {}", allPredicates);
//        logger.info("Invalid predicate set : {}", invalidX);
//        logger.info("Valid predicate mapping : {}", validXRHSs);

        // prune invalid lattice nodes
//        this.pruneLattice(invalidX, invalidXRHSs, validXRHSs, allPredicates);

        // prune with interestingness UB
//        logger.info("#### before pruning with UB, LatticeVertex num: {}", this.latticeLevel.size());
//        if (option.equals("original")) {
//            this.pruneXInterestingnessUB(interestingness, KthScore, suppRatios);
//        } else if (option.equals("anytime")) {
//            this.pruneXInterestingnessUB(interestingness, KthScore, suppRatios, partialRules);
//        }
//        logger.info("#### after pruning with UB, LatticeVertex num: {}", this.latticeLevel.size());

        /*
        else {
            logger.info("wrong option of top-K, plz select [original] or [anytime]");
            return null;
        }
         */
        // 1. scan all parent level with current pairs of relations
        int maxTupleIDInLevel = 0;
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            // expand each vertex with a new predicate
            // IBitSet k = entry.getKey();

            // test
//            if(testEntry(entry.getValue())) {
//                logger.info("test LV node");
//            }
            LatticeVertex lv = entry.getValue();
            if (this.ifAllConstantPredicates(lv.getPredicates())) {
                continue;
            }
            // check support K --- pruning
            if (invalidX.contains(lv.getPredicates().getBitset())) {
                continue;
            }
            ImmutablePair<Integer, Integer> tid_pair = lv.getLatestTupleIDs();
            maxTupleIDInLevel = Math.max(maxTupleIDInLevel, tid_pair.right);
            String r_1 = lv.getTIDRelationName(tid_pair.left);
            String r_2 = lv.getTIDRelationName(tid_pair.right);
//            logger.info("Latest relations : {} and {}", r_1, r_2);
            for (Predicate p : allPredicates) {
//                logger.info("Scan predicate {}", p);
                if (!p.checkIfCorrectRelationName(r_1, r_2)) {
                    continue;
                }
//               logger.info("Skip relations ...");
//               if (lv.ifContain(p)) {
//                   continue;
//               }
                if (!p.isConstant()) {
                    // a new lattice vertex in the next level
                    Predicate newP = predicateProviderIndex.getPredicate(p, tid_pair.left, tid_pair.right);
                    if (lv.ifContain(newP)) {
                        continue;
                    }
                    // subset checking
                    if (!this.checkValidExtension(lv, newP)) {
                        continue;
                    }
                    // check trivial rules
                    if (this.checkTrivialExtension(lv, newP)) {
                        continue;
                    }
                    // LatticeVertex lv_child = new LatticeVertex(lv, newP);
                    // LatticeVertex lv_child = this.expandLatticeByDQN(lv, newP, allPredicates, dqnmlp, predicatesHashIDs, ifDQN);
                    LatticeVertex lv_child = this.expandLatticeByTopK(lv, newP, KthScore, interestingness, predicatesHashIDs, suppRatios, topKOption);
                    if (lv_child == null) {
                        continue;
                    }
                    // check support k
//                    boolean valid = this.checkValidSubSetX(lv_child, invalidX);
//                    if (valid && (!invalidX.contains(lv_child.getPredicates().getBitset()))) {
                        nextLevel.addLatticeVertex(lv_child);
                        if (ifRL == 1 && ifExistModel) { // for RL
                            currPsel.add(lv.getPredicates());
                            newPredicates.add(newP);
//                           newLatticeVertices.add(lv_child);
                        }
//                    }
                } else {
                    for (int idx = 0; idx < 2; idx++) {
                        // left
                        if (idx == 0 && p.checkIfCorrectOneRelationName(r_1)) {
                            Predicate cp1 = predicateProviderIndex.getPredicate(p, tid_pair.left, tid_pair.left);
                            if (lv.ifContain(cp1)) {
                                continue;
                            }
                            if (!this.checkValidExtension(lv, cp1)) {
                                continue;
                            }
//                            if (!this.checkValidConstantPredicateExtension(lv, cp1)) {
//                                continue;
//                            }
                            // check (1) whether there exists constant predicates with the same index and attribute but different constant values in {lv, newP};
                            // (2) whether there exists constant predicates with the same attribute and constant but different index in {lv, newP}
                            if (this.checkInvalidExtension(lv, cp1)) {
                                continue;
                            }
                            // check trivial rules
                            if (this.checkTrivialExtension(lv, cp1)) {
                                continue;
                            }

                            // LatticeVertex lv_child = new LatticeVertex(lv, cp1);
                            // LatticeVertex lv_child = this.expandLatticeByDQN(lv, cp1, allPredicates, dqnmlp, predicatesHashIDs, ifDQN);
                            LatticeVertex lv_child = this.expandLatticeByTopK(lv, cp1, KthScore, interestingness, predicatesHashIDs, suppRatios, topKOption);
                            if (lv_child == null) {
                                continue;
                            }
//                        boolean valid = this.checkValidSubSetX(lv_child, invalidX);
//                        if (valid && (!invalidX.contains(lv_child.getPredicates().getBitset()))) {
                            nextLevel.addLatticeVertex(lv_child);
                            if (ifRL == 1 && ifExistModel) { // for RL
                                currPsel.add(lv.getPredicates());
                                newPredicates.add(cp1);
//                           newLatticeVertices.add(lv_child);
                            }
//                        }
                        }

                        // right
                        if (idx == 1 && p.checkIfCorrectOneRelationName(r_2)) {
                            Predicate cp2 = predicateProviderIndex.getPredicate(p, tid_pair.right, tid_pair.right);
                            if (lv.ifContain(cp2)) {
                                continue;
                            }
                            if (!this.checkValidExtension(lv, cp2)) {
                                continue;
                            }
//                            if (!this.checkValidConstantPredicateExtension(lv, cp2)) {
//                                continue;
//                            }
                            // check (1) whether there exists constant predicates with the same index and attribute but different constant values in {lv, newP};
                            // (2) whether there exists constant predicates with the same attribute and constant but different index in {lv, newP}
                            if (this.checkInvalidExtension(lv, cp2)) {
                                continue;
                            }
                            // check trivial rules
                            if (this.checkTrivialExtension(lv, cp2)) {
                                continue;
                            }
                            // LatticeVertex lv_child_ = new LatticeVertex(lv, cp2);
                            // LatticeVertex lv_child_ = this.expandLatticeByDQN(lv, cp2, allPredicates, dqnmlp, predicatesHashIDs, ifDQN);
                            LatticeVertex lv_child_ = this.expandLatticeByTopK(lv, cp2, KthScore, interestingness, predicatesHashIDs, suppRatios, topKOption);
                            if (lv_child_ == null) {
                                continue;
                            }
//                        boolean valid = this.checkValidSubSetX(lv_child_, invalidX);
//                        if (valid && (!invalidX.contains(lv_child_.getPredicates().getBitset()))) {
                            nextLevel.addLatticeVertex(lv_child_);
                            if (ifRL == 1 && ifExistModel) { // for RL
                                currPsel.add(lv.getPredicates());
                                newPredicates.add(cp2);
//                           newLatticeVertices.add(lv_child_);
                            }
//                        }
                        }
                    }

                }

            }
        }

//        logger.info("Expand First Step lattice : {}", nextLevel.printCurrents());

        if (this.maxTupleNumPerRule <= 2) {
            return nextLevel;
        }

        // 2. scan all parent level and expand predicates with a new predicate containing one new tuple
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            LatticeVertex lv = entry.getValue();
            if (this.ifAllConstantPredicates(lv.getPredicates())) {
                continue;
            }
            ImmutablePair<Integer, Integer> tupleIDPair = lv.getLatestTupleIDs();
            /*
            if (tupleIDPair.right != maxTupleIDInLevel) {
                continue;
            }

             */
            int new_tid = tupleIDPair.right + 1;
            // exceed the maximum number of tuples in a REE rule
            if (new_tid >= this.maxTupleNumPerRule) {
                continue;
            }
            for (int tid = 0; tid < new_tid; tid++) {
                String r_1 = lv.getTIDRelationName(tid);
                if (r_1 == null) {
                    continue;
                }
                for (Predicate p : allPredicates) {
                    if (p.isConstant()) {
                        continue;
                    }
                    if (!p.checkIfCorrectOneRelationName(r_1)) {
                        continue;
                    }
                    /*
                    if (lv.ifContain(p)) {
                        continue;
                    }
                     */
                    // a new lattice vertex in the next level
                    Predicate newP = predicateProviderIndex.getPredicate(p, tid, new_tid);
                    if (lv.ifContain(newP)) {
                        continue;
                    }
                    if (!this.checkValidExtension(lv, newP)) {
                        continue;
                    }
                    // LatticeVertex lv_child = new LatticeVertex(lv, newP);
                    // LatticeVertex lv_child = this.expandLatticeByDQN(lv, newP, allPredicates, dqnmlp, predicatesHashIDs, ifDQN);
                    LatticeVertex lv_child = this.expandLatticeByTopK(lv, newP, KthScore, interestingness, predicatesHashIDs, suppRatios, topKOption);
                    if (lv_child == null) {
                        continue;
                    }
                    // set new tid indices
                    lv_child.updateLatestTupleIDs(tid, new_tid);
                    // update latest relation names
                    lv_child.updateTIDRelationName(r_1, p.getOperand2().getColumn().getTableName());
                    if (!invalidX.contains(lv_child.getPredicates().getBitset())) {
                        nextLevel.addLatticeVertex(lv_child);
                        if (ifRL == 1 && ifExistModel) { // for RL
                            currPsel.add(lv.getPredicates());
                            newPredicates.add(newP);
//                            newLatticeVertices.add(lv_child);
                        }
                    }
                }
            }
        }

//        logger.info("Expand Second Step lattice : {}", nextLevel.printCurrents());


        // 3. jump to the next tid pair <t, s>
//        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
//            LatticeVertex lv = entry.getValue();
//            ImmutablePair<Integer, Integer> tupleIDPair = lv.getLatestTupleIDs();
//            for (int tid = tupleIDPair.left + 1; tid < tupleIDPair.right; tid ++) {
//                String r_1 = lv.getTIDRelationName(tid);
//                String r_2 = lv.getTIDRelationName(tupleIDPair.right);
//                if (r_1 == null || r_2 == null) {
//                    continue;
//                }
//                for (Predicate p : allPredicates) {
//                    if (! p.checkIfCorrectRelationName(r_1, r_2)) {
//                        continue;
//                    }
//                    /*
//                    if (lv.ifContain(p)) {
//                        continue;
//                    }
//                     */
//                    // a new lattice vertex in the next level
//                    Predicate newP = predicateProviderIndex.getPredicate(p, tid, tupleIDPair.right);
//                    if (lv.ifContain(newP)) {
//                        continue;
//                    }
//                    if (! this.checkValidExtension(lv, newP)) {
//                        continue;
//                    }
//                    LatticeVertex lv_child = new LatticeVertex(lv, newP);
//                    // set new tid indices
//                    lv_child.updateLatestTupleIDs(tid, tupleIDPair.right);
//                    // update latest relation names
//                    lv_child.updateTIDRelationName(r_1, r_2);
//                    boolean valid = this.checkValidSubSetX(lv_child, invalidX);
//                    if (valid && (!invalidX.contains(lv_child.getPredicates().getBitset()))) {
//                        nextLevel.addLatticeVertex(lv_child);
//                    }
//
//                }
//            }
//        }
//        logger.info("Expand Third Step lattice : {}", nextLevel.printCurrents());

        if (ifRL == 0) {
            logger.info("#### generate next {} LatticeVertex", nextLevel.size());
            return nextLevel;
        }

        if (ifOnlineTrainRL == 1 && !ifExistModel) {
            logger.info("#### there's no model being trained");
            return nextLevel;
        }

        if (ifOnlineTrainRL == 0 && ifOfflineTrainStage == 1) {
            return nextLevel;
        }

        if (currPsel.size() == 0) {
            return nextLevel;
        }

        // use RL to prune
        logger.info("#### before pruning with RL, there are {} LatticeVertex at next level", nextLevel.getLatticeLevel().size());
        long beginRLPruningTime = System.currentTimeMillis();

        ParallelRuleDiscoverySampling tmp_parallelRuleDiscoverySampling = new ParallelRuleDiscoverySampling();
        if (ifOnlineTrainRL == 1) {
            // load RL model from HDFS for online RL; As for offline RL, scp the model to each worker before discover rules
            long beginLoadTime = System.currentTimeMillis();
            tmp_parallelRuleDiscoverySampling.putLoadModelToFromHDFS(RL_code_path, 0, table_name);
            long loadTime = System.currentTimeMillis() - beginLoadTime;
            logger.info("#### load RL model time: {}", loadTime);
        }

        // use RL model to predict
        String sequence = tmp_parallelRuleDiscoverySampling.transformSequence(allExistPredicates, currPsel, newPredicates, null);
        long beginUseRLTime = System.currentTimeMillis();
        String predicted_results = tmp_parallelRuleDiscoverySampling.useRL(sequence, 0, 0, "", allExistPredicates.size(),
                python_path, RL_code_path, lr, rd, eg, rtr, ms, bs, table_name, N_num);  // form: "1;0;1;1;0;1;..."
        long predictTime = System.currentTimeMillis() - beginUseRLTime;
        logger.info("#### predicted results: {}", predicted_results);
        logger.info("#### use RL to predict whether expand, using time: {}", predictTime);

        // pruning
        int idx = 0;
        for (String ifExpand : predicted_results.split(";")) {
            if (ifExpand.equals("0")) {
                nextLevel.removeLatticeVertex(currPsel.get(idx), newPredicates.get(idx));
            }
            idx = idx + 1;
        }
        long RLPruningTime = System.currentTimeMillis() - beginRLPruningTime;
        logger.info("#### after pruning with RL, there are {} LatticeVertex at next level, using time: {}", nextLevel.getLatticeLevel().size(), RLPruningTime);

        return nextLevel;
    }


    /*
        2022/1/22: fast version of expanding lattice
     */
    public void removeMoreConstantPredicatesHeuristic() {
        HashSet<IBitSet> removedKeys = new HashSet<>();
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            LatticeVertex lv = entry.getValue();
            int countConstants = 0;
            for (Predicate p : lv.getPredicates()) {
                if (p.isConstant()) {
                    countConstants ++;
                }
            }
            if (countConstants > 1) {
                removedKeys.add(entry.getKey());
            }
        }
        for (IBitSet bs : removedKeys) {
            this.latticeLevel.remove(bs);
            this.allLatticeVertexBits.remove(bs);
        }
    }

    public Lattice generateNextLatticeLevelFast(ArrayList<IBitSet> IBitSet1, ArrayList<IBitSet> IBitSet2, List<Predicate> allPredicates, ArrayList<Predicate> allExistPredicates, HashSet<IBitSet> invalidX,
                                            HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                                            HashMap<IBitSet, ArrayList<Predicate>> validXRHSs,
                                            Interestingness interestingness, double KthScore, HashMap<PredicateSet, Double> suppRatios,
                                            PredicateProviderIndex predicateProviderIndex,
                                            String option, ArrayList<LatticeVertex> partialRules,
                                            int ifRL, int ifOnlineTrainRL, int ifOfflineTrainStage, boolean ifExistModel,
                                            String python_path, String RL_code_path,
                                            float lr, float rd, float eg, int rtr, int ms, int bs,
                                            String table_name, int N_num) {
        logger.info("#####generate Next Lattice level! {}", this.latticeLevel.size());

        Lattice nextLevel = new Lattice(this.maxTupleNumPerRule);

        //this.removeMoreConstantPredicatesHeuristic();
        // check whether size 1
        if (this.latticeLevel.get(IBitSet1.get(0)).getPredicates().size() <= 1) {
            for (int e1 = 0; e1 < IBitSet1.size(); e1++) {
                for (int e2 = 0; e2 < IBitSet2.size(); e2++) {
                    if (IBitSet1.equals(IBitSet2) && e1 >= e2) continue;
                    Predicate newP = null;
                    for (Predicate p : this.latticeLevel.get(IBitSet2.get(e2)).getPredicates()) {
                        newP = p;
                    }
                    LatticeVertex lv_child = new LatticeVertex(this.latticeLevel.get(IBitSet1.get(e1)), newP);
                    nextLevel.addLatticeVertex(lv_child);
                }
            }
            return nextLevel;
        }

        logger.info("#####start constructing inverted index!");
        // 1. construct index
        HashMap<Predicate, ArrayList<Integer>> invertedIndex1 = new HashMap<>();
        for (int iid = 0; iid < IBitSet1.size(); iid++) {
            IBitSet bitSet = IBitSet1.get(iid);
            LatticeVertex lv = this.latticeLevel.get(bitSet);
            int count = 0;
            for (Predicate p : lv.getPredicates()) {
                if (count >= 2) {
                    break;
                }
                if (invertedIndex1.containsKey(p)) {
                    invertedIndex1.get(p).add(iid);
                } else {
                    ArrayList<Integer> arr = new ArrayList<>();
                    arr.add(iid);
                    invertedIndex1.put(p, arr);
                }
                count ++;
            }
        }
        HashMap<Predicate, ArrayList<Integer>> invertedIndex2 = new HashMap<>();
        if (! IBitSet1.equals(IBitSet2)) {
            for (int iid = 0; iid < IBitSet2.size(); iid++) {
                IBitSet bitSet = IBitSet2.get(iid);
                LatticeVertex lv = this.latticeLevel.get(bitSet);
                int count = 0;
                for (Predicate p : lv.getPredicates()) {
                    if (count >= 2) {
                        break;
                    }
                    if (invertedIndex2.containsKey(p)) {
                        invertedIndex2.get(p).add(iid);
                    } else {
                        ArrayList<Integer> arr = new ArrayList<>();
                        arr.add(iid);
                        invertedIndex2.put(p, arr);
                    }
                    count ++;
                }
            }
        } else {
            invertedIndex2 = invertedIndex1;
        }
        HashSet<ImmutablePair<Integer, Integer>> results = new HashSet<>();
        logger.info("#####retrieve results!");
        // 2. retrieve candidates
        for (Map.Entry<Predicate, ArrayList<Integer>> entry : invertedIndex1.entrySet()) {
            ArrayList<Integer> arr1 = entry.getValue();
            if (invertedIndex2.containsKey(entry.getKey())) {
                ArrayList<Integer> arr2 = invertedIndex2.get(entry.getKey());
                for (Integer e1 : arr1) {
                    for (Integer e2 : arr2) {
                        if (IBitSet1.equals(IBitSet2) && e1.intValue() >= e2.intValue()) continue;
                        if (results.contains(new ImmutablePair<>(e1, e2)))  {
                            continue;
                        }
                        results.add(new ImmutablePair<>(e1, e2));
                        PredicateSet ps1 = this.latticeLevel.get(IBitSet1.get(e1)).getPredicates();
                        PredicateSet ps2 = this.latticeLevel.get(IBitSet2.get(e2)).getPredicates();
                        int cc = 0;
                        Predicate newP = null;
                        for (Predicate pp : ps1) {
                            if (ps2.containsPredicate(pp)) {
                                cc += 1;
                            } else {
                                newP = pp;
                            }
                        }
                        if (cc == ps1.size() - 1) {
                            ImmutablePair<Integer, Integer> pair = new ImmutablePair<>(e1, e2);
                            LatticeVertex lv_child = new LatticeVertex(this.latticeLevel.get(IBitSet2.get(e2)), newP);
                            nextLevel.addLatticeVertex(lv_child);
                        }
                    }
                }
            }
        }
        logger.info("#####finish the expansion!");
        return nextLevel;
    }


    /*
        invalidX: predicates of lattice vertex whose support < k
        option: "original" and "anytime"
     */
    public Lattice generateNextLatticeLevelSampling(List<Predicate> allPredicatesTemplate, HashSet<IBitSet> invalidX,
                                                    HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                                                    HashMap<IBitSet, ArrayList<Predicate>> validXRHSs,
                                                    HashMap<PredicateSet, Double> suppRatios,
                                                    PredicateProviderIndex predicateProviderIndex,
                                                    String option, ArrayList<LatticeVertex> partialRules) {
        Lattice nextLevel = new Lattice(this.maxTupleNumPerRule);

//        logger.info("beginning lattice : {} with option {}", this.printCurrents(), option);
//        logger.info("All predicates : {}", allPredicates);
//        logger.info("Invalid predicate set : {}", invalidX);
//        logger.info("Valid predicate mapping : {}", validXRHSs);

        // prune invalid lattice nodes
        this.pruneLattice(invalidX, invalidXRHSs, validXRHSs, allPredicatesTemplate);
        /*
        else {
            logger.info("wrong option of top-K, plz select [original] or [anytime]");
            return null;
        }
         */
        // 1. scan all parent level with current pairs of relations
        int maxTupleIDInLevel = 0;
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            // expand each vertex with a new predicate
            // IBitSet k = entry.getKey();
            LatticeVertex lv = entry.getValue();
            if (this.ifAllConstantPredicates(lv.getPredicates())) {
                continue;
            }
            // check support K --- pruning
            if (invalidX.contains(lv.getPredicates().getBitset())) {
                continue;
            }
            ImmutablePair<Integer, Integer> tid_pair = lv.getLatestTupleIDs();
            maxTupleIDInLevel = Math.max(maxTupleIDInLevel, tid_pair.right);
            String r_1 = lv.getTIDRelationName(tid_pair.left);
            String r_2 = lv.getTIDRelationName(tid_pair.right);
//            logger.info("Latest relations : {} and {}", r_1, r_2);
            for (Predicate p : allPredicatesTemplate) {
//                logger.info("Scan predicate {}", p);
                if (!p.checkIfCorrectRelationName(r_1, r_2)) {
                    continue;
                }
//               logger.info("Skip relations ...");
//               if (lv.ifContain(p)) {
//                   continue;
//               }
                if (!p.isConstant()) {
                    // a new lattice vertex in the next level
                    Predicate newP = predicateProviderIndex.getPredicate(p, tid_pair.left, tid_pair.right);
                    if (lv.ifContain(newP)) {
                        continue;
                    }
                    if (!this.checkValidExtension(lv, newP)) {
                        continue;
                    }
//               logger.info("Add new predicate {}", newP);
                    LatticeVertex lv_child = new LatticeVertex(lv, newP);
                    // check support k
//                    boolean valid = this.checkValidSubSetX(lv_child, invalidX);
//                    if (valid && (!invalidX.contains(lv_child.getPredicates().getBitset()))) {
                        nextLevel.addLatticeVertex(lv_child);
//                    }
                } else {
                    // left
                    if (p.checkIfCorrectOneRelationName(r_1)) {
                        Predicate cp1 = predicateProviderIndex.getPredicate(p, tid_pair.left, tid_pair.left);
                        if (lv.ifContain(cp1)) {
                            continue;
                        }
                        if (!this.checkValidExtension(lv, cp1)) {
                            continue;
                        }
                        if (!this.checkValidConstantPredicateExtension(lv, cp1)) {
                            continue;
                        }
                        LatticeVertex lv_child = new LatticeVertex(lv, cp1);
//                        boolean valid = this.checkValidSubSetX(lv_child, invalidX);
//                        if (valid && (!invalidX.contains(lv_child.getPredicates().getBitset()))) {
                            nextLevel.addLatticeVertex(lv_child);
//                        }
                    }

                    // right
                    if (p.checkIfCorrectOneRelationName(r_2)) {
                        Predicate cp2 = predicateProviderIndex.getPredicate(p, tid_pair.right, tid_pair.right);
                        if (lv.ifContain(cp2)) {
                            continue;
                        }
                        if (!this.checkValidExtension(lv, cp2)) {
                            continue;
                        }
                        if (!this.checkValidConstantPredicateExtension(lv, cp2)) {
                            continue;
                        }
                        LatticeVertex lv_child_ = new LatticeVertex(lv, cp2);
//                        boolean valid = this.checkValidSubSetX(lv_child_, invalidX);
//                        if (valid && (!invalidX.contains(lv_child_.getPredicates().getBitset()))) {
                            nextLevel.addLatticeVertex(lv_child_);
//                        }
                    }

                }

            }
        }

//        logger.info("Expand First Step lattice : {}", nextLevel.printCurrents());

        // 2. scan all parent level and expand predicates with a new predicate containing one new tuple
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            LatticeVertex lv = entry.getValue();
            ImmutablePair<Integer, Integer> tupleIDPair = lv.getLatestTupleIDs();
            /*
            if (tupleIDPair.right != maxTupleIDInLevel) {
                continue;
            }

             */
            int new_tid = tupleIDPair.right + 1;
            // exceed the maximum number of tuples in a REE rule
            if (new_tid >= this.maxTupleNumPerRule) {
                continue;
            }
            for (int tid = 0; tid < new_tid; tid++) {
                String r_1 = lv.getTIDRelationName(tid);
                if (r_1 == null) {
                    continue;
                }
                for (Predicate p : allPredicatesTemplate) {
                    if (p.isConstant()) {
                        continue;
                    }
                    if (!p.checkIfCorrectOneRelationName(r_1)) {
                        continue;
                    }
                    /*
                    if (lv.ifContain(p)) {
                        continue;
                    }
                     */
                    // a new lattice vertex in the next level
                    Predicate newP = predicateProviderIndex.getPredicate(p, tid, new_tid);
                    if (lv.ifContain(newP)) {
                        continue;
                    }
                    if (!this.checkValidExtension(lv, newP)) {
                        continue;
                    }
                    LatticeVertex lv_child = new LatticeVertex(lv, newP);
                    // set new tid indices
                    lv_child.updateLatestTupleIDs(tid, new_tid);
                    // update latest relation names
                    lv_child.updateTIDRelationName(r_1, p.getOperand2().getColumn().getTableName());
                    if (!invalidX.contains(lv_child.getPredicates().getBitset())) {
                        nextLevel.addLatticeVertex(lv_child);
                    }
                }
            }
        }

//        logger.info("Expand Second Step lattice : {}", nextLevel.printCurrents());


        // 3. jump to the next tid pair <t, s>
//        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
//            LatticeVertex lv = entry.getValue();
//            ImmutablePair<Integer, Integer> tupleIDPair = lv.getLatestTupleIDs();
//            for (int tid = tupleIDPair.left + 1; tid < tupleIDPair.right; tid ++) {
//                String r_1 = lv.getTIDRelationName(tid);
//                String r_2 = lv.getTIDRelationName(tupleIDPair.right);
//                if (r_1 == null || r_2 == null) {
//                    continue;
//                }
//                for (Predicate p : allPredicates) {
//                    if (! p.checkIfCorrectRelationName(r_1, r_2)) {
//                        continue;
//                    }
//                    /*
//                    if (lv.ifContain(p)) {
//                        continue;
//                    }
//                     */
//                    // a new lattice vertex in the next level
//                    Predicate newP = predicateProviderIndex.getPredicate(p, tid, tupleIDPair.right);
//                    if (lv.ifContain(newP)) {
//                        continue;
//                    }
//                    if (! this.checkValidExtension(lv, newP)) {
//                        continue;
//                    }
//                    LatticeVertex lv_child = new LatticeVertex(lv, newP);
//                    // set new tid indices
//                    lv_child.updateLatestTupleIDs(tid, tupleIDPair.right);
//                    // update latest relation names
//                    lv_child.updateTIDRelationName(r_1, r_2);
//                    boolean valid = this.checkValidSubSetX(lv_child, invalidX);
//                    if (valid && (!invalidX.contains(lv_child.getPredicates().getBitset()))) {
//                        nextLevel.addLatticeVertex(lv_child);
//                    }
//
//                }
//            }
//        }
//        logger.info("Expand Third Step lattice : {}", nextLevel.printCurrents());

        return nextLevel;
    }

    /*
        test lattice, whether exist t.A = c ^ t.A = d
     */
    public void testDupConstantPredicates() {
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            HashMap<String, Integer> statistic = new HashMap<>();
            for (Predicate p : entry.getValue().getPredicates()) {
                if (p.isConstant()) {
                    String k = p.getOperand1().toString();
                    if (statistic.containsKey(k)) {

                    }
                }
            }

        }
    }


    /*
        collect a set of work units
     */
    public ArrayList<WorkUnit> generateWorkUnits() {
        ArrayList<WorkUnit> workUnits = new ArrayList<>();
        HashMap<IBitSet, WorkUnit> workUnitHashMap = new HashMap<>();
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            // generate a set of work units for each lattice node (vertex)
            ArrayList<WorkUnit> subWorkUnits = entry.getValue().generateWorkUnits();
            for (WorkUnit wu : subWorkUnits) {
                if (workUnitHashMap.containsKey(wu.getKey())) {
                    workUnitHashMap.get(wu.getKey()).unionRHSs(wu.getRHSs());
                } else {
                    workUnitHashMap.put(wu.getKey(), wu);
                }
            }
        }
        for (WorkUnit wu : workUnitHashMap.values()) {
            workUnits.add(wu);
        }
        return workUnits;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, latticeLevel);
        kryo.writeObject(output, maxTupleNumPerRule);
        kryo.writeObject(output, allLatticeVertexBits);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.latticeLevel = kryo.readObject(input, HashMap.class);
        this.maxTupleNumPerRule = kryo.readObject(input, Integer.class);
        this.allLatticeVertexBits = kryo.readObject(input, HashSet.class);
    }

    private static Logger logger = LoggerFactory.getLogger(Lattice.class);

}
