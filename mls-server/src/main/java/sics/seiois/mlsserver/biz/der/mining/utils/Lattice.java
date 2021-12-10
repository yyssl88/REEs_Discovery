package sics.seiois.mlsserver.biz.der.mining.utils;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.util.hash.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shapeless.ops.nat;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.ParallelRuleDiscovery;
import sics.seiois.mlsserver.biz.der.mining.ParallelRuleDiscoverySampling;

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
//        }
    }

    public void removeLatticeVertex(PredicateSet currPsel, Predicate newP) {
        // remove the LatticeVertex with X being {currPsel, newP} from current lattice.
        PredicateSet tmp_ps = new PredicateSet(currPsel);
        tmp_ps.add(newP);
        IBitSet key = tmp_ps.getBitset();
        if (this.latticeLevel.containsKey(key)) {
            this.latticeLevel.remove(key);
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
                // this.latticeLevel.remove(key);
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
        }
    }

    public void pruneLattice(HashSet<IBitSet> invalidX, HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                             HashMap<IBitSet, ArrayList<Predicate>> validXRHSs, List<Predicate> allPredicates) {
        HashSet<IBitSet> removedKeys = new HashSet<>();
        for (IBitSet key : this.latticeLevel.keySet()) {
            IBitSet t = this.latticeLevel.get(key).getPredicates().getBitset();
            if (invalidX.contains(t)) {
                // this.latticeLevel.remove(key);
                removedKeys.add(key);
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
                        this.latticeLevel.get(key).getRHSs().remove(cand);
                    }
                }
            }

            // further prune
            for (Predicate r : this.latticeLevel.get(key).getPredicates()) {
                PredicateSet temp = new PredicateSet(this.latticeLevel.get(key).getPredicates());
                temp.remove(r);
                if (validXRHSs.containsKey(temp.getBitset())) {
                    for (Predicate rhs : validXRHSs.get(temp.getBitset())) {
                        this.latticeLevel.get(key).getRHSs().remove(rhs);
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
        }
    }

    // early termination
    /*
        supp_ratios:  support ratios of last validation results
        current lattice node: L,  IBitSet of supp_ratios: L
     */
    private void pruneInterestingnessUB(Interestingness interestingness, double KthScore, HashMap<IBitSet, Double> supp_ratios) {
        for (IBitSet key : this.latticeLevel.keySet()) {
            LatticeVertex lv = this.latticeLevel.get(key);
            double ub = interestingness.computeUB(supp_ratios.get(lv.getPredicates().getBitset()), 1.0, lv.getPredicates(), null);
            if (ub < KthScore) {
                this.latticeLevel.remove(key);
            }
        }
    }

    // early termination
    /*
        supp_ratios:  support ratios of last current X
        current lattice node: L, IBitSet of supp_ratios: L - 1
     */
    public void pruneXInterestingnessUB(Interestingness interestingness, double KthScore, HashMap<PredicateSet, Double> supp_ratios) {
//        ArrayList<Double> ubScores = new ArrayList<>();
//        ArrayList<Double> nonZeroUBScores = new ArrayList<>();
        HashSet<IBitSet> removeKeys = new HashSet<>();
        for (IBitSet key : this.latticeLevel.keySet()) {
//            double curr_ub = 0.0;
            LatticeVertex lv = this.latticeLevel.get(key);
            PredicateSet current = lv.getPredicates();

            // first check all predicates in lv, prune all
            if (supp_ratios.containsKey(lv.getPredicates())) {
                PredicateSet tt = new PredicateSet(current);
                double ub = interestingness.computeUB(supp_ratios.get(lv.getPredicates()), 1.0, tt, null);
//                curr_ub = ub;
//                logger.info("#### KthScore: {}, ub: {}, curr_ub: {}", KthScore, ub, curr_ub);
                if (ub < KthScore) {
                    removeKeys.add(key);
                    continue;
                }
            }

            // loop all RHSs to check whether some of them can be pruned based on the interestingness UB
            for (Predicate rhs : lv.getRHSs()) {
                if (!current.containsPredicate(rhs)) {
                    continue;
                }
                PredicateSet tt = new PredicateSet(current);
                tt.remove(rhs);
                if (supp_ratios.containsKey(tt)) {
                    double ub = interestingness.computeUB(supp_ratios.get(tt), 1.0, tt, null);
//                    if (curr_ub == 0.0) {
//                        curr_ub = ub;
//                    } else {
//                        curr_ub = curr_ub < ub ? curr_ub : ub;
//                    }
//                    logger.info("#### KthScore: {}, ub: {}, curr_ub: {}", KthScore, ub, curr_ub);
                    if (ub < KthScore) {
                        // remove the RHS predicate (p_0) if it did not meet the value of UB
                        // lv.getRHSs().remove(rhs);

                        // if at least one RHS UB does not satisfy, remove the lattice vertex, -- new ADDED, need to re-test and re-think
                        removeKeys.add(key);
                        break;
                    }
                }
            }
//            ubScores.add(curr_ub);
//            if (curr_ub != 0.0) {
//                nonZeroUBScores.add(curr_ub);
//            }
            if (lv.getRHSs().size() <= 0) {
                // this.latticeLevel.remove(key);
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
                // this.latticeLevel.remove(key);
                removeKeys.add(key);
            }
        }
        // remove useless keys
        for (IBitSet key : removeKeys) {
            this.latticeLevel.remove(key);
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

    /*
        For lattice combination, e.g., AB + BC -> ABC
     */
    private boolean checkValidExtension(LatticeVertex lv, Predicate newP) {
        for (Predicate rp : lv.getPredicates()) {
            PredicateSet ps = new PredicateSet(lv.getPredicates());
            ps.remove(rp);
            ps.add(newP);
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
        for (Predicate p : ps) {
            if (!p.isConstant()) {
                return false;
            }
        }
        return true;
    }

    /*
        invalidX: predicates of lattice vertex whose support < k
        option: "original" and "anytime"
     */
    public Lattice generateNextLatticeLevel(List<Predicate> allPredicates, ArrayList<Predicate> allExistPredicates, HashSet<IBitSet> invalidX,
                                            HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                                            HashMap<IBitSet, ArrayList<Predicate>> validXRHSs,
                                            Interestingness interestingness, double KthScore, HashMap<PredicateSet, Double> suppRatios,
                                            PredicateProviderIndex predicateProviderIndex,
                                            String option, ArrayList<LatticeVertex> partialRules,
                                            int ifRL, int ifOnlineTrainRL, int ifOnlineTrainStage, boolean ifExistModel,
                                            String python_path, String RL_code_path,
                                            float lr, float rd, float eg, int rtr, int ms, int bs,
                                            String table_name) {
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
                    if (!this.checkValidExtension(lv, newP)) {
                        continue;
                    }
//               logger.info("Add new predicate {}", newP);
                    LatticeVertex lv_child = new LatticeVertex(lv, newP);
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
                            if (ifRL == 1 && ifExistModel) { // for RL
                                currPsel.add(lv.getPredicates());
                                newPredicates.add(cp1);
//                           newLatticeVertices.add(lv_child);
                            }
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

//        logger.info("Expand First Step lattice : {}", nextLevel.printCurrents());

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
                    LatticeVertex lv_child = new LatticeVertex(lv, newP);
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

        if (ifOnlineTrainRL == 0 && ifOnlineTrainStage == 1) {
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
                python_path, RL_code_path, lr, rd, eg, rtr, ms, bs, table_name);  // form: "1;0;1;1;0;1;..."
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
