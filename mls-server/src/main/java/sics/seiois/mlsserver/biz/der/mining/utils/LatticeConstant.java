package sics.seiois.mlsserver.biz.der.mining.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.ParallelRuleDiscoverySampling;
import sics.seiois.mlsserver.biz.der.mining.model.MLPFilterClassifier;

import java.util.*;

/*
    store the intermediate results of REE breadth-first search
 */
public class LatticeConstant implements KryoSerializable {

    private static final long serialVersionUID = 110209442793015102L;

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

    public ArrayList<LatticeConstant> splitLattice(int numOfLattice) {
        ArrayList<LatticeConstant> lattices = new ArrayList<>();
        ArrayList<IBitSet> keys = new ArrayList<>();
        HashSet<IBitSet> keysHash = new HashSet<>();
        for (IBitSet key : latticeLevel.keySet()) {
            keys.add(key);
            keysHash.add(key);
        }
        int num = keys.size();
        if (num <= numOfLattice) {
            for (IBitSet key : keys) {
                LatticeConstant lTemp = new LatticeConstant(this.maxTupleNumPerRule);
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
                LatticeConstant lTemp = new LatticeConstant(this.maxTupleNumPerRule);
                for (int j = begin; j < end; j++) {
                    lTemp.addLatticeVertex(this.latticeLevel.get(keys.get(j)));
                }
                lattices.add(lTemp);
            }
            // deal with the last one
            int begin = (numOfLattice - 1) * step;
            int end = num;
            LatticeConstant lTemp = new LatticeConstant(this.maxTupleNumPerRule);
            for (int j = begin; j < end; j++) {
                lTemp.addLatticeVertex(this.latticeLevel.get(keys.get(j)));
            }
            lattices.add(lTemp);
        }

        // set hash keys
        for (LatticeConstant lattice : lattices) {
            lattice.setAllLatticeVertexBits(keysHash);
        }
        return lattices;
    }


    public LatticeConstant() {
        // this.latticeLevel = new HashSet<>();
        this.latticeLevel = new HashMap<>();
    }

    public LatticeConstant(int maxT) {
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

    public void removeInvalidLatticeAndRHSs(LatticeConstant parent) {
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
            if (supp_ratios.containsKey(lv.getPredicates())) {
                PredicateSet tt = new PredicateSet(current);
                double ub = interestingness.computeUB(supp_ratios.get(lv.getPredicates()), 1.0, tt, null, topKOption);
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
                    double ub = interestingness.computeUB(supp_ratios.get(current), 1.0, tt, rhs, topKOption);
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
            pSet.remove(rhs);
            boolean f1 = false, f2 = false;
            if (suppRatios.containsKey(pSet)) {
                f1 = true;
                double UBScore = interestingness.computeUB(suppRatios.get(pSet), 1.0, pSet, rhs, topKOption);
                if (UBScore > kth) {
                    validRHSs.add(rhs);
                }
            }
            pSet.remove(newP);
            if (suppRatios.containsKey(pSet)) {
                f2 = true;
                double UBScore = interestingness.computeUB(suppRatios.get(pSet), 1.0, pSet, rhs, topKOption);
                if (UBScore > kth) {
                    validRHSs.add(rhs);
                }
            }

            // add newP
            pSet.add(newP);
            if (f1 == false && f2 == false) {
                double UBScore = interestingness.computeUB(1, 1.0, pSet, rhs, topKOption);
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


    public LatticeConstant generateNextLatticeLevel(List<Predicate> allPredicates, ArrayList<Predicate> allExistPredicates, HashSet<IBitSet> invalidX,
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

        LatticeConstant nextLevel = new LatticeConstant(this.maxTupleNumPerRule);

        // 1. scan all parent level with current pairs of relations
        int maxTupleIDInLevel = 0;
        for (Map.Entry<IBitSet, LatticeVertex> entry : this.latticeLevel.entrySet()) {
            LatticeVertex lv = entry.getValue();
            // check support K --- pruning
            if (invalidX.contains(lv.getPredicates().getBitset())) {
                continue;
            }
            String r_1 = lv.getTIDRelationName(0);
            for (Predicate p : allPredicates) {
                if (p.checkIfCorrectOneRelationName(r_1)) {
                    Predicate cp1 = predicateProviderIndex.getPredicate(p, 0, 0);
                    if (lv.ifContain(cp1)) {
                        continue;
                    }
                    if (!this.checkValidExtension(lv, cp1)) {
                        continue;
                    }
                    if (!this.checkValidConstantPredicateExtension(lv, cp1)) {
                        continue;
                    }

                    LatticeVertex lv_child = this.expandLatticeByTopK(lv, cp1, KthScore, interestingness, predicatesHashIDs, suppRatios, topKOption);
                    if (lv_child == null) {
                        continue;
                    }
                    nextLevel.addLatticeVertex(lv_child);

                }

            }
        }

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
        }
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

    private static Logger logger = LoggerFactory.getLogger(LatticeConstant.class);

}
