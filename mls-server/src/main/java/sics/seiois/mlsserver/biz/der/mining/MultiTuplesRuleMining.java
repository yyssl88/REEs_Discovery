package sics.seiois.mlsserver.biz.der.mining;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.REEFinderEvidSet;
import sics.seiois.mlsserver.biz.der.metanome.input.InputLight;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.utils.PLILight;

import java.util.*;

public class MultiTuplesRuleMining {
    private int max_num_tuples; // the maximum number of tuples a REE contains
    private InputLight inputLight;
    private long support;
    private float confidence;

    private long maxOneRelationNum;

    // tuple numbers of each relation
    private HashMap<String, Long> tupleNumberRelations;

    private long allCount;

    private static int BATCH_TUPLE_NUM = 1000;
    private static int ENUMERATE_RATIO = 100000;

    public MultiTuplesRuleMining(int max_num_tuples, InputLight inputLight, long support, float confidence,
                                 long maxOneRelationNum, long allCount, HashMap<String, Long> tupleNumberRelations) {
        this.max_num_tuples = max_num_tuples;
        this.inputLight = inputLight;
        this.support = support;
        this.confidence = confidence;
        this.maxOneRelationNum = maxOneRelationNum;
        this.allCount = allCount;
        this.tupleNumberRelations = tupleNumberRelations;
    }

    private ArrayList<ImmutablePair<Integer, Integer>> computeTupleIDsStart(ArrayList<Predicate> currentOneGroup) {

        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start = new ArrayList<>();
        for (int i = 0; i < currentOneGroup.size(); i++) {
            int t1 = this.inputLight.getTupleIDStart(currentOneGroup.get(i).getOperand1().getColumnLight().getTableName());
            int t2 = this.inputLight.getTupleIDStart(currentOneGroup.get(i).getOperand2().getColumnLight().getTableName());
            tuple_IDs_start.add(new ImmutablePair<>(t1, t2));
        }
        return tuple_IDs_start;
    }

    private ImmutablePair<Integer, Integer> computeTupleIDsStart(Predicate p) {

        int t1 = this.inputLight.getTupleIDStart(p.getOperand1().getColumnLight().getTableName());
        int t2 = this.inputLight.getTupleIDStart(p.getOperand2().getColumnLight().getTableName());

        return new ImmutablePair<Integer, Integer>(t1, t2);
    }

    private long checkOtherPredicates(int[] PIDs, int tid_left, int tid_right, ArrayList<Predicate> currentOneGroup, ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start,
                                      HashMap<Integer, ArrayList<Integer>> res, int t_left, int t_right, ArrayList<HashSet<Integer>> usedData) {
        long supp = 0;
        boolean f = true;
        for (int i = 0; i <currentOneGroup.size(); i++) {
            if (! currentOneGroup.get(i).calculateTp(PIDs[t_left], PIDs[t_right], tid_left, tid_right,
                    tuple_IDs_start.get(i).left, tuple_IDs_start.get(i).right)) {
                f = false;
                break;
            }
        }
        if (f) {
            supp ++;
            if (res.containsKey(tid_left)) {
                res.get(tid_left).add(tid_right);
            } else {
                ArrayList<Integer> temp = new ArrayList<>();
                temp.add(tid_right);
                res.put(tid_left, temp);
            }
            usedData.get(t_left).add(tid_left);
            usedData.get(t_right).add(tid_right);
        }
        return supp;
    }

    public ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long > findResultsOneGroup_(ArrayList<Predicate> currentOneGroup,
                                                                                           int tid_left, int tid_right,
                                                                                           int[] PIDs,
                                                                                           ArrayList<HashSet<Integer>> usedData,
                                                                                           ArrayList<Boolean> usedTids) {
        HashMap<Integer, ArrayList<Integer>> res = new HashMap<>();
        if (currentOneGroup.size() == 0 || currentOneGroup == null) {
            return null;
        }

        Predicate minP = null;
        long minSupport = Long.MAX_VALUE;
        int minScript = 0;
        for (int i = 0; i < currentOneGroup.size(); i++) {
            Predicate p = currentOneGroup.get(i);
            if (p.isConstant()) {
                continue;
            }
            if (minSupport > p.getSupport()) {
                minP = p;
                minSupport = p.getSupport();
                minScript = i;
            }
        }
        log.info("The minimal SUPPORT ### : {} with Current X {}", minSupport, currentOneGroup);

        // check returned support
        long maxCount = maxOneRelationNum * maxOneRelationNum - maxOneRelationNum;
        double supp_ratio = 0.1;
        if (currentOneGroup == null || currentOneGroup.size() == 0 || minSupport < 0 || minSupport > supp_ratio * maxCount) {
            return null;
        }

        // if a predicate contains enumerate columns and its tuple ID is not <t_0, t_1>, then terminate
        //if (tid_left != 0 && tid_right != 1) {
        if (true) {
            long uniqueNum1 = minP.getOperand1().getColumnLight().getUniqueConstantNumber();
            long countR1 = this.tupleNumberRelations.get(minP.getOperand1().getColumnLight().getTableName());
            long uniqueNum2 = minP.getOperand2().getColumnLight().getUniqueConstantNumber();
            long countR2 = this.tupleNumberRelations.get(minP.getOperand2().getColumnLight().getTableName());
            if ( (uniqueNum1 <= countR1 * 1.0 / ENUMERATE_RATIO) || (uniqueNum2 <= countR2 * 1.0 / ENUMERATE_RATIO) ) {
                return null;
            }
            // if (tid_left != 0 && tid_right != 1) {
//            if (true) {
//                if (minP.getSupport() > maxOneRelationNum * 5000) {
//                    return null;
//                }
//            }
//            if (tid_left != 0 && tid_right != 1) {
//                if (minP.getSupport() > maxOneRelationNum * 10000) {
//                    return null;
//                }
//            }
        }

        currentOneGroup.remove(minScript);
        // ArrayList<ImmutablePair<Integer, Integer>> minList = minP.extractBiList();

        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start = this.computeTupleIDsStart(currentOneGroup);

        long supp = 0;
        if (minP.isML()) {
            ArrayList<ImmutablePair<Integer, Integer>> minList = minP.getMlOnePredicate();
            int beginTid1 = minP.getOperand1().getColumnLight().getTidsInterval(PIDs[tid_left]).left;
            int endTid1 = minP.getOperand1().getColumnLight().getTidsInterval(PIDs[tid_left]).right;
            int beginTid2 = minP.getOperand2().getColumnLight().getTidsInterval(PIDs[tid_right]).left;
            int endTid2 = minP.getOperand2().getColumnLight().getTidsInterval(PIDs[tid_right]).right;
            if (minList == null) {
                return null;
            }
            // filter
            for (ImmutablePair<Integer, Integer> pair : minList) {
                if (pair.left >= pair.right || pair.left < beginTid1 || pair.left >= endTid1 || pair.right < beginTid2 || pair.right >= endTid2) {
                    continue;
                }

                if (usedTids.get(tid_left) == Boolean.TRUE && (! usedData.get(tid_left).contains(pair.left))) {
                    continue;
                }

                if (usedTids.get(tid_right) == Boolean.TRUE && (! usedData.get(tid_right).contains(pair.right))) {
                    continue;
                }

                supp += this.checkOtherPredicates(PIDs, pair.left, pair.right, currentOneGroup, tuple_IDs_start, res, tid_left, tid_right,
                        usedData);

            }

        } else if (minP.isConstant()) {
            // throw new Exception("should not be constant predicate");
            return null;
        } else {

            PLILight pliPivot = minP.getOperand1().getColumnLight().getPliLight(PIDs[tid_left]);
            PLILight pliProbe = minP.getOperand2().getColumnLight().getPliLight(PIDs[tid_right]); // probing PLI
            if(pliPivot == null || pliProbe == null) {
                return null;
            }
            Collection<Integer> valuesPivot = pliPivot.getValues();
            for (Integer vPivot : valuesPivot) {
                if (vPivot == -1) continue;
                List<Integer> tidsProbe = pliProbe.getTpIDsForValue(vPivot);
                if (tidsProbe != null) {
                    List<Integer> tidsPivot = pliPivot.getTpIDsForValue(vPivot);

                    for (Integer tidPivot : tidsPivot) {
                        for (Integer tidProbe : tidsProbe) {
                            if (tidPivot >= tidProbe) {
                                continue;
                            }

                            if (usedTids.get(tid_left) == Boolean.TRUE && (! usedData.get(tid_left).contains(tidPivot))) {
                                continue;
                            }

                            if (usedTids.get(tid_right) == Boolean.TRUE && (! usedData.get(tid_right).contains(tidProbe))) {
                                continue;
                            }

                            supp += this.checkOtherPredicates(PIDs, tidPivot, tidProbe, currentOneGroup, tuple_IDs_start, res, tid_left, tid_right, usedData);

                        }
                    }

                }
            }
        }

        return new ImmutablePair<>(res, supp);
    }

    public ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long > findResultsOneGroup_(ArrayList<Predicate> currentOneGroup_,
                                                                                           int tid_left, int tid_right,
                                                                                           int[] PIDs,
                                                                                           ArrayList<HashSet<Integer>> usedData,
                                                                                           ArrayList<Boolean> usedTids,
                                                                                           HashSet<Integer> validConstantTIDs_left,
                                                                                           HashSet<Integer> validConstantTIDs_right) {
        // copy; in case that some elements are deleted
        ArrayList<Predicate> currentOneGroup = new ArrayList<>();
        for (Predicate p : currentOneGroup_) {
            currentOneGroup.add(p);
        }

        HashMap<Integer, ArrayList<Integer>> res = new HashMap<>();
        if (currentOneGroup.size() == 0 || currentOneGroup == null) {
            return null;
        }

        Predicate minP = null;
        long minSupport = Long.MAX_VALUE;
        int minScript = 0;
        for (int i = 0; i < currentOneGroup.size(); i++) {
            Predicate p = currentOneGroup.get(i);
            if (p.isConstant()) {
                continue;
            }
            if (minSupport > p.getSupport()) {
                minP = p;
                minSupport = p.getSupport();
                minScript = i;
            }
        }
        log.info("The minimal SUPPORT ### : {} with Current X {}", minSupport, currentOneGroup);

        // check returned support
        long maxCount = maxOneRelationNum * maxOneRelationNum - maxOneRelationNum;
        double supp_ratio = 0.1;
        if (currentOneGroup == null || currentOneGroup.size() == 0 || minSupport < 0 || minSupport > supp_ratio * maxCount) {
//        if (currentOneGroup == null || currentOneGroup.size() == 0 || minSupport < 0) {
            return null;
        }

        // if a predicate contains enumerate columns and its tuple ID is not <t_0, t_1>, then terminate
        //if (tid_left != 0 && tid_right != 1) {
        if (true) {
            long uniqueNum1 = minP.getOperand1().getColumnLight().getUniqueConstantNumber();
            long countR1 = this.tupleNumberRelations.get(minP.getOperand1().getColumnLight().getTableName());
            long uniqueNum2 = minP.getOperand2().getColumnLight().getUniqueConstantNumber();
            long countR2 = this.tupleNumberRelations.get(minP.getOperand2().getColumnLight().getTableName());
//            if ( (uniqueNum1 <= countR1 * 1.0 / ENUMERATE_RATIO) || (uniqueNum2 <= countR2 * 1.0 / ENUMERATE_RATIO) ) {
//                return null;
//            }
            // if (tid_left != 0 && tid_right != 1) {
//            if (true) {
//                if (minP.getSupport() > maxOneRelationNum * 5000) {
//                    return null;
//                }
//            }
//            if (tid_left != 0 && tid_right != 1) {
//                if (minP.getSupport() > maxOneRelationNum * 10000) {
//                    return null;
//                }
//            }
        }

        currentOneGroup.remove(minScript);
        // ArrayList<ImmutablePair<Integer, Integer>> minList = minP.extractBiList();

        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start = this.computeTupleIDsStart(currentOneGroup);

        long supp = 0;
        if (minP.isML()) {
            ArrayList<ImmutablePair<Integer, Integer>> minList = minP.getMlOnePredicate();
            int beginTid1 = minP.getOperand1().getColumnLight().getTidsInterval(PIDs[tid_left]).left;
            int endTid1 = minP.getOperand1().getColumnLight().getTidsInterval(PIDs[tid_left]).right;
            int beginTid2 = minP.getOperand2().getColumnLight().getTidsInterval(PIDs[tid_right]).left;
            int endTid2 = minP.getOperand2().getColumnLight().getTidsInterval(PIDs[tid_right]).right;
            if (minList == null) {
                return null;
            }
            // filter
            for (ImmutablePair<Integer, Integer> pair : minList) {
                if (pair.left >= pair.right || pair.left < beginTid1 || pair.left >= endTid1 || pair.right < beginTid2 || pair.right >= endTid2) {
                    continue;
                }

                if (usedTids.get(tid_left) == Boolean.TRUE && (! usedData.get(tid_left).contains(pair.left))) {
                    continue;
                }

                if (usedTids.get(tid_right) == Boolean.TRUE && (! usedData.get(tid_right).contains(pair.right))) {
                    continue;
                }

                // filter --- valid tids satisfying constant predicates
                if (validConstantTIDs_left != null && (! validConstantTIDs_left.contains(pair.left))) {
                    continue;
                }
                if (validConstantTIDs_right != null && (! validConstantTIDs_right.contains(pair.right))) {
                    continue;
                }

                supp += this.checkOtherPredicates(PIDs, pair.left, pair.right, currentOneGroup, tuple_IDs_start, res, tid_left, tid_right,
                        usedData);

            }

        } else if (minP.isConstant()) {
            // throw new Exception("should not be constant predicate");
            return null;
        } else {

            PLILight pliPivot = minP.getOperand1().getColumnLight().getPliLight(PIDs[tid_left]);
            PLILight pliProbe = minP.getOperand2().getColumnLight().getPliLight(PIDs[tid_right]); // probing PLI
            if(pliPivot == null || pliProbe == null) {
                return null;
            }
            Collection<Integer> valuesPivot = pliPivot.getValues();
            for (Integer vPivot : valuesPivot) {
                if (vPivot == -1) continue;
                List<Integer> tidsProbe = pliProbe.getTpIDsForValue(vPivot);
                if (tidsProbe != null) {
                    List<Integer> tidsPivot = pliPivot.getTpIDsForValue(vPivot);

                    for (Integer tidPivot : tidsPivot) {
                        // filter --- valid TIDs
                        if (validConstantTIDs_left != null && (! validConstantTIDs_left.contains(tidPivot))) {
                            continue;
                        }

                        for (Integer tidProbe : tidsProbe) {
                            if (tidPivot >= tidProbe) {
                                continue;
                            }
                            // filter --- valid TIDs
                            if (validConstantTIDs_right != null && (! validConstantTIDs_right.contains(tidProbe))) {
                                continue;
                            }

                            if (usedTids.get(tid_left) == Boolean.TRUE && (! usedData.get(tid_left).contains(tidPivot))) {
                                continue;
                            }

                            if (usedTids.get(tid_right) == Boolean.TRUE && (! usedData.get(tid_right).contains(tidProbe))) {
                                continue;
                            }

                            supp += this.checkOtherPredicates(PIDs, tidPivot, tidProbe, currentOneGroup, tuple_IDs_start, res, tid_left, tid_right, usedData);

                        }
                    }

                }
            }
        }

        return new ImmutablePair<>(res, supp);
    }


    /*
        all predicates of RHSs are <t_0, t_1> or <t_0, t_0>
     */
    private void uniVerify(int[] PIDs, Long[] counts, HashSet<Integer> resultsX, ArrayList<Predicate> uniPredicateRHSs) {

        int t_left = 0;
        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start_rhs = this.computeTupleIDsStart(uniPredicateRHSs);
        for (Integer tid : resultsX) {
            for (int i = 0; i < uniPredicateRHSs.size(); i++) {

                if (uniPredicateRHSs.get(i).calculateTp(PIDs[t_left], PIDs[t_left], tid, -1, tuple_IDs_start_rhs.get(i).left, -1)) {
                    counts[i] ++;
                }
            }
        }
    }

    private void biVerify(int[] PIDs, Long[] counts, ArrayList<ImmutablePair<Integer, Integer>> resultsX, ArrayList<Predicate> biPredicateRHSs) {

        int t_left = 0, t_right = 1;
        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start_rhs = this.computeTupleIDsStart(biPredicateRHSs);
        for (ImmutablePair<Integer, Integer> pair : resultsX) {
            for (int i = 0; i < biPredicateRHSs.size(); i++) {
                if (biPredicateRHSs.get(i).calculateTp(PIDs[t_left], PIDs[t_right], pair.left, pair.right, tuple_IDs_start_rhs.get(i).left, tuple_IDs_start_rhs.get(i).right)) {
                    counts[i] ++;
                }
            }
        }
    }


    /******************************************************************************************************************************
     *  new validation with [start, end]
     ******************************************************************************************************************************/

    private HashSet<Integer> findResultsOneConstantPredicatesGroup(int tuple_variable, int[] PIDs, ArrayList<Predicate> constantPredicatesPrefix) {
        HashSet<Integer> res = new HashSet<>();

        // 1. find the predicate of the smallest support in each group
        Predicate minP = null;
        int minSc = -1;
        long minSupport = Long.MAX_VALUE;
        for (int i = 0; i < constantPredicatesPrefix.size(); i++) {
            Predicate cp = constantPredicatesPrefix.get(i);
            if (minSupport > cp.getSupport()) {
                minP = cp;
                minSupport = cp.getSupport();
                minSc = i;
            }
        }

        int pid = PIDs[tuple_variable];
        ImmutablePair<Integer, Integer> tuple_IDs_start = this.computeTupleIDsStart(minP);

        constantPredicatesPrefix.remove(minSc);
        // 2. retrieve the list
        PLILight pli = minP.getOperand1().getColumnLight().getPliLight(PIDs[tuple_variable]);
        if (pli == null) {
            return null;
        }
        List<Integer> _list = pli.getTpIDsForValue(minP.getConstantInt());
        for (Integer tid : _list) {
            boolean f = true;
            // check other constant predicates of the same tuple_variable
            for (Predicate cp_ : constantPredicatesPrefix) {
                if (! cp_.calculateTpConstant(pid, tid, tuple_IDs_start.left)) {
                    f = false;
                    break;
                }
            }
            if (f) {
                res.add(tid);
            }
        }
        return res;

    }

    private HashMap<Integer, HashSet<Integer>> retrieveSatisfiedConstantTIDs(int[] PIDs, ArrayList<Predicate> constantPredicatesPrefix) {
        HashMap<Integer, HashSet<Integer>> res = new HashMap<>();
        // 1. group different constant predicate according to tuple variables
        HashMap<Integer, ArrayList<Predicate>> constantPredicatesPrefixGroup = new HashMap<>();
        for (Predicate cp : constantPredicatesPrefix) {
            int t_var = cp.getIndex1();
            if (constantPredicatesPrefixGroup.containsKey(t_var)) {
                constantPredicatesPrefixGroup.get(t_var).add(cp);
            } else {
                ArrayList<Predicate> _list = new ArrayList<>();
                _list.add(cp);
                constantPredicatesPrefixGroup.put(t_var, _list);
            }
        }

        // 2. find valid TIDs
        for (Map.Entry<Integer, ArrayList<Predicate>> entry : constantPredicatesPrefixGroup.entrySet()) {
            res.put(entry.getKey(), this.findResultsOneConstantPredicatesGroup(entry.getKey(), PIDs, entry.getValue()));
        }

        return res;
    }

    /*
        check whether X -> p_0 has duplicate constant predicates, e.g., t.A = c, t.A = d
        return (1) TRUE -> no duplicates, (2) FALSE -> has duplicates, invalid X
     */
    private boolean checkIfDupConstantPredicates(ArrayList<Predicate> currentX) {
        HashMap<String, Integer> stat = new HashMap<>();
        for (Predicate p : currentX) {
            if (!p.isConstant()) {
                continue;
            }
            String k = p.getOperand1().toString();
            if (stat.containsKey(k)) {
                return false;
            } else {
                stat.put(k, 1);
            }
        }
        return true;
    }

    public List<Message> validation_new(ArrayList<Predicate> currentSet, PredicateSet rhss, int[] pids) {

//        // only for test
//        if (! currentSet.get(0).toString().trim().equals("airports.t0.keywords == airports.t1.keywords")) {
//            return new ArrayList<>();
//        }

        List<Message> messages = new ArrayList<>();
        log.info("Validation CurrentX : {}, RHSs : {}", currentSet, rhss);

        // first check whether currentSet -> rhss is valid or not
        if (! this.checkIfDupConstantPredicates(currentSet)) {
            Message message_ = new Message(currentSet, 0, 0, 0);
            for (Predicate rhs : rhss) {
                message_.addInValidRHS(rhs);
            }
            messages.add(message_);
            return messages;
        }

        // select constant predicates
        ArrayList<Predicate> constantPredicates = new ArrayList<>();
        HashMap<Integer, ArrayList<Predicate>> predicateGroups = new HashMap<>();
        ArrayList<ImmutablePair<Integer, Integer>> keys = new ArrayList<>();
        ArrayList<Long> keysFreqs_ = new ArrayList<>();

        String relationT0 = null;
        int maxtuple = 0;
        for (Predicate p : currentSet) {
            // check the relation of t_0
            if (p.getIndex1() == 0) {
                relationT0 = p.getOperand1().getColumn().getTableName();
            }
            maxtuple = Math.max(maxtuple, Math.max(p.getIndex1(), p.getIndex2()));
            if (p.isConstant()) {
                constantPredicates.add(p);
            } else {
                Integer key = MultiTuplesOrder.encode(new ImmutablePair<>(p.getIndex1(), p.getIndex2()));
                if (! predicateGroups.containsKey(key)) {
                    ArrayList<Predicate> group = new ArrayList<>();
                    group.add(p);
                    predicateGroups.put(key, group);
                } else {
                    predicateGroups.get(key).add(p);
                }
            }
        }
//        for (Predicate p : constantPredicates) {
//            int index = p.getIndex1();
//            for (int t = 0; t <= maxtuple; t++) {
//                if (index == t) {
//                    continue;
//                }
//                int min_t = Math.min(index, t);
//                int max_t = Math.max(index, t);
//                int key = MultiTuplesOrder.encode(new ImmutablePair<>(min_t, max_t));
//                if (predicateGroups.containsKey(key)) {
//                    predicateGroups.get(key).add(p);
//                }
//            }
//        }

        // retrieve valid TIDs of predicatesPrefix
        HashMap<Integer, HashSet<Integer>> validConstantTIDs = this.retrieveSatisfiedConstantTIDs(pids, constantPredicates);

        // record the initial keys and keysFreq
        for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
            keys.add(MultiTuplesOrder.decode(entry.getKey()));
            keysFreqs_.add(0L);
        }
        // rearrange keys
        MultiTuplesOrder multiTuplesOrder_new = new MultiTuplesOrder(keys, keysFreqs_);
        ArrayList<ImmutablePair<Integer, Integer>> keys_new = multiTuplesOrder_new.rearrangeKeys();

        // get the tuple number of relationT0
        long countT0 = this.tupleNumberRelations.get(relationT0);
        boolean ifTermin = true;
        for (String r : this.tupleNumberRelations.keySet()) {
            if (r.equals(relationT0)) {
                continue;
            }
            if (countT0 < this.tupleNumberRelations.get(r)) {
                ifTermin = false;
            }
        }
        if (this.tupleNumberRelations.size() > 1 && ifTermin) {
            Message message_ = new Message(currentSet, 0, 0, 0);
            for (Predicate rhs : rhss) {
                message_.addInValidRHS(rhs);
            }
            messages.add(message_);
            return messages;
        }

        // only consider t_0
        int dataNumT0 = 0;
        int pidt0 = pids[0];
        for (Predicate p : currentSet) {
            if (p.getIndex1() == 0) {
                int b = p.getOperand1().getColumnLight().getTidsInterval(pidt0).left;
                int e = p.getOperand1().getColumnLight().getTidsInterval(pidt0).right;
                countT0 = e - b;
                break;
            }
        }

        // confirm and split RHSs
        // extract constant predicates
        ArrayList<Predicate> constantPs0 = new ArrayList<>();
        ArrayList<Predicate> constantPs1 = new ArrayList<>();
        ArrayList<Predicate> biPredicates = new ArrayList<>();
        for (Predicate p : rhss) {
            if (p.isConstant()) {
                if (p.getIndex1() == 0) {
                    constantPs0.add(p);
                } else if (p.getIndex1() == 1) {
                    constantPs1.add(p);
                }
            } else {
                biPredicates.add(p);
            }
        }

        HashSet<Predicate> invalidRHSs = new HashSet<>();

        for (int i = 0; i < biPredicates.size(); i++) {
            Predicate p = biPredicates.get(i);
            if (p.getSupport() < this.support) {
                // invalid RHS
                invalidRHSs.add(p);
                biPredicates.remove(i);
            }
        }

        for (int i = 0; i < constantPs0.size(); i++) {
            Predicate p = constantPs0.get(i);
            if (p.getSupport() * this.maxOneRelationNum < this.support) {
                invalidRHSs.add(p);
                constantPs0.remove(i);
            }
        }

        for (int i = 0; i < constantPs1.size(); i++) {
            Predicate p = constantPs1.get(i);
            if (p.getSupport() * this.maxOneRelationNum < this.support) {
                invalidRHSs.add(p);
                constantPs1.remove(i);
            }
        }

        int batch_num = (int)(countT0 / BATCH_TUPLE_NUM) + 1;

        long supportXAll = 0L;
        long supportCP0 = 0L;
        long supportCP1 = 0L;
        Long[] countsBiRHSs = new Long[biPredicates.size()];
        for (int i = 0; i <countsBiRHSs.length; i++) countsBiRHSs[i] = 0L;
        Long[] countsUniRHSs0 = new Long[constantPs0.size()];
        for (int i = 0; i <countsUniRHSs0.length; i++) countsUniRHSs0[i] = 0L;
        Long[] countsUniRHSs1 = new Long[constantPs1.size()];
        for (int i = 0; i <countsUniRHSs1.length; i++) countsUniRHSs1[i] = 0L;


        for (int batch_id = 0; batch_id < batch_num; batch_id++) {

            boolean flag = true;
            long tupleIDs_start_temp = batch_id * BATCH_TUPLE_NUM;
            long tupleIDs_end_temp = Math.min((batch_id + 1) * BATCH_TUPLE_NUM, countT0);
            long tupleIDs_start = tupleIDs_start_temp + this.inputLight.getTupleIDStart(relationT0);
            long tupleIDs_end = tupleIDs_end_temp + this.inputLight.getTupleIDStart(relationT0);

            // process data in a batch mode
            ArrayList<Boolean> usedTids = new ArrayList<>();
            usedTids.add(Boolean.TRUE);
            for (int t = 1; t <= maxtuple; t++) {
                usedTids.add(Boolean.FALSE);
            }
            ArrayList<HashSet<Integer>> usedData = new ArrayList<>();
            for (int t = 0; t <= maxtuple; t++) {
                usedData.add(new HashSet<>());
            }
            for (int tid = (int)tupleIDs_start; tid < tupleIDs_end; tid++) {
                usedData.get(0).add(tid);
            }


            // calculate the intersection and result for each group

            ArrayList<ImmutablePair<Integer, Integer>> keysTid = new ArrayList<>();
            ArrayList<Long> keysFreq = new ArrayList<>();
            HashMap<Integer, ArrayList<Integer>> beginTuplePairs = null;
            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict = new HashMap<>();
//        log.info("Predicate group : {}", predicateGroups);
            for (ImmutablePair<Integer, Integer> tid_pair : keys_new) {
                // for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
                int kk = MultiTuplesOrder.encode(tid_pair);
                ArrayList<Predicate> group = predicateGroups.get(kk);
                // ImmutablePair<Integer, Integer> tid_pair = MultiTuplesOrder.decode(entry.getKey());

                HashSet<Integer> validConstantTIDs_left = validConstantTIDs.get(tid_pair.left);
                HashSet<Integer> validConstantTIDs_right = validConstantTIDs.get(tid_pair.right);

                ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long> dataOneGroup =
                        this.findResultsOneGroup_(group, tid_pair.left, tid_pair.right, pids, usedData, usedTids,
                                validConstantTIDs_left, validConstantTIDs_right);
                // update usedTids
                usedTids.set(tid_pair.left, Boolean.TRUE);
                usedTids.set(tid_pair.right, Boolean.TRUE);

                if (dataOneGroup == null || dataOneGroup.left.size() <= 0) {
                    flag = false;
                    break;
                }
                // the first case
//            log.info("Predicate tuple ID pair : {}", entry.getKey());
                if (kk == MultiTuplesOrder.encode(new ImmutablePair<>(0, 1))) {
                    beginTuplePairs = dataOneGroup.left;
                } else {
                    datadict.put(kk, dataOneGroup.left);
                }

                keysFreq.add(dataOneGroup.right);
                keysTid.add(tid_pair);
            }
            if (flag == false) {
                // heuristic filter

//                /*****************************************
//                 * ONLY for heuristic
//                 */
//                // for very very large predicate group of LARGE support
//                supportXAll += support * support;
//                supportCP0 += 0.1 * BATCH_TUPLE_NUM * maxOneRelationNum;
//                supportCP1 += 0.1 * BATCH_TUPLE_NUM * maxOneRelationNum;
//                for (int i = 0; i < biPredicates.size(); i++) {
//                    countsBiRHSs[i] = biPredicates.get(i).getSupport();
//                }
//                for (int i = 0; i < constantPs0.size(); i++) {
//                    countsUniRHSs0[i] = constantPs0.get(i).getSupport();
//                }
//                for (int i = 0; i < constantPs1.size(); i++) {
//                    countsUniRHSs1[i] = constantPs1.get(i).getSupport();
//                }
//                /**********************
//
//                 ****************************************************/

                continue;
            }
            MultiTuplesOrder multiTuplesOrder = new MultiTuplesOrder(keysTid, keysFreq);
            ArrayList<ImmutablePair<Integer, Integer>> keys_new_new = multiTuplesOrder.rearrangeKeys();
            // retrieve tuple pairs of X
            ArrayList<ImmutablePair<Integer, Integer>> resultsX = multiTuplesOrder.joinAll(beginTuplePairs, keys_new_new, datadict);

//        log.info("First List : {}", beginTuplePairs);
//        log.info("Final List : {}", resultsX);

            supportXAll += resultsX.size();

            // extract lists for constant predicates
            HashSet<Integer> resultsXCP0 = new HashSet<>();
            HashSet<Integer> resultsXCP1 = new HashSet<>();
            for (ImmutablePair<Integer, Integer> pair : resultsX) {
                resultsXCP0.add(pair.left);
                resultsXCP1.add(pair.right);
            }

            supportCP0 += resultsXCP0.size();
            supportCP1 += resultsXCP1.size();

            biVerify(pids, countsBiRHSs, resultsX, biPredicates);
//        log.info("Mining after biVerify : {}, biPredicates : {}", message, biPredicates);
            uniVerify(pids, countsUniRHSs0, resultsXCP0, constantPs0);
            uniVerify(pids, countsUniRHSs1, resultsXCP1, constantPs1);
        }

        // put together
//        if (supportXAll < this.support) {
//            // all RHSs with currentList are invalid, add all
//            Message message_ = new Message(currentSet, supportXAll, supportCP0, supportCP1);
//            for (Predicate rhs : rhss) {
//                message_.addInValidRHS(rhs);
//            }
//            messages.add(message_);
//            return messages;
//        }

        Message message = new Message(currentSet, supportXAll, supportCP0, supportCP1);
        // add invalid RHSs
        for (Predicate p : invalidRHSs) {
            message.addInValidRHS(p);
        }

        message.updateAllCurrentRHSsSupport(countsBiRHSs, biPredicates);
        message.updateAllCurrentRHSsSupport(countsUniRHSs0, constantPs0);
        message.updateAllCurrentRHSsSupport(countsUniRHSs1, constantPs1);
        message.updateAllCurrentRHSsSupport(invalidRHSs);

        // uniStatistic(message, countsUniRHSs0, constantPs0, supportXAll);
        // uniStatistic(message, countsUniRHSs1, constantPs1, supportXAll);
        // biStatistic(message, countsBiRHSs, biPredicates, supportXAll);

        // add into a list of messages
        messages.add(message);
        return messages;
    }


    /***************************************************************************
     *
     * @param currentSet
     * @param rhss: all predicates in rhss have the same tuple id, e.g., <t_0, t_1> or <t_0>, or <t_0, t_2>
     *            Constant predicate ONLY contains <t_0>
     * @param pids
     * @return
     */
    public List<Message> validation_complex(ArrayList<Predicate> currentSet, PredicateSet rhss, int[] pids) {

        // first confirm the targetTID along with t_0 (0)
        int targetTID = 1;
        for (Predicate rhs : rhss) {
            if (!rhs.isConstant()) {
                targetTID = rhs.getIndex2();
                break;
            }
        }

        List<Message> messages = new ArrayList<>();
        log.info("Validation CurrentX : {}, RHSs : {}", currentSet, rhss);
        // select constant predicates
        ArrayList<Predicate> constantPredicates = new ArrayList<>();
        HashMap<Integer, ArrayList<Predicate>> predicateGroups = new HashMap<>();
        ArrayList<ImmutablePair<Integer, Integer>> keys = new ArrayList<>();
        ArrayList<Long> keysFreqs_ = new ArrayList<>();

        String relationT0 = null;
        int maxtuple = 0;
        for (Predicate p : currentSet) {
            // check the relation of t_0
            if (p.getIndex1() == 0) {
                relationT0 = p.getOperand1().getColumn().getTableName();
            }
            maxtuple = Math.max(maxtuple, Math.max(p.getIndex1(), p.getIndex2()));
            if (p.isConstant()) {
                constantPredicates.add(p);
            } else {
                Integer key = MultiTuplesOrder.encode(new ImmutablePair<>(p.getIndex1(), p.getIndex2()));
                if (! predicateGroups.containsKey(key)) {
                    ArrayList<Predicate> group = new ArrayList<>();
                    group.add(p);
                    predicateGroups.put(key, group);
                } else {
                    predicateGroups.get(key).add(p);
                }
            }
        }
        for (Predicate p : constantPredicates) {
            int index = p.getIndex1();
            for (int t = 0; t <= maxtuple; t++) {
                if (index == t) {
                    continue;
                }
                int min_t = Math.min(index, t);
                int max_t = Math.max(index, t);
                int key = MultiTuplesOrder.encode(new ImmutablePair<>(min_t, max_t));
                if (predicateGroups.containsKey(key)) {
                    predicateGroups.get(key).add(p);
                }
            }
        }

        // record the initial keys and keysFreq
        for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
            keys.add(MultiTuplesOrder.decode(entry.getKey()));
            keysFreqs_.add(0L);
        }
        // rearrange keys
        MultiTuplesOrder multiTuplesOrder_new = new MultiTuplesOrder(keys, keysFreqs_);
        ArrayList<ImmutablePair<Integer, Integer>> keys_new = multiTuplesOrder_new.rearrangeKeys();

        // get the tuple number of relationT0
        long countT0 = this.tupleNumberRelations.get(relationT0);
        boolean ifTermin = true;
        for (String r : this.tupleNumberRelations.keySet()) {
            if (r.equals(relationT0)) {
                continue;
            }
            if (countT0 < this.tupleNumberRelations.get(r)) {
                ifTermin = false;
            }
        }
        if (this.tupleNumberRelations.size() > 1 && ifTermin) {
            Message message_ = new Message(currentSet, 0, 0, 0);
            for (Predicate rhs : rhss) {
                message_.addInValidRHS(rhs);
            }
            messages.add(message_);
            return messages;
        }

        // only consider t_0
        int dataNumT0 = 0;
        int pidt0 = pids[0];
        for (Predicate p : currentSet) {
            if (p.getIndex1() == 0) {
                int b = p.getOperand1().getColumnLight().getTidsInterval(pidt0).left;
                int e = p.getOperand1().getColumnLight().getTidsInterval(pidt0).right;
                countT0 = e - b;
                break;
            }
        }

        // confirm and split RHSs
        // extract constant predicates
        ArrayList<Predicate> constantPs0 = new ArrayList<>();
        ArrayList<Predicate> constantPs1 = new ArrayList<>();
        ArrayList<Predicate> biPredicates = new ArrayList<>();
        for (Predicate p : rhss) {
            if (p.isConstant()) {
                if (p.getIndex1() == 0) {
                    constantPs0.add(p);
                } else if (p.getIndex1() == 1) {
                    constantPs1.add(p);
                }
            } else {
                biPredicates.add(p);
            }
        }

        HashSet<Predicate> invalidRHSs = new HashSet<>();

        for (int i = 0; i < biPredicates.size(); i++) {
            Predicate p = biPredicates.get(i);
            if (p.getSupport() < this.support) {
                // invalid RHS
                invalidRHSs.add(p);
                biPredicates.remove(i);
            }
        }

        for (int i = 0; i < constantPs0.size(); i++) {
            Predicate p = constantPs0.get(i);
            if (p.getSupport() * this.maxOneRelationNum < this.support) {
                invalidRHSs.add(p);
                constantPs0.remove(i);
            }
        }

        for (int i = 0; i < constantPs1.size(); i++) {
            Predicate p = constantPs1.get(i);
            if (p.getSupport() * this.maxOneRelationNum < this.support) {
                invalidRHSs.add(p);
                constantPs1.remove(i);
            }
        }

        int batch_num = (int)(countT0 / BATCH_TUPLE_NUM) + 1;

        long supportXAll = 0L;
        long supportCP0 = 0L;
        long supportCP1 = 0L;
        Long[] countsBiRHSs = new Long[biPredicates.size()];
        for (int i = 0; i <countsBiRHSs.length; i++) countsBiRHSs[i] = 0L;
        Long[] countsUniRHSs0 = new Long[constantPs0.size()];
        for (int i = 0; i <countsUniRHSs0.length; i++) countsUniRHSs0[i] = 0L;
        Long[] countsUniRHSs1 = new Long[constantPs1.size()];
        for (int i = 0; i <countsUniRHSs1.length; i++) countsUniRHSs1[i] = 0L;


        for (int batch_id = 0; batch_id < batch_num; batch_id++) {

            boolean flag = true;
            long tupleIDs_start_temp = batch_id * BATCH_TUPLE_NUM;
            long tupleIDs_end_temp = Math.min((batch_id + 1) * BATCH_TUPLE_NUM, countT0);
            long tupleIDs_start = tupleIDs_start_temp + this.inputLight.getTupleIDStart(relationT0);
            long tupleIDs_end = tupleIDs_end_temp + this.inputLight.getTupleIDStart(relationT0);

            // process data in a batch mode
            ArrayList<Boolean> usedTids = new ArrayList<>();
            usedTids.add(Boolean.TRUE);
            for (int t = 1; t <= maxtuple; t++) {
                usedTids.add(Boolean.FALSE);
            }
            ArrayList<HashSet<Integer>> usedData = new ArrayList<>();
            for (int t = 0; t <= maxtuple; t++) {
                usedData.add(new HashSet<>());
            }
            for (int tid = (int)tupleIDs_start; tid < tupleIDs_end; tid++) {
                usedData.get(0).add(tid);
            }


            // calculate the intersection and result for each group

            ArrayList<ImmutablePair<Integer, Integer>> keysTid = new ArrayList<>();
            ArrayList<Long> keysFreq = new ArrayList<>();
            HashMap<Integer, ArrayList<Integer>> beginTuplePairs = null;
            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict = new HashMap<>();
//        log.info("Predicate group : {}", predicateGroups);
            for (ImmutablePair<Integer, Integer> tid_pair : keys_new) {
                // for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
                int kk = MultiTuplesOrder.encode(tid_pair);
                ArrayList<Predicate> group = predicateGroups.get(kk);
                // ImmutablePair<Integer, Integer> tid_pair = MultiTuplesOrder.decode(entry.getKey());
                ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long> dataOneGroup =
                        this.findResultsOneGroup_(group, tid_pair.left, tid_pair.right, pids, usedData, usedTids);
                // update usedTids
                usedTids.set(tid_pair.left, Boolean.TRUE);
                usedTids.set(tid_pair.right, Boolean.TRUE);

                if (dataOneGroup == null || dataOneGroup.left.size() <= 0) {
                    flag = false;
                    break;
                }
                // the first case
//            log.info("Predicate tuple ID pair : {}", entry.getKey());
                if (kk == MultiTuplesOrder.encode(new ImmutablePair<>(0, 1))) {
                    beginTuplePairs = dataOneGroup.left;
                } else {
                    datadict.put(kk, dataOneGroup.left);
                }

                keysFreq.add(dataOneGroup.right);
                keysTid.add(tid_pair);
            }
            if (flag == false) {
                continue;
            }
            MultiTuplesOrder multiTuplesOrder = new MultiTuplesOrder(keysTid, keysFreq);
            ArrayList<ImmutablePair<Integer, Integer>> keys_new_new = multiTuplesOrder.rearrangeKeys();
            // retrieve tuple pairs of X
            // ArrayList<ImmutablePair<Integer, Integer>> resultsX = multiTuplesOrder.joinAll(beginTuplePairs, keys_new_new, datadict);

            HashSet<ImmutablePair<Integer, Integer>> resultsX = multiTuplesOrder.joinAll(beginTuplePairs, keys_new_new, datadict, targetTID);

//        log.info("First List : {}", beginTuplePairs);
//        log.info("Final List : {}", resultsX);

            supportXAll += resultsX.size();

            // extract lists for constant predicates
            HashSet<Integer> resultsXCP0 = new HashSet<>();
            HashSet<Integer> resultsXCP1 = new HashSet<>();
            for (ImmutablePair<Integer, Integer> pair : resultsX) {
                resultsXCP0.add(pair.left);
                resultsXCP1.add(pair.right);
            }

            supportCP0 += resultsXCP0.size();
            supportCP1 += resultsXCP1.size();

            biVerify(pids, countsBiRHSs, resultsX, biPredicates);
//        log.info("Mining after biVerify : {}, biPredicates : {}", message, biPredicates);
            uniVerify(pids, countsUniRHSs0, resultsXCP0, constantPs0);
            // do not need t_1 constant predicates
            //uniVerify(pids, countsUniRHSs1, resultsXCP1, constantPs1);
        }

        // put together
//        if (supportXAll < this.support) {
//            // all RHSs with currentList are invalid, add all
//            Message message_ = new Message(currentSet, supportXAll, supportCP0, supportCP1);
//            for (Predicate rhs : rhss) {
//                message_.addInValidRHS(rhs);
//            }
//            messages.add(message_);
//            return messages;
//        }

        Message message = new Message(currentSet, supportXAll, supportCP0, supportCP1);
        // add invalid RHSs
        for (Predicate p : invalidRHSs) {
            message.addInValidRHS(p);
        }

        message.updateAllCurrentRHSsSupport(countsBiRHSs, biPredicates);
        message.updateAllCurrentRHSsSupport(countsUniRHSs0, constantPs0);
        message.updateAllCurrentRHSsSupport(countsUniRHSs1, constantPs1);
        message.updateAllCurrentRHSsSupport(invalidRHSs);

        // uniStatistic(message, countsUniRHSs0, constantPs0, supportXAll);
        // uniStatistic(message, countsUniRHSs1, constantPs1, supportXAll);
        // biStatistic(message, countsBiRHSs, biPredicates, supportXAll);

        // add into a list of messages
        messages.add(message);
        return messages;
    }


    private void biVerify(int[] PIDs, Long[] counts, HashSet<ImmutablePair<Integer, Integer>> resultsX, ArrayList<Predicate> biPredicateRHSs) {

        int t_left = 0, t_right = 1;
        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start_rhs = this.computeTupleIDsStart(biPredicateRHSs);
        for (ImmutablePair<Integer, Integer> pair : resultsX) {
            for (int i = 0; i < biPredicateRHSs.size(); i++) {
                if (biPredicateRHSs.get(i).calculateTp(PIDs[t_left], PIDs[t_right], pair.left, pair.right, tuple_IDs_start_rhs.get(i).left, tuple_IDs_start_rhs.get(i).right)) {
                    counts[i] ++;
                }
            }
        }
    }

    private static Logger log = LoggerFactory.getLogger(MultiTuplesRuleMining.class);
}
