package sics.seiois.mlsserver.biz.der.mining.recovery;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.execution.columnar.ARRAY;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.evidenceset.IEvidenceSet;
import sics.seiois.mlsserver.biz.der.metanome.input.InputLight;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.Message;
import sics.seiois.mlsserver.biz.der.mining.utils.PLILight;

import java.util.*;

public class MultiTuplesRuleMiningConstantRecovery {
    private int max_num_tuples; // the maximum number of tuples a REE contains
    private InputLight inputLight;
    private long support;
    private float confidence;

    private long maxOneRelationNum;

    // tuple numbers of each relation
    private HashMap<String, Long> tupleNumberRelations;

    private long allCount;

    private static int BATCH_TUPLE_NUM = 10000;
    private static int ENUMERATE_RATIO = 100000;

    public MultiTuplesRuleMiningConstantRecovery(int max_num_tuples, InputLight inputLight, long support, float confidence,
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

    private void fillConstantTupleMapping(Predicate realCP, int tid, HashMap<Integer, PredicateSet> constantTupleMapping) {
        if (constantTupleMapping.containsKey(tid)) {
            constantTupleMapping.get(tid).add(realCP);
        } else {
            PredicateSet ps = new PredicateSet();
            ps.add(realCP);
            constantTupleMapping.put(tid, ps);
        }
    }

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

    private long checkOtherPredicates(int[] PIDs, int tid_left, int tid_right, ArrayList<Predicate> currentOneGroup,
                                      ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start,
                                      HashMap<Integer, ArrayList<Integer>> res, int t_left, int t_right, ArrayList<HashSet<Integer>> usedData,
                                      HashMap<Integer, PredicateSet> constantTupleMapping, REETemplate reeTemplate) {
        long supp = 0;
        boolean f = true;
        for (int i = 0; i <currentOneGroup.size(); i++) {
            Predicate p = currentOneGroup.get(i);
            // check constant template
            if (p.isConstant()) {
                if (p.getIndex1() == t_left) {
                    int v = p.retrieveConstantValue(PIDs[t_left], tid_left, tuple_IDs_start.get(i).left);
                    Predicate realCP = reeTemplate.findRealConstantPredicate(p, v);
                    if (realCP == null) {
                        f = false;
                        break;
                    }
                    this.fillConstantTupleMapping(realCP, tid_left, constantTupleMapping);
                } else if (p.getIndex1() == t_right) {
                    int v = p.retrieveConstantValue(PIDs[t_right], tid_right, tuple_IDs_start.get(i).right);
                    Predicate realCP = reeTemplate.findRealConstantPredicate(p, v);
                    if (realCP == null) {
                        f = false;
                        break;
                    }
                    this.fillConstantTupleMapping(realCP, tid_right, constantTupleMapping);
                }

            } else if (! currentOneGroup.get(i).calculateTp(PIDs[t_left], PIDs[t_right], tid_left, tid_right,
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

    public ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long > findResultsOneGroup_(ArrayList<Predicate> currentOneGroup_,
                                                                                           int tid_left, int tid_right,
                                                                                           int[] PIDs,
                                                                                           ArrayList<HashSet<Integer>> usedData,
                                                                                           ArrayList<Boolean> usedTids,
                                                                                           HashMap<Integer, PredicateSet> constantTupleMapping,
                                                                                           REETemplate reeTemplate,
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

                // filter --- valid tids satisfying constant predicates
                if (validConstantTIDs_left != null && (! validConstantTIDs_left.contains(pair.left))) {
                    continue;
                }
                if (validConstantTIDs_right != null && (! validConstantTIDs_right.contains(pair.right))) {
                    continue;
                }

                supp += this.checkOtherPredicates(PIDs, pair.left, pair.right, currentOneGroup, tuple_IDs_start, res, tid_left, tid_right,
                        usedData, constantTupleMapping, reeTemplate);

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

                            supp += this.checkOtherPredicates(PIDs, tidPivot, tidProbe, currentOneGroup,
                                    tuple_IDs_start, res, tid_left, tid_right, usedData, constantTupleMapping, reeTemplate);

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

    private ImmutablePair<Predicate, Double> uniVerifyConstantTemplate(int[] PIDs, HashSet<Integer> resultX, double confidence, REETemplate reeTemplate) {
        HashMap<Predicate, Integer> resTemp = new HashMap<>();
        Predicate constantTemplate = reeTemplate.getRHS();
        ImmutablePair<Integer, Integer> tuple_IDs_start_rhs = this.computeTupleIDsStart(constantTemplate);
        int t_var = constantTemplate.getIndex1();
        for (Integer tid : resultX) {
            int v = constantTemplate.retrieveConstantValue(PIDs[t_var], tid, tuple_IDs_start_rhs.left);
            Predicate realCP = reeTemplate.findRealConstantPredicate(constantTemplate, v);
            if (resTemp.containsKey(realCP)) {
                resTemp.put(realCP, resTemp.get(realCP) + 1);
            } else {
                resTemp.put(realCP, 1);
            }
        }

        Predicate validRHS = null;
        long suppRHS = Long.MAX_VALUE;
        for (Map.Entry<Predicate, Integer> entry : resTemp.entrySet()) {
            if (entry.getValue() < suppRHS) {
                validRHS = entry.getKey();
                suppRHS = entry.getValue();
            }
        }

        // check confidence
        if (suppRHS * 1.0 / resultX.size() >= confidence) {
            return new ImmutablePair<>(validRHS, suppRHS * 1.0 / resultX.size());
        }
        return null;
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

    private Long biVerify(int[] PIDs, HashSet<ImmutablePair<Integer, Integer>> resultsX, Predicate biPredicateRHS) {

        int t_left = 0, t_right = 1;
        long count = 0;
        ImmutablePair<Integer, Integer> tuple_IDs_start_rhs = this.computeTupleIDsStart(biPredicateRHS);
        for (ImmutablePair<Integer, Integer> pair : resultsX) {
            if (biPredicateRHS.calculateTp(PIDs[t_left], PIDs[t_right], pair.left, pair.right, tuple_IDs_start_rhs.left, tuple_IDs_start_rhs.right)) {
                count ++;
            }
        }
        return count;
    }

    private ArrayList<Predicate> genCurrentX(ArrayList<Predicate> constantPredicatesPrefix, REETemplate reeTemplate, PredicateSet ps) {
        ArrayList<Predicate> X = new ArrayList<>();
        X.addAll(constantPredicatesPrefix);
        X.addAll(reeTemplate.getNonConstantPredicatesX());
        for (Predicate cp : ps) {
            X.add(cp);
        }
        return X;
    }

    /******************************************************************************************************************************
     *  new validation with [start, end]
     *  RHS must be t_0.A = XXX OR t_0.A = t_1.B OR ML(t_0.A, t_1.B)
     ******************************************************************************************************************************/
    public List<Message> validation(REETemplate reeTemplate, int[] pids, ArrayList<Predicate> constantPredicatesPrefix) {
        List<Message> messages = new ArrayList<>();

        // retrieve valid TIDs of predicatesPrefix
        HashMap<Integer, HashSet<Integer>> validConstantTIDs = this.retrieveSatisfiedConstantTIDs(pids, constantPredicatesPrefix);

        log.info("CurrentX : {} ^ {}, RHSs : {}", reeTemplate.getConstantTemplatesX(), reeTemplate.getNonConstantPredicatesX(), reeTemplate.getRHS());
        // the template results
        HashMap<PredicateSet, HashSet<ImmutablePair<Integer, Integer>>> constantTemplateTID = new HashMap<>();
        // select constant predicates
        HashMap<Integer, ArrayList<Predicate>> predicateGroups = new HashMap<>();
        ArrayList<ImmutablePair<Integer, Integer>> keys = new ArrayList<>();
        ArrayList<Long> keysFreqs_ = new ArrayList<>();

        String relationT0 = null;
        int maxtuple = 0;
        // group nonConstant predicates in X
        for (Predicate p : reeTemplate.getNonConstantPredicatesX()) {
            // check the relation of t_0
            if (p.getIndex1() == 0) {
                relationT0 = p.getOperand1().getColumn().getTableName();
            }
            maxtuple = Math.max(maxtuple, Math.max(p.getIndex1(), p.getIndex2()));
            Integer key = MultiTuplesOrderConstantRecovery.encode(new ImmutablePair<>(p.getIndex1(), p.getIndex2()));
            if (! predicateGroups.containsKey(key)) {
                ArrayList<Predicate> group = new ArrayList<>();
                group.add(p);
                predicateGroups.put(key, group);
            } else {
                predicateGroups.get(key).add(p);
            }
        }

        // insert constant templates into group
        for (Predicate p : reeTemplate.getConstantTemplatesX()) {
            int index = p.getIndex1();
            for (int t = 0; t <= maxtuple; t++) {
                if (index == t) {
                    continue;
                }
                int min_t = Math.min(index, t);
                int max_t = Math.max(index, t);
                int key = MultiTuplesOrderConstantRecovery.encode(new ImmutablePair<>(min_t, max_t));
                if (predicateGroups.containsKey(key)) {
                    predicateGroups.get(key).add(p);
                    break;
                }
            }
        }

        // record the initial keys and keysFreq
        for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
            keys.add(MultiTuplesOrderConstantRecovery.decode(entry.getKey()));
            keysFreqs_.add(0L);
        }
        // rearrange keys
        MultiTuplesOrderConstantRecovery multiTuplesOrder_ConstantRecovery_new = new MultiTuplesOrderConstantRecovery(keys, keysFreqs_);
        ArrayList<ImmutablePair<Integer, Integer>> keys_new = multiTuplesOrder_ConstantRecovery_new.rearrangeKeys();

        // only consider t_0
        int pidt0 = pids[0];
        long countT0 = 0;
        for (Predicate p : reeTemplate.getNonConstantPredicatesX()) {
            if (p.getIndex1() == 0) {
                int b = p.getOperand1().getColumnLight().getTidsInterval(pidt0).left;
                int e = p.getOperand1().getColumnLight().getTidsInterval(pidt0).right;
                countT0 = e - b;
                break;
            }
        }

        int batch_num = (int)(countT0 / BATCH_TUPLE_NUM) + 1;
        for (int batch_id = 0; batch_id < batch_num; batch_id++) {

            // store constant predicates for each tuple
            HashMap<Integer, PredicateSet> constantPTupleMapping = new HashMap<>();

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
            int beginTuple_left = 0, beginTuple_right = 0;
            boolean ifFirst = false;
            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict = new HashMap<>();
//        log.info("Predicate group : {}", predicateGroups);
            for (ImmutablePair<Integer, Integer> tid_pair : keys_new) {
                int kk = MultiTuplesOrderConstantRecovery.encode(tid_pair);
                ArrayList<Predicate> group = predicateGroups.get(kk);

                HashSet<Integer> validConstantTIDs_left = validConstantTIDs.get(tid_pair.left);
                HashSet<Integer> validConstantTIDs_right = validConstantTIDs.get(tid_pair.right);

                ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long> dataOneGroup =
                        this.findResultsOneGroup_(group, tid_pair.left, tid_pair.right, pids, usedData, usedTids, constantPTupleMapping, reeTemplate,
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
//                if (kk == MultiTuplesOrderConstantRecovery.encode(new ImmutablePair<>(0, 1))) {
                if (tid_pair.left == 0 && ifFirst == false) {
                    beginTuplePairs = dataOneGroup.left;
                    beginTuple_left = tid_pair.left;
                    beginTuple_right = tid_pair.right;
                    ifFirst = true;
                } else {
                    datadict.put(kk, dataOneGroup.left);
                }

                keysFreq.add(dataOneGroup.right);
                keysTid.add(tid_pair);
            }
            if (flag == false) {
                continue;
            }
            MultiTuplesOrderConstantRecovery multiTuplesOrderConstantRecovery = new MultiTuplesOrderConstantRecovery(keysTid, keysFreq);
            ArrayList<ImmutablePair<Integer, Integer>> keys_new_new = multiTuplesOrderConstantRecovery.rearrangeKeys();
            //
            ImmutablePair<Integer, Integer> tuple_IDs_start_rhs = this.computeTupleIDsStart(reeTemplate.getRHS());
            int targetTID_left = 0, targetTID_right = 1;
            // if constant predicate, targetID_left = targetID_right
            if (reeTemplate.getRHS().isConstant()) {
                targetTID_right = targetTID_left;
            }
            // retrieve tuple pairs of X
            multiTuplesOrderConstantRecovery.joinAll(beginTuplePairs, keys_new_new, datadict,
                    targetTID_left, targetTID_right, constantPTupleMapping, reeTemplate, beginTuple_left, beginTuple_right, tuple_IDs_start_rhs, pids, constantTemplateTID);

//        log.info("First List : {}", beginTuplePairs);
//        log.info("Final List : {}", resultsX);

        }

        // statistic
        for (Map.Entry<PredicateSet, HashSet<ImmutablePair<Integer, Integer>>> entry : constantTemplateTID.entrySet()) {
            PredicateSet _ps = entry.getKey();
            HashSet<ImmutablePair<Integer, Integer>> _set = entry.getValue();
            // check confidence
            Long count = this.biVerify(pids, _set, reeTemplate.getRHS());
            ArrayList<Predicate> X = genCurrentX(constantPredicatesPrefix, reeTemplate, _ps);
            Message message = new Message(X, _set.size(), 0, 0);
            message.updateOneCurrentRHSsSupport(count, reeTemplate.getRHS());
            messages.add(message);

//            // check support
//            if (_set.size() < support) {
//                continue;
//            }
//            if (count * 1.0 / _set.size() >= confidence) {
//                ArrayList<Predicate> X = new ArrayList<>();
//                // add prefix set of predicates
//                X.addAll(predicatesPrefix);
//                // add nonConstant predicates
//                X.addAll(reeTemplate.getNonConstantPredicatesX());
//                // add constant predicates
//                for (Predicate cp : _ps) {
//                    X.add(cp);
//                }
//                Predicate validRHS = reeTemplate.getRHS();
//                // construct Message
//                Message message = new Message(X, _set.size(), 0, 0);
//                // add valid RHS
//                message.addValidRHS(validRHS, _set.size(), count * 1.0 / _set.size());
//
//                messages.add(message);
//            }
        }
        return messages;
    }



    /******************************************************************************************************************************
     *  new validation with [start, end]
     *  NOT used below !!!
     ******************************************************************************************************************************/
    public List<Message> validation_new_nonConstantRHS_(REETemplate reeTemplate, int[] pids, ArrayList<Predicate> predicatesPrefix) {
        List<Message> messages = new ArrayList<>();

        // retrieve valid TIDs of predicatesPrefix
        HashMap<Integer, HashSet<Integer>> validConstantTIDs = this.retrieveSatisfiedConstantTIDs(pids, predicatesPrefix);

        log.info("CurrentX : {} ^ {}, RHSs : {}", reeTemplate.getConstantTemplatesX(), reeTemplate.getNonConstantPredicatesX(), reeTemplate.getRHS());
        // the template results
        HashMap<PredicateSet, HashSet<ImmutablePair<Integer, Integer>>> constantTemplateTID = new HashMap<>();
        // select constant predicates
        HashMap<Integer, ArrayList<Predicate>> predicateGroups = new HashMap<>();
        ArrayList<ImmutablePair<Integer, Integer>> keys = new ArrayList<>();
        ArrayList<Long> keysFreqs_ = new ArrayList<>();

        String relationT0 = null;
        int maxtuple = 0;
        // group nonConstant predicates in X
        for (Predicate p : reeTemplate.getNonConstantPredicatesX()) {
            // check the relation of t_0
            if (p.getIndex1() == 0) {
                relationT0 = p.getOperand1().getColumn().getTableName();
            }
            maxtuple = Math.max(maxtuple, Math.max(p.getIndex1(), p.getIndex2()));
            Integer key = MultiTuplesOrderConstantRecovery.encode(new ImmutablePair<>(p.getIndex1(), p.getIndex2()));
            if (! predicateGroups.containsKey(key)) {
                ArrayList<Predicate> group = new ArrayList<>();
                group.add(p);
                predicateGroups.put(key, group);
            } else {
                predicateGroups.get(key).add(p);
            }
        }

        // insert constant templates into group
        for (Predicate p : reeTemplate.getConstantTemplatesX()) {
            int index = p.getIndex1();
            for (int t = 0; t <= maxtuple; t++) {
                if (index == t) {
                    continue;
                }
                int min_t = Math.min(index, t);
                int max_t = Math.max(index, t);
                int key = MultiTuplesOrderConstantRecovery.encode(new ImmutablePair<>(min_t, max_t));
                if (predicateGroups.containsKey(key)) {
                    predicateGroups.get(key).add(p);
                }
            }
        }

        // record the initial keys and keysFreq
        for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
            keys.add(MultiTuplesOrderConstantRecovery.decode(entry.getKey()));
            keysFreqs_.add(0L);
        }
        // rearrange keys
        MultiTuplesOrderConstantRecovery multiTuplesOrder_ConstantRecovery_new = new MultiTuplesOrderConstantRecovery(keys, keysFreqs_);
        ArrayList<ImmutablePair<Integer, Integer>> keys_new = multiTuplesOrder_ConstantRecovery_new.rearrangeKeys();

//        // get the tuple number of relationT0
//        long countT0 = this.tupleNumberRelations.get(relationT0);
//        boolean ifTermin = true;
//        for (String r : this.tupleNumberRelations.keySet()) {
//            if (r.equals(relationT0)) {
//                continue;
//            }
//            if (countT0 < this.tupleNumberRelations.get(r)) {
//                ifTermin = false;
//            }
//        }
//        if (this.tupleNumberRelations.size() > 1 && ifTermin) {
//            Message message_ = new Message(currentSet, 0, 0, 0);
//            for (Predicate rhs : rhss) {
//                message_.addInValidRHS(rhs);
//            }
//            messages.add(message_);
//            return messages;
//        }

        // only consider t_0
        int pidt0 = pids[0];
        long countT0 = 0;
        for (Predicate p : reeTemplate.getNonConstantPredicatesX()) {
            if (p.getIndex1() == 0) {
                int b = p.getOperand1().getColumnLight().getTidsInterval(pidt0).left;
                int e = p.getOperand1().getColumnLight().getTidsInterval(pidt0).right;
                countT0 = e - b;
                break;
            }
        }

        int batch_num = (int)(countT0 / BATCH_TUPLE_NUM) + 1;
        for (int batch_id = 0; batch_id < batch_num; batch_id++) {

            // store constant predicates for each tuple
            HashMap<Integer, PredicateSet> constantPTupleMapping = new HashMap<>();

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
            int beginTuple_left = 0, beginTuple_right = 0;
            boolean ifFirst = false;
            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict = new HashMap<>();
//        log.info("Predicate group : {}", predicateGroups);
            for (ImmutablePair<Integer, Integer> tid_pair : keys_new) {
                // for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
                int kk = MultiTuplesOrderConstantRecovery.encode(tid_pair);
                ArrayList<Predicate> group = predicateGroups.get(kk);
                // ImmutablePair<Integer, Integer> tid_pair = MultiTuplesOrder.decode(entry.getKey());

                HashSet<Integer> validConstantTIDs_left = validConstantTIDs.get(tid_pair.left);
                HashSet<Integer> validConstantTIDs_right = validConstantTIDs.get(tid_pair.right);

                ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long> dataOneGroup =
                        this.findResultsOneGroup_(group, tid_pair.left, tid_pair.right, pids, usedData, usedTids, constantPTupleMapping, reeTemplate,
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
//                if (kk == MultiTuplesOrderConstantRecovery.encode(new ImmutablePair<>(0, 1))) {
                if (tid_pair.left == 0 && ifFirst == false) {
                    beginTuplePairs = dataOneGroup.left;
                    beginTuple_left = tid_pair.left;
                    beginTuple_right = tid_pair.right;
                    ifFirst = true;
                } else {
                    datadict.put(kk, dataOneGroup.left);
                }

                keysFreq.add(dataOneGroup.right);
                keysTid.add(tid_pair);
            }
            if (flag == false) {
                continue;
            }
            MultiTuplesOrderConstantRecovery multiTuplesOrderConstantRecovery = new MultiTuplesOrderConstantRecovery(keysTid, keysFreq);
            ArrayList<ImmutablePair<Integer, Integer>> keys_new_new = multiTuplesOrderConstantRecovery.rearrangeKeys();
            //
            ArrayList<Predicate> rhss = new ArrayList<>();
            rhss.add(reeTemplate.getRHS());
            ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start_rhss = this.computeTupleIDsStart(rhss);
            int targetTID_left = 0, targetTID_right = 1;
            // retrieve tuple pairs of X
            HashSet<ImmutablePair<Integer, Integer>> resultsX = multiTuplesOrderConstantRecovery.joinAllNonConstantRHS(beginTuplePairs, keys_new_new, datadict,
                    targetTID_left, targetTID_right, constantPTupleMapping, reeTemplate, beginTuple_left, beginTuple_right, tuple_IDs_start_rhss.get(0), pids, constantTemplateTID);

//        log.info("First List : {}", beginTuplePairs);
//        log.info("Final List : {}", resultsX);

        }


        // statistic
        for (Map.Entry<PredicateSet, HashSet<ImmutablePair<Integer, Integer>>> entry : constantTemplateTID.entrySet()) {
            PredicateSet _ps = entry.getKey();
            HashSet<ImmutablePair<Integer, Integer>> _set = entry.getValue();
            // check confidence
            Long count = this.biVerify(pids, _set, reeTemplate.getRHS());

            ArrayList<Predicate> X = new ArrayList<>();
            X.addAll(predicatesPrefix);
            X.addAll(reeTemplate.getNonConstantPredicatesX());
            for (Predicate cp : _ps) {
                X.add(cp);
            }
            Message message = new Message(X, _set.size(), 0, 0);
            message.updateOneCurrentRHSsSupport(count, reeTemplate.getRHS());
            messages.add(message);

//            // check support
//            if (_set.size() < support) {
//                continue;
//            }
//            if (count * 1.0 / _set.size() >= confidence) {
//                ArrayList<Predicate> X = new ArrayList<>();
//                // add prefix set of predicates
//                X.addAll(predicatesPrefix);
//                // add nonConstant predicates
//                X.addAll(reeTemplate.getNonConstantPredicatesX());
//                // add constant predicates
//                for (Predicate cp : _ps) {
//                    X.add(cp);
//                }
//                Predicate validRHS = reeTemplate.getRHS();
//                // construct Message
//                Message message = new Message(X, _set.size(), 0, 0);
//                // add valid RHS
//                message.addValidRHS(validRHS, _set.size(), count * 1.0 / _set.size());
//
//                messages.add(message);
//            }
        }
        return messages;
    }

//    public List<Message> validation_new(ArrayList<Predicate> currentSet, PredicateSet rhss, int[] pids, IEvidenceSet evidenceSet) {
    /*
        Only consider X -> p_0, where p_0 is a constant predicate t_0.A = _
     */
    public List<Message> validation_new_constantRHS(REETemplate reeTemplate, int[] pids, ArrayList<Predicate> predicatesPrefix) {
        List<Message> messages = new ArrayList<>();
        log.info("CurrentX : {} ^ {}, RHSs : {}", reeTemplate.getConstantTemplatesX(), reeTemplate.getNonConstantPredicatesX(), reeTemplate.getRHS());


        // retrieve valid TIDs of predicatesPrefix
        HashMap<Integer, HashSet<Integer>> validConstantTIDs = this.retrieveSatisfiedConstantTIDs(pids, predicatesPrefix);

        // the template results
        HashMap<PredicateSet, HashSet<Integer>> constantTemplateTID0 = new HashMap<>();
        // select constant predicates
        HashMap<Integer, ArrayList<Predicate>> predicateGroups = new HashMap<>();
        ArrayList<ImmutablePair<Integer, Integer>> keys = new ArrayList<>();
        ArrayList<Long> keysFreqs_ = new ArrayList<>();

        String relationT0 = null;
        int maxtuple = 0;
        // group nonConstant predicates in X
        for (Predicate p : reeTemplate.getNonConstantPredicatesX()) {
            // check the relation of t_0
            if (p.getIndex1() == 0) {
                relationT0 = p.getOperand1().getColumn().getTableName();
            }
            maxtuple = Math.max(maxtuple, Math.max(p.getIndex1(), p.getIndex2()));
            Integer key = MultiTuplesOrderConstantRecovery.encode(new ImmutablePair<>(p.getIndex1(), p.getIndex2()));
            if (! predicateGroups.containsKey(key)) {
                ArrayList<Predicate> group = new ArrayList<>();
                group.add(p);
                predicateGroups.put(key, group);
            } else {
                predicateGroups.get(key).add(p);
            }
        }

        // insert constant templates into group
        for (Predicate p : reeTemplate.getConstantTemplatesX()) {
            int index = p.getIndex1();
            for (int t = 0; t <= maxtuple; t++) {
                if (index == t) {
                    continue;
                }
                int min_t = Math.min(index, t);
                int max_t = Math.max(index, t);
                int key = MultiTuplesOrderConstantRecovery.encode(new ImmutablePair<>(min_t, max_t));
                if (predicateGroups.containsKey(key)) {
                    predicateGroups.get(key).add(p);
                }
            }
        }

        // record the initial keys and keysFreq
        for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
            keys.add(MultiTuplesOrderConstantRecovery.decode(entry.getKey()));
            keysFreqs_.add(0L);
        }
        // rearrange keys
        MultiTuplesOrderConstantRecovery multiTuplesOrder_ConstantRecovery_new = new MultiTuplesOrderConstantRecovery(keys, keysFreqs_);
        ArrayList<ImmutablePair<Integer, Integer>> keys_new = multiTuplesOrder_ConstantRecovery_new.rearrangeKeys();

//        // get the tuple number of relationT0
//        long countT0 = this.tupleNumberRelations.get(relationT0);
//        boolean ifTermin = true;
//        for (String r : this.tupleNumberRelations.keySet()) {
//            if (r.equals(relationT0)) {
//                continue;
//            }
//            if (countT0 < this.tupleNumberRelations.get(r)) {
//                ifTermin = false;
//            }
//        }
//        if (this.tupleNumberRelations.size() > 1 && ifTermin) {
//            Message message_ = new Message(currentSet, 0, 0, 0);
//            for (Predicate rhs : rhss) {
//                message_.addInValidRHS(rhs);
//            }
//            messages.add(message_);
//            return messages;
//        }

        // only consider t_0
        int pidt0 = pids[0];
        long countT0 = 0;
        for (Predicate p : reeTemplate.getNonConstantPredicatesX()) {
            if (p.getIndex1() == 0) {
                int b = p.getOperand1().getColumnLight().getTidsInterval(pidt0).left;
                int e = p.getOperand1().getColumnLight().getTidsInterval(pidt0).right;
                countT0 = e - b;
                break;
            }
        }

        int batch_num = (int)(countT0 / BATCH_TUPLE_NUM) + 1;
        for (int batch_id = 0; batch_id < batch_num; batch_id++) {

            // store constant predicates for each tuple
            HashMap<Integer, PredicateSet> constantPTupleMapping = new HashMap<>();

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
            int beginTuple_left = 0, beginTuple_right = 0;
            boolean ifFirst = false;
            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict = new HashMap<>();
//        log.info("Predicate group : {}", predicateGroups);
            for (ImmutablePair<Integer, Integer> tid_pair : keys_new) {
                // for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
                int kk = MultiTuplesOrderConstantRecovery.encode(tid_pair);
                ArrayList<Predicate> group = predicateGroups.get(kk);
                // ImmutablePair<Integer, Integer> tid_pair = MultiTuplesOrder.decode(entry.getKey());

                HashSet<Integer> validConstantTIDs_left = validConstantTIDs.get(tid_pair.left);
                HashSet<Integer> validConstantTIDs_right = validConstantTIDs.get(tid_pair.right);

                ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long> dataOneGroup =
                        this.findResultsOneGroup_(group, tid_pair.left, tid_pair.right, pids, usedData, usedTids, constantPTupleMapping,
                                reeTemplate, validConstantTIDs_left, validConstantTIDs_right);
                // update usedTids
                usedTids.set(tid_pair.left, Boolean.TRUE);
                usedTids.set(tid_pair.right, Boolean.TRUE);

                if (dataOneGroup == null || dataOneGroup.left.size() <= 0) {
                    flag = false;
                    break;
                }
                // the first case
//            log.info("Predicate tuple ID pair : {}", entry.getKey());
//                if (kk == MultiTuplesOrderConstantRecovery.encode(new ImmutablePair<>(0, 1))) {
                if (tid_pair.left == 0 && ifFirst == false) {
                    beginTuplePairs = dataOneGroup.left;
                    beginTuple_left = tid_pair.left;
                    beginTuple_right = tid_pair.right;
                    ifFirst = true;
                } else {
                    datadict.put(kk, dataOneGroup.left);
                }

                keysFreq.add(dataOneGroup.right);
                keysTid.add(tid_pair);
            }
            if (flag == false) {
                continue;
            }
            MultiTuplesOrderConstantRecovery multiTuplesOrderConstantRecovery = new MultiTuplesOrderConstantRecovery(keysTid, keysFreq);
            ArrayList<ImmutablePair<Integer, Integer>> keys_new_new = multiTuplesOrderConstantRecovery.rearrangeKeys();
            //
            ArrayList<Predicate> rhss = new ArrayList<>();
            rhss.add(reeTemplate.getRHS());
            ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start_rhss = this.computeTupleIDsStart(rhss);
            int targetTID = 1;
            // retrieve tuple pairs of X
            HashSet<ImmutablePair<Integer, Integer>> resultsX = multiTuplesOrderConstantRecovery.joinAllConstantRHS(beginTuplePairs, keys_new_new, datadict,
                    targetTID, constantPTupleMapping, reeTemplate, beginTuple_left, beginTuple_right, tuple_IDs_start_rhss.get(0), pids, constantTemplateTID0);

//        log.info("First List : {}", beginTuplePairs);
//        log.info("Final List : {}", resultsX);

        }


        // statistic
        for (Map.Entry<PredicateSet, HashSet<Integer>> entry : constantTemplateTID0.entrySet()) {
            PredicateSet _ps = entry.getKey();
            HashSet<Integer> _set = entry.getValue();
            // check support
            if (_set.size() * maxOneRelationNum < support) {
                continue;
            }
            // check confidence
            ImmutablePair<Predicate, Double> constantRHSInfo = this.uniVerifyConstantTemplate(pids, _set, confidence, reeTemplate);
            if (constantRHSInfo != null) {
                ArrayList<Predicate> X = new ArrayList<>();
                // add prefix set of predicates
                X.addAll(predicatesPrefix);
                // add nonConstant predicates
                X.addAll(reeTemplate.getNonConstantPredicatesX());
                // add constant predicates
                for (Predicate cp : _ps) {
                    X.add(cp);
                }
                Predicate validRHS = constantRHSInfo.left;
                // construct Message
                Message message = new Message(X, _set.size() * maxOneRelationNum, 0, 0);
                // add valid RHS
                message.addValidRHS(validRHS, _set.size() * maxOneRelationNum, constantRHSInfo.right);

                messages.add(message);
            }
        }
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

    private static Logger log = LoggerFactory.getLogger(MultiTuplesRuleMiningConstantRecovery.class);
}
