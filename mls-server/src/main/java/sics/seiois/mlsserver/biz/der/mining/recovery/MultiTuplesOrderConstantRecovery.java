package sics.seiois.mlsserver.biz.der.mining.recovery;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class MultiTuplesOrderConstantRecovery {
    private ArrayList<ImmutablePair<Integer, Integer>> keys;
    private HashMap<Integer, Long> keyFreqs;

    public static final Integer MAX_TUPLE_ID = 100;

    public MultiTuplesOrderConstantRecovery(ArrayList<ImmutablePair<Integer, Integer>> keys,
                                            ArrayList<Long> freqs) {
        this.keys = keys;
        this.keyFreqs = new HashMap<>();
        for (int i = 0; i < freqs.size(); i++){
            ImmutablePair<Integer, Integer> pair = keys.get(i);
            keyFreqs.put(MultiTuplesOrderConstantRecovery.encode(pair), freqs.get(i));
        }
    }

    public static Integer encode(ImmutablePair<Integer, Integer> pair) {
        return pair.left * MAX_TUPLE_ID + pair.right;
    }

    public static ImmutablePair<Integer, Integer> decode(Integer key) {
        return new ImmutablePair<>((int)(key / MAX_TUPLE_ID), (int)(key % MAX_TUPLE_ID));
    }


    public ArrayList<ImmutablePair<Integer, Integer>> rearrangeKeys() {
        ArrayList<ImmutablePair<Integer, Integer>> keys_new = new ArrayList<>();
        // 1. find <t_0, t_X>
        HashSet<Integer> selected = new HashSet<>();
        int sc = 0;
        for (ImmutablePair<Integer, Integer> idPair : keys) {
            if (idPair.left == 0) {
                keys_new.add(new ImmutablePair<>(idPair.left, idPair.right));
                selected.add(idPair.left);
                selected.add(idPair.right);
                break;
            }
            sc++;
        }
        keys.remove(sc);
        // 2. reorder
        int remainsize = keys.size();
        for (int i = 0; i < remainsize; i++) {
            long freq = Long.MAX_VALUE;
            int cc = -1;
            // always select the one with the minimal frequency
            for (int k = 0; k < keys.size(); k++) {
                ImmutablePair<Integer, Integer> pair = keys.get(k);
                if (checkIntersection(pair, selected)) {
                    Long ff = this.keyFreqs.get(MultiTuplesOrderConstantRecovery.encode(pair));
                    if (ff <= freq) {
                        freq = ff;
                        cc = k;
                    }
                }
            }
            // update
            keys_new.add(keys.get(cc));
            // add the pair to selected
            selected.add(keys.get(cc).left);
            selected.add(keys.get(cc).right);
            keys.remove(cc);
        }
        return keys_new;
    }

    private boolean checkIntersection(ImmutablePair<Integer, Integer> pair, HashSet<Integer> selected) {
        if (selected.contains(pair.left) || selected.contains(pair.right)) {
            return true;
        }
        return false;
    }






    public void joinAll(HashMap<Integer, ArrayList<Integer>> beginTuplePairs,
                        ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                        HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict,
                        int targetTID_left, int targetTID_right, HashMap<Integer, PredicateSet> constantTupleMapping,
                        REETemplate reeTemplate, int beginTuple_left, int beginTuple_right,
                        ImmutablePair<Integer, Integer> tuple_IDs_start_rhs,
                        int[] PIDs, HashMap<PredicateSet, HashSet<ImmutablePair<Integer, Integer>>> constantTemplateTID) {

        Integer[] current = new Integer[MAX_TUPLE_ID];
        HashSet<Integer> dataOtherTID = new HashSet<>();
        for (int i = 0; i < current.length; i++) {
            current[i] = null;
        }
        PredicateSet ps = new PredicateSet();
        for (Map.Entry<Integer, ArrayList<Integer>> entry : beginTuplePairs.entrySet()) {
            current[beginTuple_left] = entry.getKey(); // beginTuple_left = 0
            dataOtherTID.clear();
            for (Integer tid : entry.getValue()) {
                current[beginTuple_right] = tid;
                ps.clear();
                join(current, 1, targetTID_left, targetTID_right, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs, constantTemplateTID);
            }
            for (int i = 1; i < current.length; i++) {
                current[i] = null;
            }
        }
    }

    /*
        t_0 = 0, t_1 = 1
        targetTID = t_1
     */
    public boolean join(Integer[] current, int index, int targetTID_left, int targetTID_right, ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                                      HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict, HashSet<Integer> dataOtherTID,
                                      PredicateSet ps, HashMap<Integer, PredicateSet> constantTupleMapping, REETemplate reeTemplate,
                                      ImmutablePair<Integer, Integer> tuple_IDs_start_rhs, int[] PIDs, HashMap<PredicateSet, HashSet<ImmutablePair<Integer, Integer>>> constantTemplateTID) {
        if (index >= keys_new.size()) {

            if (constantTemplateTID.containsKey(ps)) {
                constantTemplateTID.get(ps).add(new ImmutablePair<Integer, Integer>(current[targetTID_left], current[targetTID_right]));
            } else {
                HashSet<ImmutablePair<Integer, Integer>> _set = new HashSet<>();
                _set.add(new ImmutablePair<>(current[targetTID_left], current[targetTID_right]));
                constantTemplateTID.put(ps, _set);
            }

            return true;
        }
        ImmutablePair<Integer, Integer> pair = keys_new.get(index);
        ArrayList<Integer> next = datadict.get(MultiTuplesOrderConstantRecovery.encode(pair)).get(current[pair.left]);
        if (next == null) {
            return false;
        }
        boolean term = false;
        for (Integer e : next) {
            if (term) {
                break;
            }
            if (current[pair.right] == null) {
                current[pair.right] = e;
                // add predicateset
                if (constantTupleMapping.containsKey(e)) {
                    ps.or(constantTupleMapping.get(e));
                }
                term = join(current, index + 1, targetTID_left, targetTID_right, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs, constantTemplateTID);
                // backtrace
                current[pair.right] = null;
                if (constantTupleMapping.containsKey(e)) {
                    ps.xor(constantTupleMapping.get(e));
                }
            } else if (current[pair.right] == e) {
                term = join(current, index + 1, targetTID_left, targetTID_right, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs, constantTemplateTID);
            }
        }
        return term;
    }









    /******************************************************************************
     * NOT USED BELOW !!!!!!
     * new join and joinAll, s.t., RHSs could be <t_0>, <t_0, t_x>, x = 1, 2, ...
     * beginTuplePairs MUST contain t_0
     * Thus, beginTuple_left == 0
     ******************************************************************************/
    public HashSet<ImmutablePair<Integer, Integer>> joinAllConstantRHS(HashMap<Integer, ArrayList<Integer>> beginTuplePairs,
                                                            ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                                                            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict,
                                                            int targetTID, HashMap<Integer, PredicateSet> constantTupleMapping,
                                                            REETemplate reeTemplate, int beginTuple_left, int beginTuple_right,
                                                            ImmutablePair<Integer, Integer> tuple_IDs_start_rhs,
                                                            int[] PIDs, HashMap<PredicateSet, HashSet<Integer>> constantTemplateTID0) {
        HashSet<ImmutablePair<Integer, Integer>> res = new HashSet<>();
        Integer[] current = new Integer[MAX_TUPLE_ID];
        HashSet<Integer> dataOtherTID = new HashSet<>();
        for (int i = 0; i < current.length; i++) {
            current[i] = null;
        }
        PredicateSet ps = new PredicateSet();
        for (Map.Entry<Integer, ArrayList<Integer>> entry : beginTuplePairs.entrySet()) {
            current[0] = entry.getKey(); // beginTuple_left = 0
            dataOtherTID.clear();
            for (Integer tid : entry.getValue()) {
                current[beginTuple_right] = tid;
                ps.clear();
                joinConstantRHS(current, 1, targetTID, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs, constantTemplateTID0);
            }
            for (int i = 1; i < current.length; i++) {
                current[i] = null;
            }
            // add results
            for (Integer e : dataOtherTID) {
                res.add(new ImmutablePair<>(entry.getKey(), e));
            }
        }
        return res;
    }

    public boolean joinConstantRHS(Integer[] current, int index, int targetTID, ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                        HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict, HashSet<Integer> dataOtherTID,
                        PredicateSet ps, HashMap<Integer, PredicateSet> constantTupleMapping, REETemplate reeTemplate,
                        ImmutablePair<Integer, Integer> tuple_IDs_start_rhs, int[] PIDs, HashMap<PredicateSet, HashSet<Integer>> constantTemplateTID0) {
        if (index >= keys_new.size()) {
            dataOtherTID.add(current[targetTID]);

            if (constantTemplateTID0.containsKey(ps)) {
                constantTemplateTID0.get(ps).add(current[targetTID]);
            }

            return true;
        }
        ImmutablePair<Integer, Integer> pair = keys_new.get(index);
        ArrayList<Integer> next = datadict.get(MultiTuplesOrderConstantRecovery.encode(pair)).get(current[pair.left]);
        if (next == null) {
            return false;
        }
        boolean term = false;
        for (Integer e : next) {
            if (term) {
                break;
            }
            if (current[pair.right] == null) {
                current[pair.right] = e;
                // add predicateset
                if (constantTupleMapping.containsKey(e)) {
                    ps.or(constantTupleMapping.get(e));
                }
                term = joinConstantRHS(current, index + 1, targetTID, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs, constantTemplateTID0);
                // backtrace
                current[pair.right] = null;
                if (constantTupleMapping.containsKey(e)) {
                    ps.xor(constantTupleMapping.get(e));
                }
            } else if (current[pair.right] == e) {
                term = joinConstantRHS(current, index + 1, targetTID, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs, constantTemplateTID0);
            }
        }
        return term;
    }

    public HashSet<ImmutablePair<Integer, Integer>> joinAllNonConstantRHS(HashMap<Integer, ArrayList<Integer>> beginTuplePairs,
                                                                       ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                                                                       HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict,
                                                                       int targetTID_left, int targetTID_right, HashMap<Integer, PredicateSet> constantTupleMapping,
                                                                       REETemplate reeTemplate, int beginTuple_left, int beginTuple_right,
                                                                       ImmutablePair<Integer, Integer> tuple_IDs_start_rhs,
                                                                       int[] PIDs, HashMap<PredicateSet, HashSet<ImmutablePair<Integer, Integer>>> constantTemplateTID) {
        HashSet<ImmutablePair<Integer, Integer>> res = new HashSet<>();
        Integer[] current = new Integer[MAX_TUPLE_ID];
        HashSet<Integer> dataOtherTID = new HashSet<>();
        for (int i = 0; i < current.length; i++) {
            current[i] = null;
        }
        PredicateSet ps = new PredicateSet();
        for (Map.Entry<Integer, ArrayList<Integer>> entry : beginTuplePairs.entrySet()) {
            current[0] = entry.getKey(); // beginTuple_left = 0
            dataOtherTID.clear();
            for (Integer tid : entry.getValue()) {
                current[beginTuple_right] = tid;
                ps.clear();
                joinNonConstantRHS(current, 1, targetTID_left, targetTID_right, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs, constantTemplateTID);
            }
            for (int i = 1; i < current.length; i++) {
                current[i] = null;
            }
            // add results
            for (Integer e : dataOtherTID) {
                res.add(new ImmutablePair<>(entry.getKey(), e));
            }
        }
        return res;
    }

    /*
        t_0 = 0, t_1 = 1
        targetTID = t_1
     */
    public boolean joinNonConstantRHS(Integer[] current, int index, int targetTID_left, int targetTID_right, ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                                   HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict, HashSet<Integer> dataOtherTID,
                                   PredicateSet ps, HashMap<Integer, PredicateSet> constantTupleMapping, REETemplate reeTemplate,
                                   ImmutablePair<Integer, Integer> tuple_IDs_start_rhs, int[] PIDs, HashMap<PredicateSet, HashSet<ImmutablePair<Integer, Integer>>> constantTemplateTID) {
        if (index >= keys_new.size()) {

            if (constantTemplateTID.containsKey(ps)) {
                constantTemplateTID.get(ps).add(new ImmutablePair<Integer, Integer>(current[targetTID_left], current[targetTID_right]));
            }

            return true;
        }
        ImmutablePair<Integer, Integer> pair = keys_new.get(index);
        ArrayList<Integer> next = datadict.get(MultiTuplesOrderConstantRecovery.encode(pair)).get(current[pair.left]);
        if (next == null) {
            return false;
        }
        boolean term = false;
        for (Integer e : next) {
            if (term) {
                break;
            }
            if (current[pair.right] == null) {
                current[pair.right] = e;
                // add predicateset
                if (constantTupleMapping.containsKey(e)) {
                    ps.or(constantTupleMapping.get(e));
                }
                term = joinNonConstantRHS(current, index + 1, targetTID_left, targetTID_right, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs, constantTemplateTID);
                // backtrace
                current[pair.right] = null;
                if (constantTupleMapping.containsKey(e)) {
                    ps.xor(constantTupleMapping.get(e));
                }
            } else if (current[pair.right] == e) {
                term = joinNonConstantRHS(current, index + 1, targetTID_left, targetTID_right, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs, constantTemplateTID);
            }
        }
        return term;
    }


    public boolean join(Integer[] current, int index, int targetTID, ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                        HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict, HashSet<Integer> dataOtherTID,
                        PredicateSet ps, HashMap<Integer, PredicateSet> constantTupleMapping, REETemplate reeTemplate,
                        ImmutablePair<Integer, Integer> tuple_IDs_start_rhs, int[] PIDs) {
        if (index >= keys_new.size()) {
            dataOtherTID.add(current[targetTID]);
            // add RHS
            Predicate rhs = reeTemplate.getRHS();
            if (reeTemplate.ifConstantTemplateRHS() == false) {
                int t_left = rhs.getIndex1();
                int t_right = rhs.getIndex2();
                int tid_left = current[t_left];
                int tid_right = current[t_right];
                if (rhs.calculateTp(PIDs[t_left], PIDs[t_right], tid_left, tid_right,
                        tuple_IDs_start_rhs.left, tuple_IDs_start_rhs.right)) {
                    ps.add(rhs);
                }
            } else {
                int t_left = rhs.getIndex1();
                int tid_left = current[t_left];
                int v = rhs.retrieveConstantValue(PIDs[t_left], tid_left, tuple_IDs_start_rhs.left);
                rhs = reeTemplate.findRealConstantPredicate(rhs, v);
                if (rhs != null) {
                    ps.add(rhs);
                }
            }
            // backtrace
            ps.remove(rhs);
            return true;
        }
        ImmutablePair<Integer, Integer> pair = keys_new.get(index);
        ArrayList<Integer> next = datadict.get(MultiTuplesOrderConstantRecovery.encode(pair)).get(current[pair.left]);
        if (next == null) {
            return false;
        }
        boolean term = false;
        for (Integer e : next) {
            if (term) {
                break;
            }
            if (current[pair.right] == null) {
                current[pair.right] = e;
                // add predicateset
                if (constantTupleMapping.containsKey(e)) {
                    ps.or(constantTupleMapping.get(e));
                }
                term = join(current, index + 1, targetTID, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs);
                // backtrace
                current[pair.right] = null;
                if (constantTupleMapping.containsKey(e)) {
                    ps.xor(constantTupleMapping.get(e));
                }
            } else if (current[pair.right] == e) {
                term = join(current, index + 1, targetTID, keys_new, datadict, dataOtherTID, ps,
                        constantTupleMapping, reeTemplate, tuple_IDs_start_rhs, PIDs);
            }
        }
        return term;
    }


    private static Logger log = LoggerFactory.getLogger(MultiTuplesOrderConstantRecovery.class);

}
