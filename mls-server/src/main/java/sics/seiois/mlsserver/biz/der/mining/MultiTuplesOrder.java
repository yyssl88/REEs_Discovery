package sics.seiois.mlsserver.biz.der.mining;

import io.swagger.models.auth.In;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.sql.execution.columnar.ARRAY;
import org.apache.xpath.operations.Mult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.REEFinderEvidSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class MultiTuplesOrder {
    private ArrayList<ImmutablePair<Integer, Integer>> keys;
    private HashMap<Integer, Long> keyFreqs;

    public static final Integer MAX_TUPLE_ID = 100;

    public MultiTuplesOrder(ArrayList<ImmutablePair<Integer, Integer>> keys,
                            ArrayList<Long> freqs) {
        this.keys = keys;
        this.keyFreqs = new HashMap<>();
        for (int i = 0; i < freqs.size(); i++){
            ImmutablePair<Integer, Integer> pair = keys.get(i);
            keyFreqs.put(MultiTuplesOrder.encode(pair), freqs.get(i));
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
        // 1. find <t_0, t_1>
        HashSet<Integer> selected = new HashSet<>();
        int count = 0;
        for (ImmutablePair<Integer, Integer> idPair : keys) {
            if (idPair.left == 0 && idPair.right == 1) {
                keys_new.add(new ImmutablePair<Integer, Integer>(0, 1));
                // keys.remove(count);
                selected.add(0);
                selected.add(1);
                break;
            }
            count++;
        }
        // remove the initial key
//        log.info("Original Keys : {}", keys);
        keys.remove(count);
        // 2. rearrange
//        log.info("Keys after remove : {}", keys);
        int remainsize = keys.size();
        for (int i = 0 ; i < remainsize ; i++) {
            long freq = Long.MAX_VALUE;
            int cc = -1;
            for (int k = 0; k < keys.size() ; k ++) {
                ImmutablePair<Integer, Integer> pair = keys.get(k);
                if (checkIntersection(pair, selected)) {
                    Long ff = this.keyFreqs.get(MultiTuplesOrder.encode(pair));
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

    // compute join results
    public boolean join(Integer[] current, int index, ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                                                            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict) {
        if (index >= keys_new.size()) {
             return true;
        }

        ImmutablePair<Integer, Integer> pair = keys_new.get(index);
        // pair.left is the index key, and pair.right is the constraint condition
        ArrayList<Integer> next = datadict.get(MultiTuplesOrder.encode(pair)).get(current[pair.left]);
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
                term = join(current, index + 1, keys_new, datadict);
                // backtrace
                current[pair.right] = null;
            } else if (current[pair.right] == e) {
                term = join(current, index + 1, keys_new, datadict);
            }
        }
        return term;
    }

    public ArrayList<ImmutablePair<Integer, Integer>> joinAll(HashMap<Integer, ArrayList<Integer>> beginTuplePairs,
                                                              ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                                                              HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict) {
        ArrayList<ImmutablePair<Integer, Integer>> res = new ArrayList<>();
        // the subscripts of "current" are the tuple IDs
        Integer[] current = new Integer[MAX_TUPLE_ID];
        for (int i = 0; i < current.length; i++) {
            current[i] = null;
        }
//        for (ImmutablePair<Integer, Integer> pair : beginTuplePairs) {
        for (Map.Entry<Integer, ArrayList<Integer>> entry : beginTuplePairs.entrySet()) {
            current[0] = entry.getKey();
            for (Integer tid : entry.getValue()) {
                current[1] = tid;
                if (join(current, 1, keys_new, datadict)) {
                    // res.add(new ImmutablePair<>(current[0], current[1]));
                    res.add(new ImmutablePair<>(entry.getKey(), tid));
                }
                for (int i = 1; i < current.length; i++) {
                    current[i] = null;
                }
            }
        }
        return res;
    }


    /******************************************************************************
     * new join and joinAll, s.t., RHSs could be <t_0>, <t_0, t_x>, x = 1, 2, ...
     * MUST contain t_0
     * beginTuple
     ******************************************************************************/
    public HashSet<ImmutablePair<Integer, Integer>> joinAll(HashMap<Integer, ArrayList<Integer>> beginTuplePairs,
                                                            ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                                                            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict,
                                                            int targetTID) {
        HashSet<ImmutablePair<Integer, Integer>> res = new HashSet<>();
        Integer[] current = new Integer[MAX_TUPLE_ID];
        HashSet<Integer> dataOtherTID = new HashSet<>();
        for (int i = 0; i < current.length; i++) {
            current[i] = null;
        }
        for (Map.Entry<Integer, ArrayList<Integer>> entry : beginTuplePairs.entrySet()) {
            current[0] = entry.getKey();
            dataOtherTID.clear();
            for (Integer tid : entry.getValue()) {
                current[1] = tid;
                join(current, 1, targetTID, keys_new, datadict, dataOtherTID);
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

    public boolean join(Integer[] current, int index, int targetTID, ArrayList<ImmutablePair<Integer, Integer>> keys_new,
                        HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict, HashSet<Integer> dataOtherTID) {
        if (index >= keys_new.size()) {
            dataOtherTID.add(current[targetTID]);
            return true;
        }
        ImmutablePair<Integer, Integer> pair = keys_new.get(index);
        ArrayList<Integer> next = datadict.get(MultiTuplesOrder.encode(pair)).get(current[pair.left]);
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
                term = join(current, index + 1, targetTID, keys_new, datadict, dataOtherTID);
                // backtrace
                current[pair.right] = null;
            } else if (current[pair.right] == e) {
                term = join(current, index + 1, targetTID, keys_new, datadict, dataOtherTID);
            }
        }
        return term;
    }


    private static Logger log = LoggerFactory.getLogger(MultiTuplesOrder.class);

}
