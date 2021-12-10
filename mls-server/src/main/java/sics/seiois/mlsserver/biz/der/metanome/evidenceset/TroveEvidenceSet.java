package sics.seiois.mlsserver.biz.der.metanome.evidenceset;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.model.MLSObjectLongHashMap;
import sics.seiois.mlsserver.model.PredicateConfig;

public class TroveEvidenceSet implements IEvidenceSet, Externalizable, KryoSerializable {
    private static final Logger logger = LoggerFactory.getLogger(TroveEvidenceSet.class);

    public TroveEvidenceSet() {
        tuple_ids = new HashMap<>();
        dupFK = new HashSet<>();
    }
    private MLSObjectLongHashMap<PredicateSet> sets = new MLSObjectLongHashMap<>();

    // store all joiable tuple ID pairs
    //private List<ImmutablePair<Integer, Integer> > tuple_ids;
    private Map<IBitSet, List<ImmutablePair<Integer, Integer>>> tuple_ids;

    // duplicate check
    private HashSet<String> dupFK;

    /*
     * read the evidence set file and parse it
     */
    public void generateEvidenceSet(ArrayList<HashSet<String>> predicateStrings,
                                    HashMap<String, Predicate> predicateStringsMap,
                                    ArrayList<Long> evidCounts) {
        for (int i = 0; i < predicateStrings.size(); i++) {
            Long count = evidCounts.get(i);
            PredicateSet ps = new PredicateSet();
            for (String p : predicateStrings.get(i)) {
                ps.add(predicateStringsMap.get(p));
            }
            this.add(ps, count);
        }

    }

    public void addFK(IBitSet k, ImmutablePair<Integer, Integer> fk) {
        // IBitSet k = key.getBitset();
        if (tuple_ids.get(k) != null) {
            tuple_ids.get(k).add(fk);
        } else {
            List<ImmutablePair<Integer, Integer>> ll = new ArrayList<>();
            ll.add(fk);
            tuple_ids.put(k, ll);
        }
    }

     private void removeFKDup() {
        Map<IBitSet, List<ImmutablePair<Integer, Integer>>> tuple_ids_ = new HashMap<>();
        for (Map.Entry<IBitSet, List<ImmutablePair<Integer, Integer>>> join : tuple_ids.entrySet()) {
            HashSet<String> dup = new HashSet<>();
            List<ImmutablePair<Integer, Integer>> ll = new ArrayList<>();
            for (ImmutablePair<Integer, Integer> pair : join.getValue()) {
                String key = pair.getLeft() + "<>" + pair.getRight();
                if (dup.contains(key) != true) {
                    ll.add(pair);
                    dup.add(key);
                }
            }
            tuple_ids_.put(join.getKey(), ll);
        }
        tuple_ids = tuple_ids_;
    }

    @Override
    public void print() {
        Iterator<PredicateSet> iter = this.iterator();
        while (iter.hasNext()) {
            PredicateSet ps = iter.next();
            System.out.println(ps.toString());
            System.out.println("---------------------------------------------------------------");
        }
    }

    @Override
    public void addFKMap(Map<IBitSet, List<ImmutablePair<Integer, Integer>>> map) {

        for (Map.Entry<IBitSet, List<ImmutablePair<Integer, Integer>>> joint : map.entrySet()) {
            for (ImmutablePair<Integer, Integer> pair : joint.getValue()) {
                addFK(joint.getKey(), pair);
            }
        }
    }

    @Override
    public List<ImmutablePair<Integer, Integer>> getList(IBitSet iBitSet) {
        return tuple_ids.get(iBitSet);
    }

    @Override
    public void refine() {
        MLSObjectLongHashMap<PredicateSet> sets_ = new MLSObjectLongHashMap<PredicateSet>();
        Map<IBitSet, List<ImmutablePair<Integer, Integer>>> tuple_ids_ = new HashMap<>();
        Iterator<PredicateSet> iter = this.iterator();
        while (iter.hasNext()) {
            PredicateSet ps = iter.next();
            PredicateSet ps_ = ps.refine();
            if (ps_ == null) {
                continue;
            }
            // check the MAP
            IBitSet bs = ps.getBitset();
            if (tuple_ids.get(bs) != null) {
                tuple_ids_.put(ps_.getBitset(), tuple_ids.get(bs));
            }
            // add to new sets
            sets_.adjustOrPutValue(ps_, sets.get(ps), sets.get(ps));
        }
        // reset
        sets = sets_;
        tuple_ids = tuple_ids_;

        this.removeFKDup();
    }

    @Override
    public Map<IBitSet, List<ImmutablePair<Integer, Integer>>> getFKMap () {
        return tuple_ids;
    }

    @Override
    public boolean add(PredicateSet predicateSet) {
        return this.add(predicateSet, 1);
    }

    @Override
    public boolean add(PredicateSet create, long count) {
        if(tuple_ids.containsKey(create.getBitset())) {
            List<ImmutablePair<Integer, Integer>> pair = tuple_ids.get(create.getBitset());
            if (pair.size() < PredicateConfig.getExampleSize()) {
                if(create.getTupleIds().size() < PredicateConfig.getExampleSize()) {
                    pair.addAll(create.getTupleIds());
                } else {
                    tuple_ids.put(create.getBitset(), create.getTupleIds());
                }
            }
        } else {
            tuple_ids.put(create.getBitset(), create.getTupleIds());
        }

        return sets.adjustOrPutValue(create, count, count) == count;
    }

    @Override
    public void add(IBitSet create, List<ImmutablePair<Integer, Integer> > list) {
        if (list == null || tuple_ids.get(create) != null) {
            return;
        }
        tuple_ids.put(create, list);
    }

    @Override
    public int getIndex(PredicateSet set) {
        return sets.getIndex(set);
    }

    @Override
    public PredicateSet getPredicateSet(Integer index) {
        return (PredicateSet) sets.getKeyByIndex(index);
    }

   @Override
   public void remove(PredicateSet create) {
        sets.remove(create);
   }

    @Override
    public void add(IBitSet create, ImmutablePair<Integer, Integer> pair) {
        if (tuple_ids.get(create) == null) {
            List<ImmutablePair<Integer, Integer>> ll = new ArrayList<>();
            ll.add(pair);
            tuple_ids.put(create, ll);
        } else {
            // check duplicate
            String key = create.toString() + "<>" + pair.getLeft().toString() + "<>" + pair.getRight().toString();
            if (dupFK.contains(key) != true) {
                tuple_ids.get(create).add(pair);
                dupFK.add(key);
            }
        }
    }

    @Override
    public long getCount(PredicateSet predicateSet) {
        return sets.get(predicateSet);
    }

    @Override
    public long getAllCount() {
        long tpCounts = 0;
        for (PredicateSet ps2 : this) {
            tpCounts += this.getCount(ps2);

        }
        return tpCounts;
    }

    public void incrementCount(PredicateSet predicateSet, long newEvidences) {
        sets.adjustOrPutValue(predicateSet, newEvidences, 0);
    }

    @Override
    public boolean adjustCount(PredicateSet predicateSet, long amount) {
        return sets.adjustValue(predicateSet, amount);
    }

    @Override
    public Iterator<PredicateSet> iterator() {
        return sets.keySet().iterator();
    }

    @Override
    public Set<PredicateSet> getSetOfPredicateSets() {
        return sets.keySet();
    }

    @Override
    public int size() {
        return sets.size();
    }

    @Override
    public boolean isEmpty() {
        return sets.isEmpty();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((sets == null) ? 0 : sets.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TroveEvidenceSet other = (TroveEvidenceSet) obj;
        if (sets == null) {
            if (other.sets != null) {
                return false;
            }
        } else if (!sets.equals(other.sets)) {
            return false;
        }
        return true;
    }


    @Override
    public void write(Kryo kryo, Output output) {
        for (PredicateSet ps : sets.keySet()) {
            ps.setSupport(sets.get(ps));//要把support给取回来
            ps.setTids(tuple_ids.get(ps.getBitset()));
            kryo.writeObject(output, ps);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        while (!input.eof()) {
            try {
                PredicateSet ps = kryo.readObject(input, PredicateSet.class);
                this.add(ps, ps.getSupport());//调用这个方法，累加
            } catch (Exception e) {
                logger.info("####kryo exception:" + e.getMessage(), e);
                break;//因为不知道多少个evidence，只能通过这个判断
            }
        }
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        for (PredicateSet ps : sets.keySet()) {
            ps.setSupport(sets.get(ps));//要把support给取回来
            ps.setTids(tuple_ids.get(ps.getBitset()));
            out.writeObject(ps);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        while (true) {
            try {
                PredicateSet ps = (PredicateSet) in.readObject();
                this.add(ps, ps.getSupport());//调用这个方法，累加
            } catch (EOFException e) {
                break;//因为不知道多少个evidence，只能通过这个判断
            } catch (OptionalDataException e) {
                if (e.eof) {
                    break;
                }
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
