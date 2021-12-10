package sics.seiois.mlsserver.biz.der.mining.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.tuple.ImmutablePair;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.Message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class LatticeVertex implements KryoSerializable {

    private static final long serialVersionUID = -49578945920075720L;
    private PredicateSet predicates;

    private PredicateSet rhss;

    private int maxTupleID;
    private int latest_tupleID_1;
    private int latest_tupleID_2;

    private String[] relations; // relations for each tuple id

    private HashMap<Predicate, Double> interestingnessUB = new HashMap<>();

    public String printCurrent() {
        return predicates.toString() + "   ->  " + rhss.toString();
    }


    public void copyWithOutRHSs(LatticeVertex lv) {
        for (Predicate p : lv.getPredicates()) {
            this.predicates.add(p);
        }
        this.maxTupleID = lv.getMaxTupleID();
        ImmutablePair<Integer, Integer> pair = lv.getLatestTupleIDs();
        this.latest_tupleID_1 = pair.left;
        this.latest_tupleID_2 = pair.right;
        this.relations = new String[this.maxTupleID + 1];
        for (int i = 0; i < this.maxTupleID + 1; i++) {
            this.relations[i] = lv.getTIDRelationName(i);
        }
        // this.interestingnessUB = new HashMap<>();
    }


    public void setInterestingnessUB(Predicate p, double ub) {
        this.interestingnessUB.put(p, ub);
    }

    public double getInterestingnessUB(Predicate p) {
        if (this.interestingnessUB.containsKey(p)) {
            return this.interestingnessUB.get(p);
        } else {
            return 0;
        }
    }

    public LatticeVertex(int maxTupleID) {
        this.predicates = new PredicateSet();
        this.rhss = new PredicateSet();
        this.maxTupleID = maxTupleID;
        this.relations = new String[this.maxTupleID + 1];
        for (int i = 0; i <= this.maxTupleID; i++) {
            this.relations[i] = null;
        }
    }

    public LatticeVertex(Predicate p, int maxTupleID) {
        this.predicates = new PredicateSet(p);
        this.rhss = new PredicateSet();
        this.maxTupleID = maxTupleID;
        this.relations = new String[this.maxTupleID + 1];
        for (int i = 0; i <= this.maxTupleID; i++) {
            this.relations[i] = null;
        }
    }

    public LatticeVertex(PredicateSet parents, Predicate p, int maxTupleID) {
        this.predicates = new PredicateSet(parents);
        this.predicates.add(p);
        this.rhss = new PredicateSet();
        this.maxTupleID = maxTupleID;
        this.relations = new String[this.maxTupleID + 1];
        for (int i = 0; i <= this.maxTupleID; i++) {
            this.relations[i] = null;
        }
    }

    public LatticeVertex(PredicateSet parents, Predicate p, ArrayList<Predicate> rhss, int maxTupleID) {
        this.predicates = new PredicateSet(parents);
        this.predicates.add(p);
        this.rhss = new PredicateSet();
        this.addRHSs(rhss);
        this.maxTupleID = maxTupleID;
        this.relations = new String[this.maxTupleID + 1];
        for (int i = 0; i <= this.maxTupleID; i++) {
            this.relations[i] = null;
        }
    }

    public LatticeVertex(LatticeVertex parents, Predicate p) {
        this.predicates = new PredicateSet(parents.getPredicates());
        this.predicates.add(p);
        this.rhss = new PredicateSet();
        this.addRHSs(parents.getRHSs());
        this.maxTupleID = parents.getMaxTupleID();
        this.relations = new String[this.maxTupleID + 1];
        for (int i = 0; i <= this.maxTupleID; i++) {
            this.relations[i] = parents.getTIDRelationName(i);
        }
        // initialize tuple ID pairs
        this.latest_tupleID_1 = parents.getLatestTupleIDs().left;
        this.latest_tupleID_2 = parents.getLatestTupleIDs().right;
    }

    public void updateTIDRelationName(String r_1, String r_2) {
        this.relations[this.latest_tupleID_1] = r_1;
        this.relations[this.latest_tupleID_2] = r_2;
    }

    public PredicateSet getPredicates() {
        return predicates;
    }

    public PredicateSet getRHSs() {
        return this.rhss;
    }

    public boolean ifContain(Predicate p) {
        return this.predicates.containsPredicate(p);
    }

    public void addRHSs(ArrayList<Predicate> rhss) {
        for (Predicate p : rhss) {
            this.rhss.add(p);
        }
    }

    public void addRHS(Predicate rhs) {
        rhss.add(rhs);
    }

    private void addRHSs(PredicateSet rhss) {
//        for (Predicate p : rhss) {
//            this.rhss.add(p);
//        }
        this.rhss.union(rhss.getBitset());
    }

    /*
        intersection with two sets of RHSs
     */
    public void adjustRHSs(ArrayList<Predicate> rhss) {
        if (rhss.size() == 0) {
            return;
        }
        HashSet<Predicate> rhss_dict = new HashSet<>();
        for (Predicate p : rhss) {
            rhss_dict.add(p);
        }
        PredicateSet rhss_new = new PredicateSet();
        for (Predicate rhs : this.rhss) {
            if (rhss_dict.contains(rhs)) {
                rhss_new.add(rhs);
            }
        }
        this.rhss = rhss_new;
    }

    public void adjustRHSs(PredicateSet rhss) {
//        if (rhss.size() == 0) {
//            return;
//        }

        this.rhss.and(rhss);

//        PredicateSet rhss_new = new PredicateSet();
//        for (Predicate rhs : this.rhss) {
//            if (rhss.containsPredicate(rhs)) {
//                rhss_new.add(rhs);
//            }
//        }
//        this.rhss = rhss_new;
    }

    // only for test
    private boolean testMoreThanOneT0T1() {
        int count = 0;
        for (Predicate p : this.predicates) {
            if (p.getIndex1() == 0 && p.getIndex2() == 1) {
                count++;
            }
        }
        if (count >= 2 && this.predicates.size() >= 3 && count != this.predicates.size()) {
            return true;
        }
        return false;
    }

    private boolean validRHS(Predicate rhs) {
        // only consider RHSs of <t_0>, <t_0, t_1> and <t_0, t_2>
        if (rhs.isConstant() && rhs.getIndex1() == 0) {
            return true;
        }
        if (!rhs.isConstant()) {
            if (rhs.getIndex1() == 0 && rhs.getIndex2() == 1) {
                return true;
            }
//            if (rhs.getIndex1() == 0 && rhs.getIndex2() == 2) {
//                return true;
//            }
        }
        return false;
    }

    /*
        generate work units for current lattice node, i.e., a candidate rule X -> Y
     */
    public ArrayList<WorkUnit> generateWorkUnits() {
        /*
        if (this.testMoreThanOneT0T1()) {
            System.out.println("Test More Than One T0 and T1");
        }
         */
        ArrayList<WorkUnit> workUnits = new ArrayList<>();
        for (Predicate p : this.rhss) {
            if (! this.predicates.containsPredicate(p)) {
                continue;
            }

            // only consider RHSs of <t_0>, <t_0, t_1> and <t_0, t_2>
            if (! this.validRHS(p)) {
                continue;
            }

            WorkUnit workUnit = new WorkUnit();
            workUnit.addRHS(p);
            // p is the RHS
            // In work unit, there is at least one <t_0, t_1>
            boolean containT0T1 = false;
            for (Predicate x : this.predicates) {
                if (x.equals(p)) {
                    continue;
                }
                if (x.getIndex1() == 0 && x.getIndex2() == 1) {
                    containT0T1 = true;
                }
                workUnit.addCurrent(x);
            }
            if (containT0T1) {
                // workUnit.setTransferData();
                workUnits.add(workUnit);
            }
        }
        return workUnits;
    }

    /*
        pruning
        prunedRHSs: (1) a RHS predicate satisfy X -> p_0
                    (2) a RHS predicate does not satisfy the support threshold
                    key:    predicates in current lattice vertex
                    value:  a set of predicates that need to be pruned
        // return: true: the node is still active; false: the node can be deleted
     */
    public void prune(HashMap<IBitSet, ArrayList<Predicate>> prunedRHSs) {
        if (prunedRHSs.containsKey(this.predicates.getBitset())) {
            ArrayList<Predicate> prunedPredicates = prunedRHSs.get(this.predicates.getBitset());
            for (Predicate p : prunedPredicates) {
                if (this.rhss.containsPredicate(p)) {
                    this.rhss.remove(p);
                }
            }
        }
    }

    public boolean checkEmpty() {
        return this.rhss.size() == 0;
    }

    public int getMaxTupleID() {
        return this.maxTupleID;
    }

    public void updateLatestTupleIDs(int tid_1, int tid_2) {
        this.latest_tupleID_1 = tid_1;
        this.latest_tupleID_2 = tid_2;
    }

    public ImmutablePair<Integer, Integer> getLatestTupleIDs() {
        ImmutablePair<Integer, Integer> pair = new ImmutablePair<>(this.latest_tupleID_1, this.latest_tupleID_2);
        return pair;
    }

//    public void updateLatestRelations(String r_1, String r_2) {
//        this.latest_relation_1 = r_1;
//        this.latest_relation_2 = r_2;
//    }

//    public ImmutablePair<String, String> getLatestRelations() {
//        ImmutablePair<String, String> pair = new ImmutablePair<>(this.latest_relation_1, this.latest_relation_2);
//        return pair;
//    }

    public String getTIDRelationName(int tid) {
        return this.relations[tid];
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, predicates);
        kryo.writeObject(output, rhss);
        kryo.writeObject(output, maxTupleID);
        kryo.writeObject(output, latest_tupleID_1);
        kryo.writeObject(output, latest_tupleID_2);
        kryo.writeObject(output, relations);
        kryo.writeObject(output, interestingnessUB);

    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.predicates = kryo.readObject(input, PredicateSet.class);
        this.rhss = kryo.readObject(input, PredicateSet.class);
        this.maxTupleID = kryo.readObject(input, Integer.class);
        this.latest_tupleID_1 = kryo.readObject(input, Integer.class);
        this.latest_tupleID_2 = kryo.readObject(input, Integer.class);
        this.relations = kryo.readObject(input, String[].class);
        this.interestingnessUB = kryo.readObject(input, HashMap.class);

    }


}
