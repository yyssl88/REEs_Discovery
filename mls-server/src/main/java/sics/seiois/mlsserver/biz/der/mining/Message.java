package sics.seiois.mlsserver.biz.der.mining;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Message implements Serializable {
    private static final long serialVersionUID = -7007743277256247070L;
    private static final Logger logger = LoggerFactory.getLogger(Message.class);
    private PredicateSet current;
    private PredicateSet validRHSs;
    private PredicateSet invalidRHSs; // RHS predicates that do not satisfy the support

    private PredicateSet candidateRHSs; // RHS predicates that X can be further expanded
    private ArrayList<Long> candidateSupports;

    private long currentSupp;
    private long currentSuppCP0;
    private long currentSuppCP1;
    // support and confidence for each valid RHS, current -> p_0, p_0 \in validRHSs
    private ArrayList<Double> confidences;
    private ArrayList<Long> supports;

    // only for efficiency
    transient private PredicateSet currentSet = null;

    // only for heavy skew partition method
    HashMap<Integer, Long> allCurrentRHSsSupport;

    public HashMap<Predicate, Long> getAllCurrentRHSsSupport() {
        HashMap<Predicate, Long> map = new HashMap<>();
        for (Map.Entry<Integer, Long> entry : this.allCurrentRHSsSupport.entrySet()) {
            Integer i = entry.getKey();
            Predicate predicate = PredicateSet.getPredicate(i);
            map.put(predicate,entry.getValue());
        }

        return map;
    }

    public void transformCurrent() {
        this.currentSet = new PredicateSet();
        for (Predicate p : current) {
            this.currentSet.add(p);
        }
    }

    public PredicateSet getCurrentSet() {
        return this.current;
    }

    public void updateAllCurrentRHSsSupport(Long[] counts, ArrayList<Predicate> p_arr) {
        for (int i = 0; i < p_arr.size(); i++) {
            Integer p = PredicateSet.getIndex(p_arr.get(i));
            if (this.allCurrentRHSsSupport.containsKey(p)) {
                this.allCurrentRHSsSupport.put(p, this.allCurrentRHSsSupport.get(p) + counts[i]);
            } else {
                this.allCurrentRHSsSupport.put(p, counts[i]);
            }
        }
    }

    public void updateOneCurrentRHSsSupport(Long count, Predicate rhs) {
        Integer rhsId = PredicateSet.getIndex(rhs);
        if (this.allCurrentRHSsSupport.containsKey(rhsId)) {
            this.allCurrentRHSsSupport.put(rhsId, this.allCurrentRHSsSupport.get(rhsId) + count);
        } else {
            this.allCurrentRHSsSupport.put(rhsId, count);
        }
    }

    public void updateAllCurrentRHSsSupport(HashSet<Predicate> p_arr) {
        for (Predicate predicate : p_arr) {
            Integer p = PredicateSet.getIndex(predicate);
            if (! this.allCurrentRHSsSupport.containsKey(p)) {
                this.allCurrentRHSsSupport.put(p, 0L);
            }
        }
    }

    public void mergeAllCurrentRHSsSupport(HashMap<Predicate, Long> allCurrentRHSsSupport_) {
        for (Map.Entry<Predicate, Long> entry : allCurrentRHSsSupport_.entrySet()) {
//            logger.info(">>>>rhs: {} | support: {}", entry.getKey(), entry.getValue());

            Integer key = PredicateSet.getIndex(entry.getKey());
            if (this.allCurrentRHSsSupport.containsKey(key)) {
                this.allCurrentRHSsSupport.put(key, this.allCurrentRHSsSupport.get(key) + entry.getValue());
            } else {
                this.allCurrentRHSsSupport.put(key, entry.getValue());
            }
//            logger.info(">>>>support become: {}", allCurrentRHSsSupport.get(key));
        }
    }

    public void mergeMessage(Message message) {
        this.currentSupp += message.getCurrentSupp();
        this.currentSuppCP0 += message.getCurrentSuppCP0();
        this.currentSuppCP1 += message.getCurrentSuppCP1();
        // set invalid RHSs
        for (Predicate rhs : message.getInvalidRHSs()) {
            this.invalidRHSs.add(rhs);
        }
        // set valid RHSs
        for (Predicate rhs : message.getValidRHSs()) {
            this.validRHSs.add(rhs);
        }
        this.mergeAllCurrentRHSsSupport(message.getAllCurrentRHSsSupport());

    }

    public void updateMessage(long support, double confidence, long maxTupleRelation) {
//        logger.info(">>>>update show rhs: {}", this.allCurrentRHSsSupport);
        for (Map.Entry<Integer, Long> entry : this.allCurrentRHSsSupport.entrySet()) {
            Predicate rhs = PredicateSet.getPredicate(entry.getKey());
            long supportXRHS = entry.getValue();
            if (rhs.isConstant()) {
                if (rhs.getIndex1() == 1) {
                    continue;
                }
                if (supportXRHS * maxTupleRelation < support) {
//                    logger.info(">>>> {} support : {} * {} < {}", rhs, supportXRHS, maxTupleRelation, support);
                    this.addInValidRHS(rhs);
                } else {
                    double conf = supportXRHS * 1.0 / this.currentSuppCP0;
                    logger.info(">>>> {} conf : {} | {}", rhs, conf, confidence);
                    if (conf >= confidence) {
                        this.addValidRHS(rhs, supportXRHS * maxTupleRelation, conf);
                    }
                }
            } else {
                if (supportXRHS < support) {
//                    logger.info(">>>> {} support : {} < {}", rhs, supportXRHS, support);
                    this.addInValidRHS(rhs);
                } else {
                    double conf = supportXRHS * 1.0 / this.currentSupp;
//                    logger.info(">>>> {} conf : {} | {}", rhs, conf, confidence);
                    if (conf >= confidence) {
                        this.addValidRHS(rhs, supportXRHS, conf);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return "[ Current : " + current.toString() + " ] " + " [ valid : " + validRHSs.toString() +
                " ] " + " [ invalid : " + invalidRHSs.toString() + " ] ";
    }

    public ArrayList<Long> getCandidateSupports() {
        return candidateSupports;
    }

    public ArrayList<Predicate> getCandidateRHSs() {
        return transfer(candidateRHSs);
    }

    public void setCurrentSupp(long currentSupp) {
        this.currentSupp = currentSupp;
    }

    public long getCurrentSupp() {
        return this.currentSupp;
    }

    public long getCurrentSuppCP0() {
        return this.currentSuppCP0;
    }

    public long getCurrentSuppCP1() {
        return this.currentSuppCP1;
    }

    public ArrayList<Double> getConfidences() {
        return this.confidences;
    }

    public ArrayList<Long> getSupports() {
        return this.supports;
    }

    public Message() {
        this.current = new PredicateSet();
        this.validRHSs = new PredicateSet();
        this.invalidRHSs = new PredicateSet();
        this.confidences = new ArrayList<>();
        this.candidateRHSs = new PredicateSet();
        this.candidateSupports = new ArrayList<>();
        this.supports = new ArrayList<>();

        this.allCurrentRHSsSupport = new HashMap<>();
    }

    public Message(ArrayList<Predicate> current, long support, long supportCP0, long supportCP1) {
        PredicateSet ps = new PredicateSet();
        for (Predicate predicate : current) {
            ps.add(predicate);
        }

        this.current = ps;
        this.validRHSs = new PredicateSet();
        this.invalidRHSs = new PredicateSet();
        this.confidences = new ArrayList<>();
        this.candidateRHSs = new PredicateSet();
        this.candidateSupports = new ArrayList<>();
        this.supports = new ArrayList<>();
        this.currentSupp = support;
        this.currentSuppCP0 = supportCP0;
        this.currentSuppCP1 = supportCP1;

        this.allCurrentRHSsSupport = new HashMap<>();
    }

    public void addValidRHS(Predicate p, long supp, double conf) {
        this.validRHSs.add(p);
        this.supports.add(supp);
        this.confidences.add(conf);
    }

    public void addCandidateRHS(Predicate p, long candSupp) {
        this.candidateRHSs.add(p);
        this.candidateSupports.add(candSupp);
    }

    public void addInValidRHS(Predicate p) {
        this.invalidRHSs.add(p);
    }

    public ArrayList<Predicate> getValidRHSs() {
        return transfer(this.validRHSs);
    }

    public ArrayList<Predicate> getInvalidRHSs() {
        return transfer(this.invalidRHSs);
    }

    public ArrayList<Predicate> getCurrent() {
        return transfer(this.current);
    }

    private ArrayList<Predicate> transfer(PredicateSet predicates){
        ArrayList<Predicate> ret = new ArrayList<>();
        for (Predicate predicate : predicates) {
            ret.add(predicate);
        }
        return ret;
    }

}
