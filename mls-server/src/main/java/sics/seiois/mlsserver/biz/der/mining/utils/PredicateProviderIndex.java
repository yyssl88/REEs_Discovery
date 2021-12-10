package sics.seiois.mlsserver.biz.der.mining.utils;

import de.metanome.algorithm_integration.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateProvider;
import sics.seiois.mlsserver.biz.der.metanome.predicates.operands.ColumnOperand;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class PredicateProviderIndex implements Serializable {

    private static final long serialVersionUID = 637799890778402017L;

    private static PredicateProviderIndex instance;
    private static final Logger logger = LoggerFactory.getLogger(PredicateProviderIndex.class);


    private PredicateProviderIndex() {
        predicates_ = new HashMap<>();
    }

    public Map<String, Predicate> getPredicates_() {
        return predicates_;
    }

    // 所有谓词 p
    // p.toString(), p
    private Map<String, Predicate> predicates_;

    public void addPredicate(Predicate p) {
        if (!this.predicates_.containsKey(p.toString())) {
            this.predicates_.put(p.toString(), p);
        }
    }

    public Predicate getPredicate(Predicate p) {
        Predicate p_ = predicates_.get(p.toString());
        if (p_ == null) {
            predicates_.put(p.toString(), p);
        }
        return predicates_.get(p.toString());
    }

    public Predicate getPredicate(Predicate p, int index1, int index2) {
        Predicate pp = new Predicate(p, index1, index2);
        Predicate pp_ = predicates_.get(pp.toString());
        if (pp_ == null) {
            predicates_.put(pp.toString(), pp);
        }
        return pp;
    }

    public Predicate getConstantTemplate(Predicate p, int index1, int index2) {
        Predicate pp = new Predicate(p, index1, index2);
        pp.changeToConstantTemplate();
        Predicate pp_ = predicates_.get(pp.toString());
        if (pp_ == null) {
            predicates_.put(pp.toString(), pp);
        }
        return pp;
    }

    public Predicate getPredicate(Predicate p, int index1, int index2, String constant, int constantInt, String mloption, boolean ifConstantTemplate) {
        Predicate pp = null;
        if (p.isConstant() && ifConstantTemplate) {
            pp = new Predicate(p, index1, index2, constant, constantInt, mloption);
        } else {
            pp = new Predicate(p, index1, index2);
        }
        Predicate pp_ = predicates_.get(pp.toString());
        if (pp_ == null) {
            predicates_.put(pp.toString(), pp);
        }
        return pp;
    }

    public Long getSupportOnePredicate(Predicate p) {
        return this.predicates_.get(p.toString()).getSupport();
    }

    static {
        instance = new PredicateProviderIndex();
    }

    public static PredicateProviderIndex getInstance() {
        return instance;
    }
}
