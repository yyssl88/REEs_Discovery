package sics.seiois.mlsserver.biz.der.metanome.predicates;

import java.util.HashMap;
import java.util.Map;

import de.metanome.algorithm_integration.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.predicates.operands.ColumnOperand;



public class PredicateProvider {
    private static PredicateProvider instance;
    private static final Logger logger = LoggerFactory.getLogger(PredicateProvider.class);

    private Map<Operator, Map<ColumnOperand<?>, Map<ColumnOperand<?>, Predicate>>> predicates;

    private PredicateProvider() {
        predicates = new HashMap<>();
        predicates_ = new HashMap<>();
    }

    public Predicate getPredicate(Operator op, ColumnOperand<?> op1, ColumnOperand<?> op2) {
        Map<ColumnOperand<?>, Predicate> map = predicates.computeIfAbsent(op, a -> new HashMap<>()).computeIfAbsent(op1, a -> new HashMap<>());
        Predicate p = map.get(op2);
        if (p == null) {
            p = new Predicate(op, op1, op2);
            map.put(op2, p);
        }
        return p;
    }

    public Map<String, Predicate> getPredicates_() {
        return predicates_;
    }

    private Map<String, Predicate> predicates_;
    public Predicate getPredicate(Operator op, ColumnOperand<?> op1, ColumnOperand<?> op2, String constant) {
        String key = op.toString() + "-" + op1.toString() + "-" + op2.toString() + "-";
        if (constant != null) {
            key += constant;
        }
        Predicate p = predicates_.get(key);
        if (p == null) {
            p = new Predicate(op, op1, op2);
            if (constant != null) {
                p.setConstant(constant);
            }
            predicates_.put(key, p);
        }
        return p;
    }

    public Predicate getPredicate(Operator op, ColumnOperand<?> op1, ColumnOperand<?> op2, String constant, String mloption) {
        String key = op.toString() + "-" + op1.toString() + "-" + op2.toString() + "-Constant";
        if (constant != null) {
            key += constant + "-ML";
        }
        if (mloption != null) {
            key += mloption;
        }
        Predicate p = predicates_.get(key);
        if (p == null) {
            p = new Predicate(op, op1, op2);
            if (constant != null) {
                p.setConstant(constant);
            }
            if (mloption != null) {
                p.setML(mloption);
            }
            predicates_.put(key, p);
        }
        return p;
    }

    static {
        instance = new PredicateProvider();
    }

    public static PredicateProvider getInstance() {
        return instance;
    }
}
