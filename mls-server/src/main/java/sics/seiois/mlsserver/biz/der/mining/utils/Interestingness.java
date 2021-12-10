package sics.seiois.mlsserver.biz.der.mining.utils;

import shapeless.ops.nat;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraint;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class Interestingness implements Serializable {
    private static final long serialVersionUID = 123770436027404717L;
//    private float support_ratio;
//    private float confidence;
//    private float diversity;
//    private float succinctness;

//    private float subjective_value;

    private float w_supp, w_conf, w_diver, w_succ, w_sub;


    // counter for attributes
    // 初始值为 列名 -> 100
    HashMap<String, Integer> counters;

    // 本来是所有表的所有行数和，现在是 maxOneRelationNum，最长表的长度
    long allCount;

    /**
     * 在构造 Interestingness 调用
     * 传入所有的谓词
     *
     * @param p 谓词
     */
    // initial count
    private void initCounter(Predicate p) {
        // 列名
        String k_1 = p.getOperand1().getColumn().getName();
        String k_2 = p.getOperand2().getColumn().getName();

        if (!this.counters.containsKey(k_1)) {
            this.counters.put(k_1, 100);
        }
        if (!this.counters.containsKey(k_2)) {
            this.counters.put(k_2, 100);
        }
    }

    // set count for a REE
    public void updateCounter(PredicateSet ps) {
        HashSet<String> keys = new HashSet<>();
        for (Predicate p : ps) {
            String k_1 = p.getOperand1().getColumn().getName();
            String k_2 = p.getOperand2().getColumn().getName();
            keys.add(k_1);
            keys.add(k_2);
        }
        for (String k : keys) {
            this.counters.put(k, this.counters.get(k) + 1);
        }
    }

    // compute diversity
    private double computeDeiversity(PredicateSet ps) {
        double diver_ub_d = 0;
        for (Predicate p : ps) {
            int a = this.counters.get(p.getOperand1().getColumn().getName());
            int b = this.counters.get(p.getOperand2().getColumn().getName());
            diver_ub_d = Math.max(diver_ub_d, Math.max(a, b));
        }
        double diver_ub = 1.0 / (diver_ub_d + 1e-5);
        return diver_ub;
    }

    public Interestingness() {

    }

    /**
     * @param w_1           w_supp
     * @param w_2           w_conf
     * @param w_3           w_diver
     * @param w_4           w_succ
     * @param w_5           w_sub
     * @param allPredicates 所有谓词
     * @param allCount      本来是所有表的所有行数和，现在是 maxOneRelationNum，最长表的长度
     */
    public Interestingness(float w_1, float w_2, float w_3, float w_4, float w_5, List<Predicate> allPredicates, long allCount) {
        this.w_supp = w_1;
        this.w_conf = w_2;
        this.w_diver = w_3;
        this.w_succ = w_4;
        this.w_sub = w_5;
        this.counters = new HashMap<>();
        for (Predicate p : allPredicates) {
            this.initCounter(p);
        }
        this.allCount = allCount;
    }

    public Interestingness(ArrayList<Predicate> allPredicates, long allCount) {
        this.w_supp = 1;
        this.w_conf = 1;
        this.w_diver = 1;
        this.w_succ = 1;
        this.w_sub = 1;
        this.counters = new HashMap<>();
        for (Predicate p : allPredicates) {
            this.initCounter(p);
        }
        this.allCount = allCount;
    }

    /*
        compute the interestingness score for a valid REE
     */
    public double computeInterestingness(double support_ratio, double confidence, double diversity, double succinctness, double subjective_fea) {
        return this.w_supp * support_ratio + this.w_conf * confidence + this.w_diver * diversity +
                this.w_succ * succinctness + this.w_sub * subjective_fea;
    }

    public double computeInterestingness(DenialConstraint ree) {
//        double support_ratio = ree.getSupport() * 1.0 / allCount;
        double support_ratio = ree.getSupport() * 1.0 / allCount / Math.sqrt(allCount);
//        support_ratio = sigmoid(support_ratio);
        double confidence = ree.getConfidence();
        double diversity = this.computeDeiversity(ree.getPredicateSet());
        double succinctness = 1.0 / ree.getPredicateSet().size();
        double subjective_fea = ree.getSubjective_feature();
        return this.computeInterestingness(support_ratio, confidence, diversity, succinctness, subjective_fea);
    }

    /*
        compute the upper bound of a X -> p_0 with only one p_0
     */
    public double computeUB(double support_ratio_ofX, double confidence, PredicateSet X, Predicate p_0) {
//        double supp_ub = support_ratio_ofX;
        double supp_ub = support_ratio_ofX / Math.sqrt(allCount);
        double confidence_ub = 1.0f;
        double diver_ub = this.computeDeiversity(X);
        double succ_ub = 1.0 / X.size();
        double sub_ub = 1.0;
        return this.w_supp * supp_ub + this.w_conf * confidence_ub + this.w_diver * diver_ub +
                this.w_succ * succ_ub + this.w_sub * sub_ub;
    }

    public static double sigmoid(double num) {
        return 1.0 / ( 1 + Math.exp(-num));
    }

}
