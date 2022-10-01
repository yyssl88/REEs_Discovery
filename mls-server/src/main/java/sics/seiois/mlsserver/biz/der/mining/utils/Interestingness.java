package sics.seiois.mlsserver.biz.der.mining.utils;

import com.google.inject.internal.cglib.core.$ObjectSwitchCallback;
import shapeless.ops.nat;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraint;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.model.InterestingnessModel;
import sics.seiois.mlsserver.biz.der.mining.model.MLPFilterRegressor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    private double w_supp, w_conf, w_diver, w_succ, w_sub;

    private boolean ifNN; // if use linear model or the NN
    // Rule Interestingness model and Filter regressor
    InterestingnessModel interestingnessModel;
    MLPFilterRegressor mlpFilterRegressor;
    HashMap<String, Integer> predicatesHashID;


    // counter for attributes
    // 初始值为 列名 -> 100
    HashMap<String, Integer> counters;

    // 本来是所有表的所有行数和，现在是 maxOneRelationNum，最长表的长度
    long allCount;

    int featuresNum;

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

    public long getAllCount() {
        return this.allCount;
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

    /*
        ifNN: true means using NN, false means using linear model
     */
    public Interestingness(String tokenToIDFile, String interestingnessModelFile, String filterRegressionFile,
                           List<Predicate> allPredicates, long allCount, FileSystem hdfs, HashMap<String, Integer> predicatesHashID) {
            // load the rule interestingness model
            this.interestingnessModel = new InterestingnessModel(tokenToIDFile, interestingnessModelFile, hdfs);
            // load Filter regression model
            this.mlpFilterRegressor = new MLPFilterRegressor(filterRegressionFile, hdfs);
            this.predicatesHashID = predicatesHashID;
            ArrayList<Double> objWeights = this.interestingnessModel.getObjectiveWeights();
            this.featuresNum = objWeights.size();
            this.w_supp = objWeights.get(0);
            this.w_conf = objWeights.get(1);
            this.w_succ = objWeights.get(2);
            this.w_sub = objWeights.get(3);
            this.allCount = allCount;
            this.counters = new HashMap<>();
            for (Predicate p : allPredicates) {
                this.initCounter(p);
            }
    }


    public Interestingness(String tokenToIDFile, String interestingnessModelFile, String filterRegressionFile,
                           List<Predicate> allPredicates, long allCount, HashMap<String, Integer> predicatesHashID) {
        // load the rule interestingness model
        this.interestingnessModel = new InterestingnessModel(tokenToIDFile, interestingnessModelFile);
        // load Filter regression model
        this.mlpFilterRegressor = new MLPFilterRegressor(filterRegressionFile);
        this.predicatesHashID = predicatesHashID;
        ArrayList<Double> objWeights = this.interestingnessModel.getObjectiveWeights();
        this.featuresNum = objWeights.size();
        this.w_supp = objWeights.get(0);
        this.w_conf = objWeights.get(1);
        this.w_succ = objWeights.get(2);
        this.w_sub = objWeights.get(3);
        this.allCount = allCount;
        this.counters = new HashMap<>();
        for (Predicate p : allPredicates) {
            this.initCounter(p);
        }
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
        compute the interestingness score for a valid REE with Neural Network
     */
    public double computeInterestingnessNN(ArrayList<Predicate> reeLHS, Predicate rhs, double[][] reeobj) {
        return this.interestingnessModel.run(reeLHS, rhs, reeobj);
    }

    /*
        compute the upper bound of the interestingness score
     */
    public double computeUBSubjectiveScore(PredicateSet X, Predicate p_0) {
        int numPredicates = this.predicatesHashID.size();
        double[][] feature_vectors = new double[1][numPredicates * 2];
        // add P_sel
        for (Predicate p : X) {
            feature_vectors[0][this.predicatesHashID.get(p.toString().trim())] = 1.0;
        }
        feature_vectors[0][this.predicatesHashID.get(p_0.toString().trim())] = 1.0;
        // compute the UB
        return this.mlpFilterRegressor.run(feature_vectors);
    }


    /*
        compute the interestingness score for a valid REE
     */
//    private double computeInterestingness(double support_ratio, double confidence, double diversity, double succinctness, double subjective_fea) {
//        return this.w_supp * support_ratio + this.w_conf * confidence + this.w_diver * diversity +
//                this.w_succ * succinctness + this.w_sub * subjective_fea;
//    }

    public double computeInterestingness(DenialConstraint ree) {
        ArrayList<Predicate> pSel = new ArrayList<>();
        Predicate rhs = ree.getRHS();
        for (Predicate p : ree.getPredicateSet()) {
            if (p.equals(rhs)) {
                continue;
            }
            pSel.add(p);
        }
        double[][] objFeas = new double[1][this.featuresNum - 1];
        // 1. support
        objFeas[0][0] = ree.getSupport() * 1.0 / (this.allCount * this.allCount);
        // 2. confidence
        objFeas[0][1] = ree.getConfidence();
        // 3. conciseness
        objFeas[0][2] = 1.0 / pSel.size();
        return this.computeInterestingnessNN(pSel, rhs, objFeas);
    }

    /*
        compute the upper bound of a X -> p_0 with only one p_0
     */
    public double computeUB(double support_ratio_ofX, double confidence, PredicateSet X, Predicate p_0, String topKOption) {
        double subjectiveScore = this.interestingnessModel.getUBSubjectiveScore();
        if (topKOption.equals("allFiltering") && p_0 != null) {
            subjectiveScore = this.computeUBSubjectiveScore(X, p_0);
        }
//        double supp_ub = support_ratio_ofX;
        double supp_ub = support_ratio_ofX / allCount / allCount; //Math.sqrt(allCount);
        double confidence_ub = 1.0f;
        // double diver_ub = this.computeDeiversity(X);
        double succ_ub = 1.0 / X.size();
        return this.w_supp * supp_ub + this.w_conf * confidence_ub + this.w_succ * succ_ub + this.w_sub * subjectiveScore;
    }

    public static double sigmoid(double num) {
        return 1.0 / ( 1 + Math.exp(-num));
    }

}
