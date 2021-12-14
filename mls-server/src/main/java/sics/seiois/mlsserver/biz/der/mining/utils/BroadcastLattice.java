package sics.seiois.mlsserver.biz.der.mining.utils;


import org.apache.hadoop.util.hash.Hash;
import shapeless.ops.nat;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateProvider;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * broadcast lattice data
 */

public class BroadcastLattice implements Serializable {
    private static final long serialVersionUID = 7770770360229647717L;

    private List<Predicate> allPredicates;
    private HashSet<IBitSet> invalidX;
    private HashMap<IBitSet, ArrayList<Predicate>> invalidRHSs;
    private HashMap<IBitSet, ArrayList<Predicate>> validXRHSs;
    private Interestingness interestingness;
    private double KthScore;
    private HashMap<PredicateSet, Double> suppRatios;
    private PredicateProviderIndex predicateProviderIndex;
    private String option;

    // for RL
    private int ifRL = 0;
    private int ifOnlineTrainRL;
    private int ifOfflineTrainStage;
    private String PI_path;
    private String RL_code_path;
    private boolean ifExistModel;
    private float learning_rate;
    private float reward_decay;
    private float e_greedy;
    private int replace_target_iter;
    private int memory_size;
    private int batch_size;

    private ArrayList<Predicate> allExistPredicates;

    private String table_name;
    private int N;


    public BroadcastLattice(List<Predicate> allPredicates, HashSet<IBitSet> invalidX,
                            HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                            HashMap<IBitSet, ArrayList<Predicate>> validXRHSs,
                            Interestingness interestingness, double KthScore,
                            HashMap<PredicateSet, Double> suppRatios, PredicateProviderIndex predicateProviderIndex,
                            String option) {
        this.allPredicates = allPredicates;
        this.invalidX = invalidX;
        this.invalidRHSs = invalidXRHSs;
        this.validXRHSs = validXRHSs;
        this.interestingness = interestingness;
        this.KthScore = KthScore;
        this.suppRatios = suppRatios;
        this.predicateProviderIndex = predicateProviderIndex;
        this.option = option;
    }

    public BroadcastLattice(List<Predicate> allPredicates, ArrayList<Predicate> allExistPredicates, HashSet<IBitSet> invalidX,
                            HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                            HashMap<IBitSet, ArrayList<Predicate>> validXRHSs,
                            Interestingness interestingness, double KthScore,
                            HashMap<PredicateSet, Double> suppRatios, PredicateProviderIndex predicateProviderIndex,
                            String option,
                            int ifRL, int ifOnlineTrainRL, int ifOfflineTrainStage, boolean ifExistModel, String python_path, String RL_code_path, float lr, float rd, float eg, int rtr, int ms, int bs,
                            String table_name, int N_num) {
        this(allPredicates, invalidX, invalidXRHSs, validXRHSs, interestingness, KthScore, suppRatios, predicateProviderIndex, option);
        this.allExistPredicates = allExistPredicates;
        this.ifRL = ifRL;
        this.ifOnlineTrainRL = ifOnlineTrainRL;
        this.ifOfflineTrainStage = ifOfflineTrainStage;
        this.ifExistModel = ifExistModel;
        this.PI_path = python_path;
        this.RL_code_path = RL_code_path;
        this.learning_rate = lr;
        this.reward_decay = rd;
        this.e_greedy = eg;
        this.replace_target_iter = rtr;
        this.memory_size = ms;
        this.batch_size = bs;
        this.table_name = table_name;
        this.N = N_num;
    }


    public List<Predicate> getAllPredicates () {
        return this.allPredicates;
    }

    public ArrayList<Predicate> getAllExistPredicates() {
        return this.allExistPredicates;
    }

    public HashSet<IBitSet> getInvalidX() {
        return this.invalidX;
    }

    public HashMap<IBitSet, ArrayList<Predicate>> getInvalidRHSs() {
        return this.invalidRHSs;
    }

    public HashMap<IBitSet, ArrayList<Predicate>> getValidXRHSs() {
        return this.validXRHSs;
    }

    public double getKthScore() {
        return this.KthScore;
    }

    public Interestingness getInterestingness() {
        return interestingness;
    }

    public HashMap<PredicateSet, Double> getSuppRatios() {
        return this.suppRatios;
    }

    public PredicateProviderIndex getPredicateProviderIndex() {
        return this.predicateProviderIndex;
    }

    public String getOption() {
        return this.option;
    }

    public int getIfRL() {
        return this.ifRL;
    }

    public int getIfOnlineTrainRL() {
        return this.ifOnlineTrainRL;
    }

    public int getIfOfflineTrainStage() {
        return this.ifOfflineTrainStage;
    }

    public String getPI_path() {
        return this.PI_path;
    }

    public String getRL_code_path() {
        return this.RL_code_path;
    }

    public boolean getIfExistModel() {
        return this.ifExistModel;
    }

    public float getLearning_rate() {
        return this.learning_rate;
    }

    public float getReward_decay() {
        return this.reward_decay;
    }

    public float getE_greedy() {
        return this.e_greedy;
    }

    public int getReplace_target_iter() {
        return this.replace_target_iter;
    }

    public int getMemory_size() {
        return this.memory_size;
    }

    public int getBatch_size() {
        return this.batch_size;
    }

    public String getTable_name() {
        return this.table_name;
    }

    public int getN() {
        return this.N;
    }

}
