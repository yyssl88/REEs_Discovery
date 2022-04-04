package sics.seiois.mlsserver.biz.der.mining;

import gnu.trove.list.array.TIntArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shapeless.ops.nat;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraint;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraintSet;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.input.InputLight;
import sics.seiois.mlsserver.biz.der.metanome.input.ParsedColumn;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.model.DQNMLP;
import sics.seiois.mlsserver.biz.der.mining.model.MLPFilterClassifier;
import sics.seiois.mlsserver.biz.der.mining.utils.*;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.model.PredicateExpressionModel;
import sics.seiois.mlsserver.model.SparkContextConfig;
import sics.seiois.mlsserver.service.impl.PredicateSetAssist;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.io.*;
import java.util.stream.Collectors;

public class ParallelRuleDiscoverySampling {
    // predicate set
    // each predicate attaches corresponding data in ParsedColumn
    private List<Predicate> allPredicates;
    private int K; // retrieve top-K rules
    private int k_score = -1; // the current k-th interestingness score
    private int maxTupleNum; // the maximum number of tuples in a rule
    private long support;
    private float confidence;
    private long maxOneRelationNum;
    private long allCount;
//    private Input input;
    private InputLight inputLight;

    public static int MIN_NUM_WORK_UNITS = 200000;

    public static int BATCH_SIZE = 500;

    // max number of partition of Lattice
    public static int NUM_LATTICE = 200;
    public static int MAX_CURRENT_PREDICTES = 7;

    public static int MAX_WORK_UNITS_PARTITION = 20;

    // generate all predicate set including different tuple ID pair
    private static final PredicateProviderIndex predicateProviderIndex = PredicateProviderIndex.getInstance();

    private PriorityQueue<DenialConstraint> topKREEsTemp;
    private DenialConstraint[] topKREEs;
    private Double[] topKREEScores;
    // the k-th UB score
    private Double kthUB;

    private HashSet<IBitSet> invalidX;
    private HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs;

    private HashMap<IBitSet, ArrayList<Predicate>> validXRHSs;
    private Map<PredicateSet, List<Predicate>> validConstantRule = new HashMap<>();

    private int ifPrune; // 1: use prune; 0: do not use prune

    private int if_conf_filter;
    private float conf_filter_thr;

    private int if_cluster_workunits;
    private int filter_enum_number;

    // interestingness
    Interestingness interestingness;
    private String topKOption;

    private String table_name;
    // use reinforcement learning for predicate association compute
    private int ifRL = 0;
    private int ifOnlineTrainRL = 1;
    private int ifOfflineTrainStage; // for offline RL; 1 at training stage, 0 at predicting stage
    private HashMap<PredicateSet, Long> ps_rewards;
    private ArrayList<PredicateSet> MEM;
    private String PI_path;
    private String RL_code_path;
    private int N;
    private int DeltaL;
    private float learning_rate;
    private float reward_decay;
    private float e_greedy;
    private int replace_target_iter;
    private int memory_size;
    private int batch_size;
    // for RL
    private ArrayList<Predicate> allExistPredicates; // whole predicates, of which indices are rather than 0 and 1
    private ArrayList<Integer> highSelectivityPredicateIndices; // the indices of predicates with supports less than 0.1 * (maxOneRelationNum * maxOneRelationNum - maxOneRelationNum)

    // set the DQNMLP model
    private MLPFilterClassifier dqnmlp;
    private HashMap<String, Integer> predicateDQNHashIDs = null;
    private boolean ifDQN;

    // set the DQNMLP
    void loadDQNModel(MLPFilterClassifier dqnmlp) throws IOException {
        if (dqnmlp == null) {
            this.dqnmlp = new MLPFilterClassifier();
        } else {
            this.dqnmlp = dqnmlp;
        }
        this.predicateDQNHashIDs = new HashMap<>();
//        for (int pid = 0; pid < this.allPredicates.size(); pid++) {
//            this.predicateDQNHashIDs.put(this.allPredicates.get(pid).toString(), pid);
//        }
        String data_name = null;
        for (Predicate p : this.allPredicates) {
            if (p.getTableName().contains("AMiner")) {
                data_name = "aminer";
                break;
            }
            if (p.getTableName().contains("Property")) {
                data_name = "property";
                break;
            }
        }
        if (data_name == null) {
            data_name = this.allPredicates.get(0).getTableName();
        }
//        String data_name = this.allPredicates.get(0).getTableName();
//        if (data_name.contains("AMiner")) {
//            data_name = "aminer";
//        } else if (data_name.contains("Property")) {
//            data_name = "property";
//        }
        loadAllPredicates(data_name);
    }

    // load all Predicates from file
//    private HashMap<Integer, Predicate> index2predicates;
    public void loadAllPredicates(String data_name) throws IOException {
//        this.index2predicates = new HashMap<>();
        FileSystem hdfs = FileSystem.get(new Configuration());
        String inputTxtPath = PredicateConfig.MLS_TMP_HOME + "allPredicates/" + data_name + "_predicates.txt";
        FSDataInputStream inputTxt = hdfs.open(new Path(inputTxtPath));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        String line;
        int k = 0;
        while ((line = bReader.readLine()) != null) {
//            Predicate p = PredicateBuilder.parsePredicateString(this.input, line);
//            this.index2predicates.put(k, p);
            this.predicateDQNHashIDs.put(line, k);
            k++;
        }
    }

    // load model locally
    void loadDQNModel(MLPFilterClassifier dqnmlp, String predicateHashIDsFile) throws IOException {
        if (dqnmlp == null) {
            this.dqnmlp = new MLPFilterClassifier();
        } else {
            this.dqnmlp = dqnmlp;
        }
        this.predicateDQNHashIDs = new HashMap<>();
//        for (int pid = 0; pid < this.allPredicates.size(); pid++) {
//            this.predicateDQNHashIDs.put(this.allPredicates.get(pid).toString(), pid);
//        }

        File inputTxt = new File(predicateHashIDsFile);
        FileReader fr = new FileReader(inputTxt);
        BufferedReader br = new BufferedReader(fr);
        String line;
        int k = 0;
        while ((line = br.readLine()) != null) {
//            Predicate p = PredicateBuilder.parsePredicateString(this.input, line);
//            this.index2predicates.put(k, p);
            this.predicateDQNHashIDs.put(line, k);
            k++;
        }

    }

    // load Rule Interestingness model NN version
    void loadInterestingnessModel(String tokenToIDFile, String interestingnessModelFile, String filterRegressionFile,
                                  FileSystem hdfs) {
        try {
            if (this.predicateDQNHashIDs == null) {
                String data_name = null;
                for (Predicate p : this.allPredicates) {
                    if (p.getTableName().contains("AMiner")) {
                        data_name = "aminer";
                        break;
                    }
                    if (p.getTableName().contains("Property")) {
                        data_name = "property";
                        break;
                    }
                }
                if (data_name == null) {
                    data_name = this.allPredicates.get(0).getTableName();
                }
                this.loadAllPredicates(data_name);
            }
        } catch (IOException e) {

        }
        // reconstruct the interestingness object
        this.interestingness = new Interestingness(tokenToIDFile, interestingnessModelFile,
                filterRegressionFile, this.allPredicates, this.allCount, hdfs, this.predicateDQNHashIDs);

    }

    void loadInterestingnessModel(String tokenToIDFile, String interestingnessModelFile, String filterRegressionFile, String predicateHashIDsFile) throws IOException {

        // 1. load all predicates
        this.predicateDQNHashIDs = new HashMap<>();
//        for (int pid = 0; pid < this.allPredicates.size(); pid++) {
//            this.predicateDQNHashIDs.put(this.allPredicates.get(pid).toString(), pid);
//        }

        File inputTxt = new File(predicateHashIDsFile);
        FileReader fr = new FileReader(inputTxt);
        BufferedReader br = new BufferedReader(fr);
        String line;
        int k = 0;
        while ((line = br.readLine()) != null) {
//            Predicate p = PredicateBuilder.parsePredicateString(this.input, line);
//            this.index2predicates.put(k, p);
            this.predicateDQNHashIDs.put(line, k);
            k++;
        }

        // 2. reconstruct the interestingness object
        this.interestingness = new Interestingness(tokenToIDFile, interestingnessModelFile, filterRegressionFile, this.allPredicates, this.allCount, this.predicateDQNHashIDs);

    }

    public ParallelRuleDiscoverySampling() {
    }

    public ParallelRuleDiscoverySampling(List<Predicate> predicates, int K, int maxTupleNum, long support,
                                         float confidence, long maxOneRelationNum, Input input, long allCount,
                                         float w_1, float w_2, float w_3, float w_4, float w_5, int ifPrune,
                                         int if_conf_filter, float conf_filter_thr, int if_cluster_workunits,
                                         int filter_enum_number) {
        this.allPredicates = predicates;
        this.K = K;
        this.maxTupleNum = maxTupleNum;
        this.support = support;
        this.confidence = confidence;
        this.maxOneRelationNum = maxOneRelationNum;
        this.allCount = allCount;
//        this.input = input;
        this.inputLight = new InputLight(input);

        this.ifPrune = ifPrune;

        this.if_conf_filter = if_conf_filter;
        this.conf_filter_thr = conf_filter_thr;

        this.if_cluster_workunits = if_cluster_workunits;

        this.filter_enum_number = filter_enum_number;

        // set support for each predicate;
        HashMap<String, HashMap<Integer, Long>> statistic = new HashMap<>();
        for (Predicate p : this.allPredicates) {
            // statistic
            // op1
            ParsedColumn<?> col1 = p.getOperand1().getColumn();
            if (!statistic.containsKey(col1.toStringData())) {
                HashMap<Integer, Long> temp = new HashMap<>();
                for (int i = 0; i < col1.getValueIntSize(); i++) {
                    if (!temp.containsKey(col1.getValueInt(i))) {
                        temp.put(col1.getValueInt(i), 1L);
                    } else {
                        temp.put(col1.getValueInt(i), temp.get(col1.getValueInt(i)) + 1);
                    }
                }
                statistic.put(col1.toStringData(), temp);
            }

            // op2
            ParsedColumn<?> col2 = p.getOperand2().getColumn();
            if (!statistic.containsKey(col2.toStringData())) {
                HashMap<Integer, Long> temp = new HashMap<>();
                for (int i = 0; i < col2.getValueIntSize(); i++) {
                    if (!temp.containsKey(col2.getValueInt(i))) {
                        temp.put(col2.getValueInt(i), 1L);
                    } else {
                        temp.put(col2.getValueInt(i), temp.get(col2.getValueInt(i)) + 1);
                    }
                }
                statistic.put(col2.toStringData(), temp);
            }
            // statistic support for predicate
            p.setSupportPredicate(statistic);
        }

        // a max heap to select top-K interesting REEs
        this.topKREEsTemp = new PriorityQueue<DenialConstraint>(new Comparator<DenialConstraint>() {
            @Override
            public int compare(DenialConstraint o1, DenialConstraint o2) {
                if (o1.getInterestingnessScore() < o2.getInterestingnessScore()) {
                    return 1;
                } else if (o1.getInterestingnessScore() > o2.getInterestingnessScore()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
        this.topKREEs = new DenialConstraint[this.K];
        this.topKREEScores = new Double[this.K];
        for (int i = 0; i < this.K; i++) {
            this.topKREEs[i] = null;
            this.topKREEScores[i] = -Double.MAX_VALUE;
        }
        // invalid predicate combinations
        this.invalidX = new HashSet<>();
        this.invalidXRHSs = new HashMap<>();
        // valid X and RHSs
        this.validXRHSs = new HashMap<>();

        // interestingness computation
        // this.interestingness = new Interestingness(w_1, w_2, w_3, w_4, w_5, allPredicates, this.allCount);
        this.interestingness = new Interestingness(w_1, w_2, w_3, w_4, w_5, allPredicates, this.maxOneRelationNum);

        // add to Predicate Set
        for (Predicate p : this.allPredicates) {
            predicateProviderIndex.addPredicate(p);
        }
    }

    public ParallelRuleDiscoverySampling(List<Predicate> predicates, int K, int maxTupleNum, long support,
                                         float confidence, long maxOneRelationNum, Input input, long allCount,
                                         float w_1, float w_2, float w_3, float w_4, float w_5, int ifPrune,
                                         int if_conf_filter, float conf_filter_thr, int if_cluster_workunits, int filter_enum_number,
                                         boolean ifDQN, MLPFilterClassifier dqnmlp) throws IOException {
        this(predicates, K, maxTupleNum, support,
                confidence, maxOneRelationNum, input, allCount,
                w_1, w_2, w_3, w_4, w_5, ifPrune, if_conf_filter, conf_filter_thr, if_cluster_workunits, filter_enum_number);

        this.ifDQN = ifDQN;
        this.loadDQNModel(dqnmlp);
    }

    // local version of DQN classifier
    public ParallelRuleDiscoverySampling(List<Predicate> predicates, int K, int maxTupleNum, long support,
                                         float confidence, long maxOneRelationNum, Input input, long allCount,
                                         float w_1, float w_2, float w_3, float w_4, float w_5, int ifPrune,
                                         int if_conf_filter, float conf_filter_thr, int if_cluster_workunits, int filter_enum_number,
                                         boolean ifDQN, MLPFilterClassifier dqnmlp, String predicateHashIDsFile) throws IOException {
        this(predicates, K, maxTupleNum, support,
                confidence, maxOneRelationNum, input, allCount,
                w_1, w_2, w_3, w_4, w_5, ifPrune, if_conf_filter, conf_filter_thr, if_cluster_workunits, filter_enum_number);

        this.ifDQN = ifDQN;
        this.loadDQNModel(dqnmlp, predicateHashIDsFile);
    }


    /*
        top-K interestingness rules discovery
     */

    public ParallelRuleDiscoverySampling(List<Predicate> predicates, int K, int maxTupleNum, long support,
                                         float confidence, long maxOneRelationNum, Input input, long allCount,
                                         float w_1, float w_2, float w_3, float w_4, float w_5, int ifPrune,
                                         int if_conf_filter, float conf_filter_thr, int if_cluster_workunits, int filter_enum_number,
                                         String topKOption, String tokenToIDFile, String interestingnessModelFile, String filterRegressionFile,
                                         FileSystem hdfs) throws IOException {
        this(predicates, K, maxTupleNum, support,
                confidence, maxOneRelationNum, input, allCount,
                w_1, w_2, w_3, w_4, w_5, ifPrune, if_conf_filter, conf_filter_thr, if_cluster_workunits, filter_enum_number);

        // topKOption is the indicator to switch different ablation study
        this.topKOption = topKOption;
        this.loadInterestingnessModel(tokenToIDFile, interestingnessModelFile, filterRegressionFile, hdfs);
    }

    public ParallelRuleDiscoverySampling(List<Predicate> predicates, int K, int maxTupleNum, long support,
                                         float confidence, long maxOneRelationNum, Input input, long allCount,
                                         float w_1, float w_2, float w_3, float w_4, float w_5, int ifPrune,
                                         int if_conf_filter, float conf_filter_thr, int if_cluster_workunits, int filter_enum_number,
                                         String topKOption, String tokenToIDFile, String interestingnessModelFile, String filterRegressionFile,
                                         String predicateHashIDsFile) throws IOException {
        this(predicates, K, maxTupleNum, support,
                confidence, maxOneRelationNum, input, allCount,
                w_1, w_2, w_3, w_4, w_5, ifPrune, if_conf_filter, conf_filter_thr, if_cluster_workunits, filter_enum_number);

        // topKOption is the indicator to switch different ablation study
        this.topKOption = topKOption;
        this.loadInterestingnessModel(tokenToIDFile, interestingnessModelFile, filterRegressionFile, predicateHashIDsFile);
    }

    private ArrayList<Predicate> applicationDrivenSelection(List<Predicate> predicates) {
        int whole_num_nonCons = 0;
        int whole_num_cons = 0;
        for (Predicate p : predicates) {
            if (p.isConstant()) {
                whole_num_cons++;
            } else {
                whole_num_nonCons++;
            }
        }

        ArrayList<Predicate> applicationRHSs = new ArrayList<>();
        // 1 - choose the first few non-constant rhss
//        int NUM_rhs = 4;
//        if (NUM_rhs > whole_num_nonCons) {
//            NUM_rhs = whole_num_nonCons;
//        }
//        logger.info("#### choose the first {} rhss", NUM_rhs);
//        int count = 0;
//        HashMap<String, Predicate> temp = new HashMap<>();
//        for (Predicate p : predicates) {
//            if (p.isConstant()) {
//                continue;
//            }
//            if (!temp.containsKey(p.toString())) {
//                temp.put(p.toString(), p);
//            }
//        }
//
//        for (Map.Entry<String, Predicate> entry : temp.entrySet()) {
//            applicationRHSs.add(entry.getValue());
//            count++;
//            if (count == NUM_rhs) {
//                break;
//            }
//        }


        // 2 - choose some rhss with minimum support value
//        int NUM_rhs = 4;
//        if (NUM_rhs > predicates.size()) {
//            NUM_rhs = predicates.size();
//        }
//        logger.info("#### choose {} RHSs with minimum support value", NUM_rhs);
//        ArrayList<Long> allSupports = new ArrayList<>();
//        for (Predicate p : predicates) {
//            allSupports.add(p.getSupport());
//        }
//        allSupports.sort(Comparator.naturalOrder());
//        Long minSupp = allSupports.get(NUM_rhs - 1);
//        for (Predicate p : predicates) {
//            if (p.getSupport() <= minSupp) {
//                applicationRHSs.add(p);
//            }
//        }
//        if (applicationRHSs.size() > NUM_rhs) { // maybe exist redundant supp value minSupp
//            applicationRHSs.subList(0, NUM_rhs);
//        }


        // 3 - choose 4 rhss: one min-supp, one max-supp, and two random rhss
//        logger.info("#### choose 4 RHSs: one min-supp, one max-supp, and two random RHSs");
//        Collections.sort(predicates, new Comparator<Predicate>() {
//            @Override
//            public int compare(Predicate o1, Predicate o2) {
//                return o1.toString().compareTo(o2.toString());
//            }
//        });
//        Random rand = new Random();
//        rand.setSeed(1234567);
//        HashSet<Integer> random_idx = new HashSet<>();
//        while (random_idx.size() < 2) {
//            int idx = rand.nextInt(predicates.size());
//            random_idx.add(idx);
//        }
//        for (int choose_idx : random_idx) {
//            applicationRHSs.add(predicates.get(choose_idx));
//        }
//        ArrayList<Long> allSupports = new ArrayList<>();
//        for (Predicate p : predicates) {
//            allSupports.add(p.getSupport());
//        }
//        allSupports.sort(Comparator.naturalOrder());
//        Long minSupp = allSupports.get(0);
//        Long maxSupp = allSupports.get(allSupports.size() - 1);
//        if (applicationRHSs.get(0).getSupport() == minSupp || applicationRHSs.get(1).getSupport() == minSupp) {
//            minSupp = allSupports.get(1);
//        }
//        if (applicationRHSs.get(0).getSupport() == maxSupp || applicationRHSs.get(1).getSupport() == maxSupp) {
//            maxSupp = allSupports.get(allSupports.size() - 2);
//        }
//        for (Predicate p : predicates) {
//            if (applicationRHSs.contains(p)) {
//                continue;
//            }
//            if (p.getSupport() == minSupp || p.getSupport() == maxSupp) {
//                applicationRHSs.add(p);
//            }
//            if (applicationRHSs.size() == 4) {
//                break;
//            }
//        }

        // 4. randomly choose some RHS.
//        int NUM_rhs = 4;
//        if (NUM_rhs > predicates.size()) {
//            NUM_rhs = predicates.size();
//        }
//        logger.info("#### randomly choose {} RHSs", NUM_rhs);
//        Collections.sort(predicates, new Comparator<Predicate>() {
//            @Override
//            public int compare(Predicate o1, Predicate o2) {
//                return o1.toString().compareTo(o2.toString());
//            }
//        });
//        Random rand = new Random();
//        rand.setSeed(1234567);
//        HashSet<Integer> random_idx = new HashSet<>();
//        while (random_idx.size() < NUM_rhs) {
//            int idx = rand.nextInt(predicates.size());
//            random_idx.add(idx);
//        }
//        for (int choose_idx : random_idx) {
//            applicationRHSs.add(predicates.get(choose_idx));
//        }

        // 5. randomly choose (n+m) RHSs, including n nonConstant and m constant RHSs
//        int NUM_Constant = 3;
//        int NUM_NonConstant = 2;
//        if (NUM_Constant > whole_num_cons) {
//            NUM_Constant = whole_num_cons;
//        }
//        if (NUM_NonConstant > whole_num_nonCons) {
//            NUM_NonConstant = whole_num_nonCons;
//        }
//        logger.info("#### randomly choose {} RHSs, including {} nonConstant RHSs and {} constant RHSs", NUM_Constant + NUM_NonConstant, NUM_NonConstant, NUM_Constant);
//        Collections.sort(predicates, new Comparator<Predicate>() {
//            @Override
//            public int compare(Predicate o1, Predicate o2) {
//                return o1.toString().compareTo(o2.toString());
//            }
//        });
//        Random rand = new Random();
//        rand.setSeed(1234567);
//        HashSet<Integer> random_idx = new HashSet<>();
//        int constant_num = 0;
//        int nonConstant_num = 0;
//        while (random_idx.size() < (NUM_Constant + NUM_NonConstant)) {
//            int idx = rand.nextInt(predicates.size());
//            if (predicates.get(idx).isConstant()) {
//                if (constant_num < NUM_Constant && !random_idx.contains(idx)) {
//                    random_idx.add(idx);
//                    constant_num = constant_num + 1;
//                }
//            } else {
//                if (nonConstant_num < NUM_NonConstant && !random_idx.contains(idx)) {
//                    random_idx.add(idx);
//                    nonConstant_num = nonConstant_num + 1;
//                }
//            }
//        }
//        for (int choose_idx : random_idx) {
//            applicationRHSs.add(predicates.get(choose_idx));
//        }

        // 6. randomly choose some non-constant RHS.
//        int NUM_NonConstant = 4;
//        if (NUM_NonConstant > whole_num_nonCons) {
//            NUM_NonConstant = whole_num_nonCons;
//        }
//        logger.info("#### randomly choose {} non-constant RHSs", NUM_NonConstant);
//        Collections.sort(predicates, new Comparator<Predicate>() {
//            @Override
//            public int compare(Predicate o1, Predicate o2) {
//                return o1.toString().compareTo(o2.toString());
//            }
//        });
//        Random rand = new Random();
//        rand.setSeed(1234567);
//        HashSet<Integer> random_idx = new HashSet<>();
//        while (random_idx.size() < NUM_NonConstant) {
//            int idx = rand.nextInt(predicates.size());
//            if (!predicates.get(idx).isConstant()) {
//                random_idx.add(idx);
//            }
//        }
//        for (int choose_idx : random_idx) {
//            applicationRHSs.add(predicates.get(choose_idx));
//        }

        // 7. use all non-constant predicates as RHSs
        logger.info("#### choose all non-constant RHSs");
        for (Predicate p : predicates) {
            if (!p.isConstant()) {
                applicationRHSs.add(p);
            }
        }

        // 8. use all predicates as RHSs
//        logger.info("#### choose all RHSs");
//        for (Predicate p : predicates) {
////            if (!p.isConstant() && p.getOperand1().getColumnLight().getName().contains("scheduled_service")) {
////                continue;
////            }
//            applicationRHSs.add(p);
//        }

        // 9. test NCVoter
//        logger.info("#### choose voting_intention as RHS");
//        for (Predicate p : predicates) {
//            if (p.getOperand1().toString(0).equals("t0.voting_intention")) {
//                applicationRHSs.add(p);
//            }
//        }

        // 10. test airports - fix RHSs
//        for (Predicate p : predicates) {
//            if (p.isConstant()) {
//                continue;
//            }
//            if (p.getOperand1().getColumnLight().getName().contains("wikipedia_link") ||
//                p.getOperand1().getColumnLight().getName().contains("home_link") ||
//                p.getOperand1().getColumnLight().getName().contains("iso_region") ||
//                p.getOperand1().getColumnLight().getName().contains("type")) {
//                applicationRHSs.add(p);
//            }
//        }

        // 11. test inspection - fix RHSs
//        for (Predicate p : predicates) {
//            if (p.isConstant()) {
//                continue;
//            }
//            if (p.getOperand1().getColumnLight().getName().contains("City") ||
//                p.getOperand1().getColumnLight().getName().contains("Inspection_ID") ||
//                p.getOperand1().getColumnLight().getName().contains("DBA_Name") ||
//                p.getOperand1().getColumnLight().getName().contains("Facility_Type") ||
//                p.getOperand1().getColumnLight().getName().contains("Latitude") ||
//                p.getOperand1().getColumnLight().getName().contains("Longitude") ||
//                p.getOperand1().getColumnLight().getName().contains("Results")) {
//                applicationRHSs.add(p);
//            }
//        }

        // 12. test ncvoter - fix RHSs
//        for (Predicate p : predicates) {
//            if (p.isConstant()) {
//                continue;
//            }
//            if (p.getOperand1().getColumnLight().getName().equals("city") ||
//                p.getOperand1().getColumnLight().getName().equals("city_id2") ||
//                p.getOperand1().getColumnLight().getName().equals("county_id") ||
//                p.getOperand1().getColumnLight().getName().equals("id")) {
//                applicationRHSs.add(p);
//            }
//        }

        logger.info("applicationRHSs size : {}", applicationRHSs.size());
        for (Predicate p : applicationRHSs) {
            logger.info("applicationRHSs: {}", p.toString());
        }
        return applicationRHSs;
    }

    /**
     * if RHS only contain non-constant predicates, then filter the irrelevant predicates
     * */
    private void filterIrrelevantPredicates(ArrayList<Predicate> applicationRHSs, List<Predicate> allPredicates) {
        // filter the predicates that are irrelevant to rhs
        boolean allNonConstant = true;
        HashSet<String> relationRHS = new HashSet<>();
        for (Predicate p : applicationRHSs) {
            if (p.isConstant()) {
                allNonConstant = false;
                break;
            }
            relationRHS.add(p.getOperand1().getColumn().getTableName());
            relationRHS.add(p.getOperand2().getColumn().getTableName());
        }
//        for (String name : relationRHS) {
//            logger.info("Table name: {}", name);
//        }
        if (allNonConstant) {
            ArrayList<Predicate> removePredicates = new ArrayList<>();
            for (Predicate p : allPredicates) {
                String name_1 = p.getOperand1().getColumn().getTableName();
                String name_2 = p.getOperand2().getColumn().getTableName();
//                logger.info("currPredicate: {}, name_1: {}, name_2: {}", p, name_1, name_2);
                if (!relationRHS.contains(name_1) || !relationRHS.contains(name_2)) {
                    removePredicates.add(p);
                }
            }
            logger.info("#### Filter Irrelevant Predicates size: {}", removePredicates.size());
            for (Predicate p : removePredicates) {
//                logger.info("remove predicate: {}", p);
                allPredicates.remove(p);
            }
        }
    }

    // remove the constant predicates related to Property_Feature dataset
    private void removePropertyFeatureCPredicates(List<Predicate> allPredicates) {
        ArrayList<Predicate> removePredicates = new ArrayList<>();
        for (Predicate p : allPredicates) {
            if (p.isConstant() && p.getOperand1().getColumn().getTableName().contains("Property_Features")) {
                removePredicates.add(p);
            }
        }
        logger.info("#### Filter Enum Constant Predicates size of Table Property_Features for X: {}", removePredicates.size());
        for (Predicate p : removePredicates) {
            allPredicates.remove(p);
        }
    }

    private void removeEnumPredicates(List<Predicate> allPredicates, ArrayList<Predicate> allExistPredicates) {
        ArrayList<Predicate> removePredicates = new ArrayList<>();
        for (Predicate p : allPredicates) {
            if (!p.isConstant()) {
                continue;
            }
            if (p.getOperand1().getColumnLight().getUniqueConstantNumber() <= this.filter_enum_number ||
                p.getOperand2().getColumnLight().getUniqueConstantNumber() <= this.filter_enum_number) {
                removePredicates.add(p);
            }
        }
        logger.info("#### Filter Enum Constant Predicates size for X: {}", removePredicates.size());
        for (Predicate p : removePredicates) {
            allPredicates.remove(p);
            allExistPredicates.remove(p);
        }
    }

    private void removeEnumPredicates(List<Predicate> allPredicates) {
        ArrayList<Predicate> removePredicates = new ArrayList<>();
        for (Predicate p : allPredicates) {
            if (!p.isConstant()) {
                continue;
            }
            if (p.getOperand1().getColumnLight().getUniqueConstantNumber() <= this.filter_enum_number ||
                p.getOperand2().getColumnLight().getUniqueConstantNumber() <= this.filter_enum_number) {
                removePredicates.add(p);
            }
        }
        logger.info("#### Filter Enum Constant Predicates size for RHSs: {}", removePredicates.size());
        for (Predicate p : removePredicates) {
            allPredicates.remove(p);
        }
    }

    /**
     * merge work units by X
     */
    private ArrayList<WorkUnit> mergeWorkUnits(ArrayList<WorkUnit> workUnits) {
        ArrayList<WorkUnit> workUnits_res = new ArrayList<>();
        HashMap<PredicateSet, PredicateSet> dups = new HashMap<>();
        for (WorkUnit wu : workUnits) {
            if (dups.containsKey(wu.getCurrrent())) {
                dups.get(wu.getCurrrent()).or(wu.getRHSs());
            } else {
                PredicateSet ps = new PredicateSet(wu.getRHSs());
                dups.put(wu.getCurrrent(), ps);
            }
        }
        for (Map.Entry<PredicateSet, PredicateSet> entry : dups.entrySet()) {
            WorkUnit workUnit = new WorkUnit();
            for (Predicate p : entry.getKey()) {
                workUnit.addCurrent(p);
            }
            for (Predicate p : entry.getValue()) {
                workUnit.addRHS(p);
            }
            workUnits_res.add(workUnit);
        }
        return workUnits_res;
    }

    public void levelwiseRuleDiscovery(String taskId, SparkSession spark, SparkContextConfig sparkContextConfig) {
        // 1. initialize the 1st level combinations

        this.table_name = "";
        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long) inputLight.getLineCounts()[i]);
            this.table_name += inputLight.getNames().get(i);
            this.table_name += "_";
        }
        this.table_name = this.table_name.substring(0, this.table_name.length() - 1); // remove last "_"
        logger.info("#### table_name: {}", this.table_name);


//        if (table_name.contains("Property_Features")) {
//            removePropertyFeatureCPredicates(this.allPredicates);
//        }

        this.prepareAllPredicatesMultiTuples();

        List<Predicate> tmp_allPredicates = new ArrayList<>();
        for (Predicate p : this.allPredicates) {
            tmp_allPredicates.add(p);
        }
        removeEnumPredicates(tmp_allPredicates);
        ArrayList<Predicate> applicationRHSs = this.applicationDrivenSelection(tmp_allPredicates);

        // remove predicates that are irrelevant to RHSs
        if (this.maxTupleNum <= 2) {
            filterIrrelevantPredicates(applicationRHSs, this.allPredicates);
        }

        // remove constant predicates of enumeration type
        removeEnumPredicates(this.allPredicates, this.allExistPredicates);

        logger.info("Parallel Mining with Predicates size {} and Predicates {}", this.allPredicates.size(), this.allPredicates);
        int cpsize = 0;
        int psize = 0;
        for (Predicate p : this.allPredicates) {
            if (p.isConstant()) {
                cpsize++;
            } else {
                psize++;
            }
        }
        logger.info("#### after filtering, there are {} predicates, constant size: {}, non-constant size: {}", this.allPredicates.size(), cpsize, psize);


        Lattice lattice = new Lattice(this.maxTupleNum);


        lattice.initialize(this.allPredicates, this.maxTupleNum, applicationRHSs);

        int level = 0;

        String option = "original";
        if (this.ifPrune == 0) {
            option = "none";
        }

        // if there exist trained model. Since Psel could be empty initially, at level 0, leading to no model trained.
        boolean ifExistModel = true;

        // for offline RL
        String sequenceResults = "";
        int count_seq = 0;
        boolean ifReachN = false;

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//        Broadcast<List<Predicate>> broadAllPredicate = sc.broadcast(allPredicates);

        PredicateSetAssist psAssist = new PredicateSetAssist();
        psAssist.setIndexProvider(PredicateSet.indexProvider);//在这里设置，这个值肯定是确定了的
        psAssist.setBf(PredicateSet.bf);
        psAssist.setTaskId(taskId);
        Broadcast<PredicateSetAssist> bcpsAssist = sc.broadcast(psAssist);

        while (lattice != null && level <= MAX_CURRENT_PREDICTES) {
            // print lattice
            // collect a set of work units
            logger.info("Search level : {}", level);

            // train RL model
            logger.info("ifRL: {}", ifRL);
            if (this.ifRL == 1 && this.ifOnlineTrainRL == 1 && level % this.DeltaL == 0) {
                logger.info("#### Online Train RL Model...");
                long beginRLTime = System.currentTimeMillis();

                String sequence;
                ArrayList<PredicateSet> Psel = new ArrayList<>();
                ArrayList<Predicate> next_p = new ArrayList<>();
                ArrayList<Long> reward = new ArrayList<>();
                ArrayList<PredicateSet> RHSs = new ArrayList<>();
                // save the predicate indices in Psel
                ArrayList<HashSet<Integer>> Psel_indices_set = new ArrayList<>();
                // if there exists no legal next_p, remove current Psel
                ArrayList<Integer> removePselIndex = new ArrayList<>();

//                int rhss_size = 0;
//                for (Map.Entry<IBitSet, LatticeVertex> entry: lattice.getLatticeLevel().entrySet()) {
//                    rhss_size = entry.getValue().getRHSs().size();
//                    break;
//                }
//                int seq_num = Math.min(lattice.getLatticeLevel().size() * (rhss_size - 1), this.N);

                int currLVNum = lattice.getLatticeLevel().size();
                int seq_num = Math.min(currLVNum, this.N);
                logger.info("#### the number of sequences for RL is {}", seq_num);

                // select Psel randomly, and select next_p randomly at level 0
                HashSet<Integer> nonConstantIndices = new HashSet<>();
                if (level == 0) { // make sure the initial Psel contains non-constant predicate
                    int tmp_idx = 0;
                    for (Map.Entry<IBitSet, LatticeVertex> entry : lattice.getLatticeLevel().entrySet()) {
                        for (Predicate p : entry.getValue().getPredicates()) {
                            if (!p.isConstant()) {
                                nonConstantIndices.add(tmp_idx);
                                break;
                            }
                        }
                        tmp_idx = tmp_idx + 1;
                    }
                    seq_num = Math.min(seq_num, nonConstantIndices.size());
                }
                HashSet<Integer> Psel_idx = new HashSet<>();
                while (Psel_idx.size() < seq_num) {
                    int rand_num = (int) (Math.random() * currLVNum);
                    if (level != 0) {
                        Psel_idx.add(rand_num);
                        continue;
                    }
                    if (nonConstantIndices.contains(rand_num)) {
                        Psel_idx.add(rand_num);
                    }
                }
                int tmp_i = 0;
                for (Map.Entry<IBitSet, LatticeVertex> entry : lattice.getLatticeLevel().entrySet()) {
                    if (!Psel_idx.contains(tmp_i)) {
                        tmp_i = tmp_i + 1;
                        continue;
                    }

                    // in case of generating empty workunits for reward computation
                    if (entry.getValue().getRHSs().size() == 0) {
                        tmp_i = tmp_i + 1;
                        continue;
                    }

                    PredicateSet tmp_set = new PredicateSet();
                    HashSet<Integer> tmp_idx_set = new HashSet<>();
                    for (Predicate p : entry.getValue().getPredicates()) {
                        tmp_set.add(p);
                        tmp_idx_set.add(p.getIndex1());
                        if (!p.isConstant()) {
                            tmp_idx_set.add(p.getIndex2());
                        }
                    }
                    Psel.add(tmp_set);
                    Psel_indices_set.add(tmp_idx_set);
                    RHSs.add(new PredicateSet(entry.getValue().getRHSs()));

                    if (level == 0) { // initial, select action randomly; LHS only contains one predicate
                        int ifExistLegalNextP = 0;
                        int idx = 0;
                        for (Predicate p : this.allExistPredicates) {
                            if (tmp_set.containsPredicate(p)) {
                                idx = idx + 1;
                                continue;
                            }
                            // check whether p contains index in Psel
                            if (p.isConstant() && !tmp_idx_set.contains(p.getIndex1())) {
                                idx = idx + 1;
                                continue;
                            }
                            if (!p.isConstant() && !tmp_idx_set.contains(p.getIndex1()) &&
                                    !tmp_idx_set.contains(p.getIndex2())) {
                                idx = idx + 1;
                                continue;
                            }
                            // check support
                            if (!this.highSelectivityPredicateIndices.contains(idx)) {
                                idx = idx + 1;
                                continue;
                            }
                            idx = idx + 1;
                            next_p.add(p);
                            ifExistLegalNextP = 1;
                            break;
                        }
                        if (ifExistLegalNextP == 0) {
                            removePselIndex.add(Psel.size() - 1);
                        }
                    }
                    tmp_i = tmp_i + 1;
                }

                String legal_nextP_indices = getLegalNextPIndices(this.allExistPredicates, this.highSelectivityPredicateIndices, Psel_indices_set);

                // select next_p using Mcorr at level DeltaL
                if (level > 0 && !legal_nextP_indices.equals("")) {
                    // select action with maximum predicted reward by current model Mcorr.
                    sequence = transformSequence(this.allExistPredicates, Psel, null, null);
                    long beginUseRLTime = System.currentTimeMillis();
                    String predict_actions = useRL(sequence, 0, 0, legal_nextP_indices,
                            this.allExistPredicates.size(), this.PI_path, this.RL_code_path, this.learning_rate, this.reward_decay,
                            this.e_greedy, this.replace_target_iter, this.memory_size, this.batch_size, this.table_name, this.N); // form: "pid1;pid2;pid3;..."
                    long predictRLTime = System.currentTimeMillis() - beginUseRLTime;
                    logger.info("#### finish predicting next predicate at level {} with step 0, using time: {}", level, predictRLTime);
                    int rm_idx = 0;
                    for (String action : predict_actions.split(";")) {
                        if (action.equals("-1")) {
                            removePselIndex.add(rm_idx);
                            rm_idx = rm_idx + 1;
                            continue;
                        }
                        next_p.add(this.allExistPredicates.get(Integer.parseInt(action)));
                        rm_idx = rm_idx + 1;
                    }
                }

                // remove Psel without legal next_p
                for (tmp_i = removePselIndex.size() - 1; tmp_i >= 0; tmp_i = tmp_i - 1) {
                    int rm_idx = removePselIndex.get(tmp_i);
                    Psel.remove(rm_idx);
                    Psel_indices_set.remove(rm_idx);
                    RHSs.remove(rm_idx);
                }
                removePselIndex.clear();

                if (legal_nextP_indices.equals("")) { // when N = 1, and there's no legal next p
                    Psel.clear();
                }

                // compute supports and rewards, save to MEM
                for (tmp_i = 0; tmp_i < Psel.size(); tmp_i = tmp_i + 1) {
                    reward.add(0L);
                }
                long beginRewardTime = System.currentTimeMillis();
                computeRewardAndSaveMEM(Psel, next_p, reward, RHSs,
                        option, taskId, spark, sparkContextConfig, tupleNumberRelations, bcpsAssist);
                long computeRewardTime = System.currentTimeMillis() - beginRewardTime;
                logger.info("#### finish computing reward at level {} with step 0, using time: {}", level, computeRewardTime);

                Boolean ifContinue = true;
                if (Psel.size() == 0) {
                    ifContinue = false;
                }
                if (level == 0 && !ifContinue) {
                    ifExistModel = false;
                }

                // send (Psel_i, p_i, reward_i), then train Mcorr_i+1 and save it. i++
                sequence = transformSequence(this.allExistPredicates, Psel, next_p, reward);
                int ifInitTrain = 0;
                if (level == 0) {
                    ifInitTrain = 1;
                }
                if (ifContinue) {
                    long beginUseRLTime = System.currentTimeMillis();
                    useRL(sequence, 1, ifInitTrain, "", this.allExistPredicates.size(),
                            this.PI_path, this.RL_code_path, this.learning_rate, this.reward_decay,
                            this.e_greedy, this.replace_target_iter, this.memory_size, this.batch_size, this.table_name, this.N);
                    long useRLTime = System.currentTimeMillis() - beginUseRLTime;
                    logger.info("#### finish training RL model at level {} with step 0, using time: {}", level, useRLTime);
                }

                // the left (DeltaL - 1) step
                for (int step = 1; step < this.DeltaL; step = step + 1) {
                    if (!ifContinue) {
                        break;
                    }
                    // 1. For Psel_i+1, select action with maximum predicted reward by current model Mcorr_i+1.
                    for (tmp_i = 0; tmp_i < Psel.size(); tmp_i = tmp_i + 1) { // update Psel_i to Psel_i+1
                        Predicate tmp_p = next_p.get(tmp_i);
                        Psel.get(tmp_i).add(tmp_p);
                        Psel_indices_set.get(tmp_i).add(tmp_p.getIndex1());
                        if (!tmp_p.isConstant()) {
                            Psel_indices_set.get(tmp_i).add(tmp_p.getIndex2());
                        }
                    }
                    // remove duplicate Psel
                    deduplicateSequence(Psel, RHSs, Psel_indices_set);
                    // remove invalid Psel, minimal RHSs, and invalidXRHSs RHSs
                    pruneSequence(Psel, RHSs, Psel_indices_set);
                    if (Psel.size() == 0) {
                        ifContinue = false;
                        break;
                    }
                    legal_nextP_indices = getLegalNextPIndices(this.allExistPredicates, this.highSelectivityPredicateIndices, Psel_indices_set);
                    if (legal_nextP_indices.equals("")) { // when N = 1, and there's no legal next p
                        Psel.clear();
                        ifContinue = false;
                        break;
                    }
                    sequence = transformSequence(this.allExistPredicates, Psel, null, null);
                    long beginUseRLTime = System.currentTimeMillis();
                    String predict_actions = useRL(sequence, 0, 0, legal_nextP_indices,
                            this.allExistPredicates.size(), this.PI_path, this.RL_code_path, this.learning_rate, this.reward_decay,
                            this.e_greedy, this.replace_target_iter, this.memory_size, this.batch_size, this.table_name, this.N); // form: "pid1;pid2;pid3;..."
                    long predictRLTime = System.currentTimeMillis() - beginUseRLTime;
                    logger.info("#### finish predicting next predicate at level {} with step {}, using time: {}", level, step, predictRLTime);
                    next_p.clear();
                    int rm_idx = 0;
                    for (String action : predict_actions.split(";")) {
                        if (action.equals("-1")) {
                            removePselIndex.add(rm_idx);
                            rm_idx = rm_idx + 1;
                            continue;
                        }
                        next_p.add(this.allExistPredicates.get(Integer.parseInt(action)));
                        rm_idx = rm_idx + 1;
                    }

                    // remove Psel without legal next_p
                    for (tmp_i = removePselIndex.size() - 1; tmp_i >= 0; tmp_i = tmp_i - 1) {
                        rm_idx = removePselIndex.get(tmp_i);
                        Psel.remove(rm_idx);
                        Psel_indices_set.remove(rm_idx);
                        RHSs.remove(rm_idx);
                    }
                    removePselIndex.clear();

                    if (Psel.size() == 0) {
                        ifContinue = false;
                        break;
                    }

                    // 2. compute supports and rewards, save to MEM
                    reward.clear();
                    for (tmp_i = 0; tmp_i < Psel.size(); tmp_i = tmp_i + 1) {
                        reward.add(0L);
                    }
                    beginRewardTime = System.currentTimeMillis();
                    computeRewardAndSaveMEM(Psel, next_p, reward, RHSs,
                            option, taskId, spark, sparkContextConfig, tupleNumberRelations, bcpsAssist);
                    computeRewardTime = System.currentTimeMillis() - beginRewardTime;
                    logger.info("#### finish computing reward at level {} with step {}, using time: {}", level, step, computeRewardTime);

                    if (Psel.size() == 0) {
                        ifContinue = false;
                        break;
                    }

                    // 3. send (Psel_i, p_i, reward_i), then train Mcorr_i+1 and save it. i++
                    sequence = transformSequence(this.allExistPredicates, Psel, next_p, reward);
                    beginUseRLTime = System.currentTimeMillis();
                    useRL(sequence, 1, 0, "", this.allExistPredicates.size(),
                            this.PI_path, this.RL_code_path, this.learning_rate, this.reward_decay,
                            this.e_greedy, this.replace_target_iter, this.memory_size, this.batch_size, this.table_name, this.N);
                    long trainRLTime = System.currentTimeMillis() - beginUseRLTime;
                    logger.info("#### begin training RL model at level {} with step {}, using time: {}", level, step, trainRLTime);
                }

                // put the trained RL model in HDFS
                long beginSaveModelTime = System.currentTimeMillis();
                if (ifExistModel) {
                    putLoadModelToFromHDFS(this.RL_code_path, 1, this.table_name);
                }
                long saveModelTime = System.currentTimeMillis() - beginSaveModelTime;
                long RLTime = System.currentTimeMillis() - beginRLTime;
                logger.info("#### finish save model to HDFS, using time: {}", saveModelTime);
                logger.info("#### finish training RL model at level {}, using time: {}", level, RLTime);
            }

            ArrayList<WorkUnit> workUnits_init = lattice.generateWorkUnits();
            workUnits_init = this.mergeWorkUnits(workUnits_init);

            if (level == 0 || level == 1) {
                this.conf_filter_thr = 0.001f;
            } else if (level == 2) {
                this.conf_filter_thr = 0.01f;
            } else {
                this.conf_filter_thr = 0.1f;
            }
            logger.info("Work Units size : {} with level {}, conf_filter_thr: {}", workUnits_init.size(), level, this.conf_filter_thr);

            // use MEM to remove workunits that have been computed
            if (this.ifRL == 1 && this.ifOnlineTrainRL == 1) {
                logger.info("#### before using MEM, there are {} workunits", workUnits_init.size());
                ArrayList<WorkUnit> removeWu = new ArrayList<>();
                for (WorkUnit wu : workUnits_init) {
                    if (this.MEM.contains(wu.getCurrrent()))
                        removeWu.add(wu);
                }
                for (WorkUnit wu : removeWu) {
                    workUnits_init.remove(wu);
                }
                logger.info("#### after using MEM, there are {} workunits", workUnits_init.size());
            }

            for (WorkUnit task : workUnits_init) {
                task.setTransferData();
            }
            // split RHS predicates of each work unit to make sure that RHSs of one work unit ONLY contain one type of TID
//            workUnits_init = this.partitionWorkUnitsByRHSs(workUnits_init);
            // batch mode
            HashMap<PredicateSet, Double> currentSupports = new HashMap<>();
            boolean ifTerm = true;

            while (workUnits_init.size() != 0) {
                // solve skewness
                // ArrayList<WorkUnit> workUnits = this.solveSkewness(workUnits_init);
                ArrayList<WorkUnit> workUnits = new ArrayList<>();
                int remainStartSc = this.solveSkewness(workUnits_init, workUnits);
                // prepare data shipment
                //this.dataTransfer(workUnits);
                // validate all work units
                List<Message> messages = null;
                if (workUnits.size() > 0) {
                    messages = this.run(workUnits, taskId, sc, tupleNumberRelations, bcpsAssist);
                    logger.info("Integrate messages ...");
                    messages = this.integrateMessages(messages);
                    ifTerm = false;
//                    for (int index = 0; index < messages.size(); index++ ) {
//                        Message message = messages.get(index);
//                        logger.info(">>>>sup after :{} | rhs support after : {}", message.getCurrentSupp(), message.getSupports());
//                    }
                }

                // maintain top-K max heap of REEs
                // logger.info("Returned Messages from all workers");
                if (messages != null) {
                    for (Message message : messages) {
                        // logger.info("message : {}", message);
                        DenialConstraintSet rees = this.transformRule(message);
                        // print all rules
                        for (Predicate p : message.getCurrent()) {
                            if (p.isConstant()) {
                                for (DenialConstraint ree : rees) {
                                    if (ree != null) {
                                        validConstantRule.putIfAbsent(message.getCurrentSet(), new ArrayList<>());
                                        validConstantRule.get(message.getCurrentSet()).add(ree.getRHS());
                                    }
                                }
                                break;
                            }
                        }
                        this.maintainTopKRules(rees);
                        // transform Current to currentSet
                        message.transformCurrent();
                    }
                }
                // collect supports of candidate rules
//            HashMap<IBitSet, Double> candSupports = this.getCandidateSupportRatios(messages);
                this.getCandidateXSupportRatios(currentSupports, messages);
                // collect invalid X
                this.addInvalidX(messages);
                // collective valid X -> p_0
                this.addValidXRHSs(messages);
                // prune meanineless work units
                if (option.equals("original")) {
                    workUnits_init = this.pruneXWorkUnits(interestingness, this.getKthInterestingnessScore(), currentSupports, workUnits_init, remainStartSc, this.topKOption);
                } else {
                    ArrayList<WorkUnit> remainTasks = new ArrayList<>();
                    for (int tid = remainStartSc; tid < workUnits_init.size(); tid++) {
                        remainTasks.add(workUnits_init.get(tid));
                    }
                    workUnits_init = remainTasks;
                }

            }

            if (this.ifRL == 1 && this.ifOnlineTrainRL == 0 && this.ifOfflineTrainStage == 1 && ifReachN) {
                break;
            }

            if (level > 0 && ifTerm) {
                break;
            }

            if (level + 1 > MAX_CURRENT_PREDICTES) {
                break;
            }
            Lattice nextLattice = runNextLattices(lattice, this.allPredicates, this.invalidX, this.invalidXRHSs, this.validXRHSs,
                    ifExistModel, interestingness, this.getKthInterestingnessScore(), currentSupports, predicateProviderIndex, option, null, spark);

            if (nextLattice == null || nextLattice.size() == 0) {
                break;
            }
            lattice = nextLattice;
            level++;
        }

        // save training data, i.e., rewards for offline RL
        if (this.ifRL == 1 && this.ifOnlineTrainRL == 0 && this.ifOfflineTrainStage == 1) {
            long beginOfflineTrainRLTime = System.currentTimeMillis();
            // method-1
//            trainRLModel(this.ps_rewards);

            // method-2
//            writeToFile(this.ps_rewards);

            // method-3
            writeToFile(sequenceResults.substring(0, sequenceResults.length() - 1));

            long offlineTrainRLTime = System.currentTimeMillis() - beginOfflineTrainRLTime;
            logger.info("#### finish saving training data, i.e., rewards for OFFLINE RL, using time: {}", offlineTrainRLTime);
        }
    }

    /*
        prune work units using interestingness scores and supports
     */
    private ArrayList<WorkUnit> pruneXWorkUnits(Interestingness interestingness, double KthScore, HashMap<PredicateSet, Double> supp_ratios,
                                                ArrayList<WorkUnit> tasks, int remainedStartSc, String topKOption) {
        logger.info("#### all workunits num: {}, remainedStartSc: {}", tasks.size(), remainedStartSc);
        logger.info("#### before pruning, workunits num: {}", tasks.size());
        ArrayList<WorkUnit> prunedTasks = new ArrayList<>();
        for (int tid = remainedStartSc; tid < tasks.size(); tid++) {
            WorkUnit task = tasks.get(tid);

            if (supp_ratios.containsKey(task.getCurrrent())) {
                // check support
                long suppOne = (long) (supp_ratios.get(task.getCurrrent()) * this.allCount);
                if (suppOne < support) {
                    continue;
                }
                // check interestingness UB
                for (Predicate rhs : task.getRHSs()) {
                    double ub = interestingness.computeUB(suppOne * 1.0 / this.allCount, 1.0, task.getCurrrent(), rhs, topKOption);
                    if (ub <= KthScore) {
                        task.getRHSs().remove(rhs);
                    }
                }
                // if no RHSs to do in this task, continue to handle other ones
                if (task.getRHSs().size() == 0) {
                    continue;
                }
            }
            prunedTasks.add(task);
        }
        logger.info("#### after pruning, workunits num: {}", prunedTasks.size());
        return prunedTasks;
    }

    public Lattice runNextLattices(Lattice lattice, List<Predicate> allPredicates, HashSet<IBitSet> invalidX,
                                   HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                                   HashMap<IBitSet, ArrayList<Predicate>> validXRHSs,
                                   boolean ifExistModel,
                                   Interestingness interestingness, double KthScore,
                                   HashMap<PredicateSet, Double> suppRatios,
                                   PredicateProviderIndex predicateProviderIndex,
                                   String option, ArrayList<LatticeVertex> partialRules, SparkSession spark) {

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        BroadcastLattice broadcastLattice;

        broadcastLattice = new BroadcastLattice(allPredicates, invalidX, invalidXRHSs, validXRHSs,
                interestingness, KthScore, suppRatios, predicateProviderIndex, option, this.topKOption, this.predicateDQNHashIDs);

        Broadcast<BroadcastLattice> bcpsLattice = sc.broadcast(broadcastLattice);

        // broadcast provideIndex, i.e., all predicates
        PredicateSetAssist psAssist = new PredicateSetAssist();
        psAssist.setIndexProvider(PredicateSet.indexProvider);//在这里设置，这个值肯定是确定了的
        psAssist.setBf(PredicateSet.bf);
        Broadcast<PredicateSetAssist> bcpsAssist = sc.broadcast(psAssist);
//        logger.info("####level:{}, Before dig indexProvider size:{}, list:{}", 0,
//                PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());


        // prune using support and interestingness UB
        // prune invalid lattice nodes
        lattice.pruneLattice(invalidX, invalidXRHSs, validXRHSs, allPredicates);
        // prune with interestingness UB
        if (option.equals("original")) {
//            this.pruneLattice(invalidX, validXRHSs);
            lattice.pruneXInterestingnessUB(interestingness, KthScore, suppRatios, this.topKOption);
        } else if (option.equals("anytime")) {
//            this.pruneLattice(invalidX, validXRHSs);
            lattice.pruneXInterestingnessUB(interestingness, KthScore, suppRatios, partialRules);
        }

        // split work units
        ArrayList<Lattice> workUnitLattices = lattice.splitLattice(NUM_LATTICE);
        // print split lattices
//        logger.info("Lattices Number : {}", workUnitLattices.size());
        /*
        for (Lattice ll : workUnitLattices) {
            logger.info("Split Lattice : {}", ll.printCurrents());
        }

         */
        if (workUnitLattices.size() == 0) {
            return null;
        }

        // parallel expand lattice vertices of next level
        Lattice nextLattice = sc.parallelize(workUnitLattices, workUnitLattices.size()).map(task -> {

            // synchronized (BroadcastLattice.class) {
            // get IndexProvider instances
            PredicateSetAssist assist = bcpsAssist.getValue();
            PredicateSet.indexProvider = assist.getIndexProvider();
            PredicateSet.bf = assist.getBf();
            //}

            // logger.info("In workers, lattice is {}", task.printCurrents());
            // get lattice data
            BroadcastLattice bLattice = bcpsLattice.getValue();

            Lattice latticeWorker = task.generateNextLatticeLevel(bLattice.getAllPredicates(), bLattice.getAllExistPredicates(), bLattice.getInvalidX(), bLattice.getInvalidRHSs(), bLattice.getValidXRHSs(),
                    bLattice.getInterestingness(), bLattice.getKthScore(), bLattice.getSuppRatios(), bLattice.getPredicateProviderIndex(), bLattice.getOption(), null,
                    bLattice.getIfRL(), bLattice.getIfOnlineTrainRL(), bLattice.getIfOfflineTrainStage(), bLattice.getIfExistModel(),
                    bLattice.getPI_path(), bLattice.getRL_code_path(), bLattice.getLearning_rate(), bLattice.getReward_decay(), bLattice.getE_greedy(),
                    bLattice.getReplace_target_iter(), bLattice.getMemory_size(), bLattice.getBatch_size(), bLattice.getTable_name(), bLattice.getN(),
                    bLattice.getPredicatesHashIDs(), bLattice.getTopKOption());
            return latticeWorker;

        }).aggregate(null, new ILatticeAggFunction(), new ILatticeAggFunction());
        long now = System.currentTimeMillis();

//        Lattice nextLattice = workUnitLattices.stream().parallel().map(task -> {
//
//            // synchronized (BroadcastLattice.class) {
//            // get IndexProvider instances
////            PredicateSetAssist assist = bcpsAssist.getValue();
////            PredicateSet.indexProvider = assist.getIndexProvider();
////            PredicateSet.bf = assist.getBf();
//            //}
//
//            // logger.info("In workers, lattice is {}", task.printCurrents());
//            // get lattice data
////            BroadcastLattice bLattice = bcpsLattice.getValue();
//            BroadcastLattice bLattice = broadcastLattice;
//
//            Lattice latticeWorker = task.generateNextLatticeLevel(bLattice.getAllPredicates(), bLattice.getAllExistPredicates(), bLattice.getInvalidX(), bLattice.getInvalidRHSs(), bLattice.getValidXRHSs(),
//                    bLattice.getInterestingness(), bLattice.getKthScore(), bLattice.getSuppRatios(), bLattice.getPredicateProviderIndex(), bLattice.getOption(), null,
//                    bLattice.getIfRL(), bLattice.getIfOnlineTrainRL(), bLattice.getIfOfflineTrainStage(), bLattice.getIfExistModel(),
//                    bLattice.getPI_path(), bLattice.getRL_code_path(), bLattice.getLearning_rate(), bLattice.getReward_decay(), bLattice.getE_greedy(),
//                    bLattice.getReplace_target_iter(), bLattice.getMemory_size(), bLattice.getBatch_size(), bLattice.getTable_name(), bLattice.getN());
//            return latticeWorker;
//
//        }).reduce(new Lattice(), new ILatticeAggFunction()::call, new ILatticeAggFunction()::call);

        logger.info(">>>> finish runNext Lattice: {}", (System.currentTimeMillis() - now)/1000);
        // final pruning
        nextLattice.removeInvalidLatticeAndRHSs(lattice);

        return nextLattice;
    }

//    private void dataTransfer(ArrayList<WorkUnit> workUnits) {
//        for (WorkUnit wu : workUnits) {
//            wu.dataTransfer();
//        }
//    }


    public List<Message> run(ArrayList<WorkUnit> workUnits, String taskId, SparkSession spark,
                             SparkContextConfig sparkContextConfig, HashMap<String, Long> tupleNumberRelations, Broadcast<PredicateSetAssist> bcpsAssist) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Broadcast<List<Predicate>> broadAllPredicate = sc.broadcast(allPredicates);
        return this.run(workUnits, taskId, sc, tupleNumberRelations, bcpsAssist);
    }


    public List<Message> run(ArrayList<WorkUnit> workUnits, String taskId,
                             JavaSparkContext sc, HashMap<String, Long> tupleNumberRelations, Broadcast<PredicateSetAssist> bcpsAssist) {
        BroadcastObj broadcastObj = new BroadcastObj(this.maxTupleNum, this.inputLight, this.support, this.confidence,
                this.maxOneRelationNum, tupleNumberRelations);
        // broadcast data
        // ... left for future

        broadcastObj.setValidConstantRule(validConstantRule);
        Broadcast<BroadcastObj> scInputLight = sc.broadcast(broadcastObj);

        // broadcast provideIndex, i.e., all predicates
//        PredicateSetAssist psAssist = new PredicateSetAssist();
//        psAssist.setIndexProvider(PredicateSet.indexProvider);//在这里设置，这个值肯定是确定了的
//        psAssist.setBf(PredicateSet.bf);
//        psAssist.setTaskId(taskId);
//        Broadcast<PredicateSetAssist> bcpsAssist = sc.broadcast(psAssist);
//        logger.info("####level:{}, Before dig indexProvider size:{}, list:{}", 0,
//                PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());


        // print all work units
//        for (WorkUnit task : workUnits) {
//            logger.info("### Work Unit : {}", task);
//        }


        //增加聚类方法聚合Unit
        logger.info(">>>>begin cluster");
        ArrayList<WorkUnits> unitSets;
        if (this.if_cluster_workunits == 1) {
            unitSets = clusterByPredicate(workUnits, 2);
        } else {
            unitSets = new ArrayList<>();
            for (WorkUnit workUnit : workUnits) {
                WorkUnits cluster = new WorkUnits();
                cluster.addUnit(workUnit);
                unitSets.add(cluster);
            }
        }
        logger.info(">>>>end cluster");

        // set allCount of each work unit
        for (WorkUnit task : workUnits) {
            task.clearData();
        }
        List<Message> ruleMessages = new ArrayList<>();
        logger.info("running by MultiTuplesRuleMiningOpt!!");

        for(WorkUnits set : unitSets) {
            set.setAllCount(this.allCount);
        }

//        JavaRDD<WorkUnits> rdd = sc.parallelize(unitSets);
        List<Message> ruleMessagesSub = sc.parallelize(unitSets, unitSets.size()).map(unitSet -> {
            if (unitSet.getCurrrent().size() == 0) {
                return null;
            }

            PredicateSetAssist assist = bcpsAssist.getValue();
            PredicateSet.indexProvider = assist.getIndexProvider();
            PredicateSet.bf = assist.getBf();
            String taskid = assist.getTaskId();

            BroadcastObj bobj = scInputLight.getValue();
            Map<PredicateSet, List<Predicate>> validConsRuleMap = bobj.getValidConstantRule();
            Map<PredicateSet, Map<String, Predicate>> constantXMap = new HashMap<>();
            PredicateSet sameSet = unitSet.getSameSet();
            for (PredicateSet set : validConsRuleMap.keySet()) {
                PredicateSet tupleX = new PredicateSet();
                Map<String, Predicate> constantX = new HashMap<>();
                for (Predicate p : set) {
                    if (p.isConstant()) {
                        constantX.put(p.getOperand1().toString_(0), p);
                    } else {
                        tupleX.add(p);
                    }
                }

                if(tupleX.size() > 0) {
                    constantXMap.putIfAbsent(tupleX, constantX);
                }
            }

            List<WorkUnit> units = unitSet.getUnits();
//            WorkUnits newTask = new WorkUnits();
            for(PredicateSet tuplePs : constantXMap.keySet()) {
                if(sameSet.size() > 0) {
                    if(!tuplePs.containsPS(sameSet)) {
                        continue;
                    }
                }
                for (WorkUnit unit : units) {
                    PredicateSet tupleX = new PredicateSet();
                    PredicateSet constantX = new PredicateSet();
                    for (Predicate p : unit.getCurrrent()) {
                        if (p.isConstant()) {
                            constantX.add(p);
                        } else {
                            tupleX.add(p);
                        }
                    }


                    if (tupleX.containsPS(tuplePs)) {
                        Map<String, Predicate> constantsMap = constantXMap.get(tuplePs);
                        boolean iscont = true;
                        for (Predicate p : constantX) {
                            if (!constantsMap.containsKey(p.getOperand1().toString_(0))) {
                                iscont = false;
                                break;
                            }
                        }

                        if (iscont) {
                            PredicateSet lhs = new PredicateSet();
                            lhs.addAll(tuplePs);
                            for (Predicate p : constantsMap.values()) {
                                lhs.add(p);
                            }
                            List<Predicate> rhs = validConsRuleMap.get(lhs);

                            for (Predicate p : rhs) {
                                if (unit.getRHSs().containsPredicate(p)) {
                                    unit.getRHSs().remove(p);
                                    logger.info(">>>> test cut: {}", p);
                                }
                            }
                        }
                    }
                }

//                if(unit.getRHSs().size() > 0) {
//                    newTask.addUnit(unit);
//                }
            }
//            newTask.setAllCount(unitSet.getAllCount());


//            Map<TIntArrayList, List<Integer>> leftMap = new HashMap<>();
//            Map<TIntArrayList, List<Integer>> rightMap = new HashMap<>();

            Predicate pBegin = null;
//            PredicateSet sameSet = newTask.getSameSet();
//            ArrayList<Predicate> samePs = new ArrayList<>();
//            if (sameSet.size() > 0) {
//                for (Predicate p : sameSet) {
//                    samePs.add(p);
//                }
//            }
//            ArrayList<Predicate> currentList = new ArrayList<>();
            for (Predicate p : unitSet.getSameSet()) {
//                if (!sameSet.containsPredicate(p)) {
//                    currentList.add(p);
//                }
                if (!p.isML() && !p.isConstant()) {
                    pBegin = p;
                    break;
                }
            }

//            ArrayList<Predicate> rhsList = new ArrayList<>();
//            for (Predicate p : newTask.getRHSs()) {
//                rhsList.add(p);
//            }

//            logger.info(">>>Will do multiTuplesRuleMining! {} | {}", currentList, rhsList);
            MultiTuplesRuleMiningOpt multiTuplesRuleMining = new MultiTuplesRuleMiningOpt(bobj.getMax_num_tuples(),
                    bobj.getInputLight(), bobj.getSupport(), bobj.getConfidence(), bobj.getMaxOneRelationNum(),
                    unitSet.getAllCount(), bobj.getTupleNumberRelations());

//            TIntArrayList _list1 = pBegin.getOperand1().getColumnLight().getValueIntList(unitSet.getPids()[pBegin.getIndex1()]);
//            TIntArrayList _list2 = pBegin.getOperand2().getColumnLight().getValueIntList(unitSet.getPids()[pBegin.getIndex2()]);
//            leftMap = multiTuplesRuleMining.createHashMap(_list1, samePs, unitSet.getPids(), true);
//            rightMap = multiTuplesRuleMining.createHashMap(_list2, samePs, unitSet.getPids(), false);

//            logger.info(">>>Will do multiTuple valid! {} | {}", currentList, rhsList);
//            List<Message> messages = multiTuplesRuleMining.validation(current, rhs, task.getPids());

//            List<Message> messages = multiTuplesRuleMining.validationMap(currentList, unitSet, leftMap, rightMap);
            List<Message> messages = multiTuplesRuleMining.validationMap1(unitSet, pBegin);

//            logger.info(">>>Finish multiTuple valid! {} | {}", currentList, rhsList);
            return messages;
//            List<Message> messages = new ArrayList();
//            return messages;
        }).aggregate(null, new IMessageAggFunction(), new IMessageAggFunction());



//        List<Message> ruleMessagesSub = sc.parallelize(workUnits, workUnits.size()).map(task -> {
//
//            // share PredicateSet static instance, i.e., providerIndex
//
////            logger.info("####level:{}, In workers XXXXXX indexProvider size:{}, list:{}", 0,
////                    PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());
//
//            //synchronized (BroadcastObj.class) {
//            PredicateSetAssist assist = bcpsAssist.getValue();
//            PredicateSet.indexProvider = assist.getIndexProvider();
//            PredicateSet.bf = assist.getBf();
//            String taskid = assist.getTaskId();
//            //}
//
////            logger.info("####level:{}, In workers indexProvider size:{}, list:{}", 0,
////                    PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());
////
////                // print
////                logger.info("***** Work Unit, {}", task);
//
//            if (task.getCurrrent().size() == 0) {
//                return null;
//            }
//
//            // retrieve data
//            task.retrieveTransferData();
//
//            BroadcastObj bobj = scInputLight.getValue();
//            // validate candidate rules in workers
//
////                MultiTuplesRuleMining multiTuplesRuleMining = new MultiTuplesRuleMining(bobj.getMax_num_tuples(),
////                        bobj.getInputLight(), bobj.getSupport(), bobj.getConfidence(), bobj.getMaxOneRelationNum(),
////                        task.getAllCount(), bobj.getTupleNumberRelations());
////
////                ArrayList<Predicate> current = new ArrayList<>();
////                for (Predicate p : task.getCurrrent()) {
////                    current.add(p);
////                }
////                // List<Message> messages = multiTuplesRuleMining.validation(current, task.getRHSs());
////                List<Message> messages = multiTuplesRuleMining.validation_new(current, task.getRHSs(), task.getPids());
//
//            MultiTuplesRuleMiningOpt multiTuplesRuleMining = new MultiTuplesRuleMiningOpt(bobj.getMax_num_tuples(),
//                    bobj.getInputLight(), bobj.getSupport(), bobj.getConfidence(), bobj.getMaxOneRelationNum(),
//                    task.getAllCount(), bobj.getTupleNumberRelations());
//
//            ArrayList<Predicate> current = new ArrayList<>();
//            for (Predicate p : task.getCurrrent()) {
//                current.add(p);
//            }
//            List<Message> messages = multiTuplesRuleMining.validation(current, task.getRHSs(), task.getPids());
//
//            return messages;
//        }).aggregate(null, new IMessageAggFunction(), new IMessageAggFunction());

//        for (int wid = 0; wid < workUnits.size(); wid += BATCH_SIZE) {
//            int begin = wid;
//            int end = Math.min(wid + BATCH_SIZE, workUnits.size());
//            ArrayList<WorkUnit> subTasks = new ArrayList<>();
//            for (int z = begin; z < end; z++) {
//                subTasks.add(workUnits.get(z));
//            }
//        List<Message> ruleMessages = sc.parallelize(workUnits, workUnits.size()).map(task -> {

//        Collections.shuffle(workUnits); // 解决数据倾斜
//        List<Message> ruleMessagesSub1 = sc.parallelize(workUnits).mapPartitions(tasks -> {
//            List<WorkUnit> taskList = new ArrayList<>();
//            while (tasks.hasNext()) {
//                WorkUnit next = tasks.next();
//                if (next.getCurrrent().size() == 0) {
//                    continue;
//                }
//                taskList.add(next);
//            }
//
//            PredicateSetAssist assist = bcpsAssist.getValue();
//            PredicateSet.indexProvider = assist.getIndexProvider();
//            PredicateSet.bf = assist.getBf();
//            String taskid = assist.getTaskId();
//
//            BroadcastObj bobj = scInputLight.getValue();
//
//            // 内部并行利用 18 核心
//            return taskList.stream()/*.parallel()*/.map(task -> {
//
//                // share PredicateSet static instance, i.e., providerIndex
//
////            logger.info("####level:{}, In workers XXXXXX indexProvider size:{}, list:{}", 0,
////                    PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());
//
//                //synchronized (BroadcastObj.class) {
//
//                //}
//
////            logger.info("####level:{}, In workers indexProvider size:{}, list:{}", 0,
////                    PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());
////
////                // print
////                logger.info("***** Work Unit, {}", task);
//                // retrieve data
////                task.retrieveTransferData();
////                List<Predicate> predicatesList = broadAllPredicate.getValue();
////                task.setTransferData(predicatesList);
////                if (task.getCurrrent().size() == 0) {
////                    return null;
////                }
////                if (task.getCurrentIndexs().size() == 0) {
////                    return null;
////                }
//
//
//                // validate candidate rules in workers
//
////                MultiTuplesRuleMining multiTuplesRuleMining = new MultiTuplesRuleMining(bobj.getMax_num_tuples(),
////                        bobj.getInputLight(), bobj.getSupport(), bobj.getConfidence(), bobj.getMaxOneRelationNum(),
////                        task.getAllCount(), bobj.getTupleNumberRelations());
////
//                ArrayList<Predicate> current = new ArrayList<>();
//                for (Predicate p : task.getCurrrent()) {
//                    current.add(p);
//                }
//                ArrayList<Predicate> rhs = new ArrayList<>();
//                for (Predicate p : task.getRHSs()) {
//                    rhs.add(p);
//                }
////                // List<Message> messages = multiTuplesRuleMining.validation(current, task.getRHSs());
////                List<Message> messages = multiTuplesRuleMining.validation_new(current, task.getRHSs(), task.getPids());
//                logger.info(">>>Will do multiTuplesRuleMining!");
//                MultiTuplesRuleMiningOpt multiTuplesRuleMining = new MultiTuplesRuleMiningOpt(bobj.getMax_num_tuples(),
//                        bobj.getInputLight(), bobj.getSupport(), bobj.getConfidence(), bobj.getMaxOneRelationNum(),
//                        task.getAllCount(), bobj.getTupleNumberRelations());
//
////                for (int index : task.getCurrentIndexs()) {
////                    Predicate p = predicatesList.get(index);
////                    current.add(p);
////                }
////                PredicateSet rhss = new PredicateSet();
////                for (int index : task.getRHSIndexs()) {
////                    Predicate p = predicatesList.get(index);
////                    rhss.add(p);
////                }
//                logger.info(">>>Will do multiTuple valid!");
//                List<Message> messages1 = multiTuplesRuleMining.validation(current, rhs, task.getPids());
//
////                List<Message> messages = multiTuplesRuleMining.validation(current, rhss, task.getPids());
//                logger.info(">>>Finish multiTuple valid!");
//                return messages1;
//
//            })
//                    .collect(Collectors.toList()).iterator();
//
//        }).aggregate(null, new IMessageAggFunction(), new IMessageAggFunction());


        ruleMessages.addAll(ruleMessagesSub);
//        for (int index = 0; index < ruleMessages.size(); index++) {
//            Message message = ruleMessages.get(index);
//            Message message1 = ruleMessagesSub1.get(index);
//            logger.info(">>>>current: {} support :{}, rhs supp: {}", message.getCurrentSet(), message.getCurrentSupp(),
//                    message.getAllCurrentRHSsSupport());
//            logger.info(">>>>current1: {} support1 :{}, rhs supp1: {}", message1.getCurrentSet(), message1.getCurrentSupp(),
//                    message1.getAllCurrentRHSsSupport());
//        }
        return ruleMessages;
    }

    public ArrayList<WorkUnits> clusterByPredicate(List<WorkUnit> units, int clusterSize) {
        List<WorkUnits> clusters = new ArrayList<>();
        List<WorkUnit> workUnits = new ArrayList<>();
        int minSize = 0;
        Map<String, List<WorkUnit>> workUnitMap = new HashMap<>();
        Map<IBitSet, List<WorkUnit>> sameUnitMap = new HashMap<>();
        for (WorkUnit unit : units) {
            workUnitMap.putIfAbsent(unit.getPidString(), new ArrayList<>());
            workUnitMap.get(unit.getPidString()).add(unit);
            minSize = Math.max(unit.getCurrrent().size(), minSize);

            sameUnitMap.putIfAbsent(unit.getKey(), new ArrayList<>());
            sameUnitMap.get(unit.getKey()).add(unit);
        }

        for (List<WorkUnit> list : sameUnitMap.values()) {
            list.sort(new Comparator<WorkUnit>() {
                @Override
                public int compare(WorkUnit o1, WorkUnit o2) {
                    int[] pids1 = o1.getPids();
                    int[] pids2 = o2.getPids();
                    for (int index = 0; index < pids1.length; index++) {
                        if (pids1[index] != pids2[index]) {
                            return pids1[index] - pids2[index];
                        }
                    }
                    return 0;
                }
            });
        }

        minSize = Math.max(minSize + 1, 0);
//        minSize = Math.max((minSize * 2) - 3, 1);
//        for(Predicate p : this.allPredicates) {
//            WorkUnits cluster = new WorkUnits();
//            cluster.addSameSet(p);
//            size++;
//            if (size == clusterSize) {
//                break;
//            }
//        }

        for (List<WorkUnit> sameUnit : workUnitMap.values()) {
            workUnits.addAll(sameUnit);

            int totalSize = Math.min(clusterSize, units.size());
            for (int size = 0; size < totalSize; size++) {
                WorkUnits cluster = new WorkUnits();
                cluster.addUnit(units.get(size));
                clusters.add(cluster);
                workUnits.remove(units.get(size));
            }

            for (WorkUnit unit : workUnits) {
                int max = minSize;
                int current = -1;
                for (int index = 0; index < clusters.size(); index++) {
                    WorkUnits cluster = clusters.get(index);
                    int sameNum = cluster.calDistance(unit);
                    if (sameNum > max) {
                        max = sameNum;
                        current = index;
                    }
                }

                if (current > -1) {
                    WorkUnits cluster = clusters.get(current);
                    cluster.addUnit(unit);
                } else {
                    WorkUnits cluster = new WorkUnits();
                    cluster.addUnit(unit);
                    clusters.add(cluster);
                }
            }

            workUnits.clear();
            break;
        }
        ArrayList<WorkUnits> result = new ArrayList<>();
        for (WorkUnits cluster : clusters) {
            if (cluster.getUnits().size() > 0) {

                List<WorkUnits> list = new ArrayList<>();
                for (WorkUnit unit : cluster.getUnits()) {
                    int count = 0;
                    for (WorkUnit allUnit : sameUnitMap.get(unit.getKey())) {
                        if (list.size() == count) {
                            WorkUnits newCluster = new WorkUnits();
                            newCluster.addUnit(allUnit);
                            list.add(newCluster);
                        } else {
                            list.get(count).addUnit(allUnit);
                        }
                        count++;
                    }
                }
                result.addAll(list);
            }
        }

//        for (WorkUnits cluster : result) {
//            logger.info(">>>>show unit size : {} | same set: {}", cluster.getUnits().size(), cluster.getSameSet());
//        }
        return result;
    }

    public void computeRewardAndSaveMEM(ArrayList<PredicateSet> Psel,
                                        ArrayList<Predicate> next_p,
                                        ArrayList<Long> reward,
                                        ArrayList<PredicateSet> RHSs,
                                        String option, String taskId, SparkSession spark,
                                        SparkContextConfig sparkContextConfig,
                                        HashMap<String, Long> tupleNumberRelations,
                                        Broadcast<PredicateSetAssist> bcpsAssist) {
        // in case that there exists same workunit, i.e., {Psel, next_p}
        ArrayList<Boolean> visited = new ArrayList<>();
        for (int i = 0; i < Psel.size(); i = i + 1) {
            visited.add(false);
        }

        // generate WorkUnits
        ArrayList<WorkUnit> workUnits_ = new ArrayList<>();
        for (int i = 0; i < Psel.size(); i++) {
            WorkUnit workUnit = new WorkUnit();
            for (Predicate p : Psel.get(i)) {
                workUnit.addCurrent(p);
            }
            workUnit.addCurrent(next_p.get(i));
            for (Predicate p : RHSs.get(i)) {
                if (!this.validRHS(p)) {
                    continue;
                }
                workUnit.addRHS(p);
            }
            this.MEM.add(workUnit.getCurrrent());
            workUnits_.add(workUnit);
        }

        ArrayList<WorkUnit> workUnits_init = workUnits_;
        workUnits_init = this.mergeWorkUnits(workUnits_init);
        for (WorkUnit task : workUnits_init) {
            task.setTransferData();
        }

        // split RHS predicates of each work unit to make sure that RHSs of one work unit ONLY contain one type of TID
        workUnits_init = this.partitionWorkUnitsByRHSs(workUnits_init);
        // batch mode
        HashMap<PredicateSet, Double> currentSupports = new HashMap<>();
        while (workUnits_init.size() != 0) {
            // solve skewness
            // ArrayList<WorkUnit> workUnits = this.solveSkewness(workUnits_init);
            ArrayList<WorkUnit> workUnits = new ArrayList<>();
            int remainStartSc = this.solveSkewness(workUnits_init, workUnits);
            // prepare data shipment
            //this.dataTransfer(workUnits);
            // validate all work units
            List<Message> messages = null;
            if (workUnits.size() > 0) {
                messages = this.run(workUnits, taskId, spark, sparkContextConfig, tupleNumberRelations, bcpsAssist);
                logger.info("Integrate messages ...");
                messages = this.integrateMessages(messages);
            }

            // update reward for RL
            if (messages != null) {
                for (Message msg : messages) {
                    if (msg.getCurrentSupp() == 0) {
                        continue;
                    }
                    for (int tmp_i = 0; tmp_i < workUnits_.size(); tmp_i = tmp_i + 1) {
                        if (msg.getCurrentSet().equals(workUnits_.get(tmp_i).getCurrrent()) && !visited.get(tmp_i)) {
                            reward.set(tmp_i, msg.getCurrentSupp() - this.support);
                            visited.set(tmp_i, true);
                        }
                    }
                }
            }

            // maintain top-K max heap of REEs
            // logger.info("Returned Messages from all workers");
            if (messages != null) {
                for (Message message : messages) {
                    // logger.info("message : {}", message);
                    DenialConstraintSet rees = this.transformRule(message);
                    // print all rules
                    for (Predicate p : message.getCurrent()) {
                        if (p.isConstant()) {
                            for (DenialConstraint ree : rees) {
                                if (ree != null) {
                                    validConstantRule.putIfAbsent(message.getCurrentSet(), new ArrayList<>());
                                    validConstantRule.get(message.getCurrentSet()).add(ree.getRHS());
                                }
                            }
                            break;
                        }
                    }
                    this.maintainTopKRules(rees);
                    // transform Current to currentSet
                    message.transformCurrent();
                }
            }
            // collect supports of candidate rules
//            HashMap<IBitSet, Double> candSupports = this.getCandidateSupportRatios(messages);
            this.getCandidateXSupportRatios(currentSupports, messages);
            // collect invalid X
            this.addInvalidX(messages);
            // collective valid X -> p_0
            this.addValidXRHSs(messages);
            // prune meanineless work units
            if (option.equals("original")) {
                workUnits_init = this.pruneXWorkUnits(interestingness, this.getKthInterestingnessScore(), currentSupports, workUnits_init, remainStartSc, this.topKOption);
            } else {
                ArrayList<WorkUnit> remainTasks = new ArrayList<>();
                for (int tid = remainStartSc; tid < workUnits_init.size(); tid++) {
                    remainTasks.add(workUnits_init.get(tid));
                }
                workUnits_init = remainTasks;
            }

        }

        // remove Psel without reward, since some workunits with high support X are not computed
        for (int i = visited.size() - 1; i >= 0; i = i - 1) {
            if (visited.get(i)) {
                continue;
            }
            Psel.remove(i);
            next_p.remove(i);
            RHSs.remove(i);
            reward.remove(i);
        }
    }


    private void prepareAllPredicatesMultiTuples() {
        HashMap<String, ParsedColumnLight<?>> colsMap = new HashMap<>();
        for (Predicate p : this.allPredicates) {
            String k = p.getOperand1().getColumn().toStringData();
            if (!colsMap.containsKey(k)) {
                ParsedColumnLight<?> col = new ParsedColumnLight<>(p.getOperand1().getColumn());
                colsMap.put(k, col);
            }
            k = p.getOperand2().getColumn().toStringData();
            if (!colsMap.containsKey(k)) {
                ParsedColumnLight<?> col = new ParsedColumnLight<>(p.getOperand2().getColumn());
                colsMap.put(k, col);
            }
        }
        // delete value int data of ParsedColumn
        for (Predicate p : this.allPredicates) {
            p.getOperand1().getColumn().cleanValueIntBeforeBroadCast();
            p.getOperand2().getColumn().cleanValueIntBeforeBroadCast();
        }
        // insert parsedColumnLight for each predicate
        PredicateSet ps = new PredicateSet();
        for (Predicate p : this.allPredicates) {
            // set columnLight
            String k = p.getOperand1().getColumn().toStringData();
            p.getOperand1().setColumnLight(colsMap.get(k));
            k = p.getOperand2().getColumn().toStringData();
            p.getOperand2().setColumnLight(colsMap.get(k));
            ps.add(p);
            for (int t1 = 0; t1 < maxTupleNum; t1++) {
                if (p.isConstant()) {
                    Predicate p_new = predicateProviderIndex.getPredicate(p, t1, t1);
                    k = p_new.getOperand1().getColumn().toStringData();
                    p_new.getOperand1().setColumnLight(colsMap.get(k));
                    k = p_new.getOperand2().getColumn().toStringData();
                    p_new.getOperand2().setColumnLight(colsMap.get(k));
                    ps.add(p_new);
                    continue;
                }
                for (int t2 = t1 + 1; t2 < maxTupleNum; t2++) {
                    Predicate p_new = predicateProviderIndex.getPredicate(p, t1, t2);
                    k = p_new.getOperand1().getColumn().toStringData();
                    p_new.getOperand1().setColumnLight(colsMap.get(k));
                    k = p_new.getOperand2().getColumn().toStringData();
                    p_new.getOperand2().setColumnLight(colsMap.get(k));
                    ps.add(p_new);
                }
            }
        }

//        if (this.ifRL == 1) {
            this.allExistPredicates = new ArrayList<>();
            this.highSelectivityPredicateIndices = new ArrayList<>();
            long maxCount = maxOneRelationNum * maxOneRelationNum - maxOneRelationNum;
            int idx = 0;
            for (Predicate p : ps) {
                this.allExistPredicates.add(p);
                if (p.getSupport() <= 0.1 * maxCount) {
                    this.highSelectivityPredicateIndices.add(idx);
                }
                idx = idx + 1;
            }
//        }
//        PredicateSet ps = new PredicateSet();
//        for(Predicate p : this.allPredicates) {
//            p.initialPredicateDataTransfer();
//            ps.add(p);
//            for (int t1 = 0; t1 < maxTupleNum; t1++) {
//                if (p.isConstant()) {
//                    Predicate p_new = predicateProviderIndex.getPredicate(p, t1, t1);
//                    p_new.initialPredicateDataTransfer();
//                    ps.add(p_new);
//                    continue;
//                }
//                for (int t2 = t1 + 1; t2 < maxTupleNum; t2++) {
//                    Predicate p_new = predicateProviderIndex.getPredicate(p, t1, t2);
//                    p_new.initialPredicateDataTransfer();
//                    ps.add(p_new);
//                }
//            }
//        }
    }

    /*
        calculate the interestingness score for a rule
     */
    private double computeInterestingness(DenialConstraint ree) {
        // use the formula discussed before
        return this.interestingness.computeInterestingness(ree);
    }

    private DenialConstraintSet transformRule(Message message) {
        DenialConstraintSet rees = new DenialConstraintSet();
        ArrayList<Predicate> currentList = message.getCurrent();
        PredicateSet currentX = new PredicateSet();
        for (Predicate p : currentList) {
            currentX.add(p);
        }
//        for (Predicate rhs : message.getValidRHSs()) {
        long lhsSupport = message.getCurrentSupp();
        for (int i = 0; i < message.getValidRHSs().size(); i++) {
            Predicate rhs = message.getValidRHSs().get(i);
            long supp = message.getSupports().get(i);
            double conf = message.getConfidences().get(i); //((double) supp) / lhsSupport; // if rules containing constant for Y, replace lhsSupport to SupportCP0
            // check constant rhs
            if (rhs.isConstant()) {
                // supp = (long)(supp / this.maxOneRelationNum);
            }
            PredicateSet ps = new PredicateSet(currentX);
            ps.addRHS(rhs);
            DenialConstraint ree = new DenialConstraint(ps);
            // set support and confidence
            ree.setSupport(supp);
            ree.setViolations(lhsSupport - supp);
            ree.setConfidence(conf);
//            logger.info("Transform rule {} with support {} and confidence {}", ree, supp, conf);
            // compute the interestingness score of each REE rule
            double score = this.computeInterestingness(ree);
            ree.setInterestingnessScore(score);
            rees.add(ree);
        }
        rees.setTotalTpCount(allCount*allCount);
        // remove RHSs in X
        rees.removeRHS();
        return rees;
    }

    /*
        store the current top-K interesting REEs
     */
    private void maintainTopKRules(DenialConstraintSet rees) {
        for (DenialConstraint ree : rees) {
            topKREEsTemp.add(ree);
        }
        // score the current top-K REEs and their corresponding interesting scores
        for (int i = 0; i < this.K; i++) {
            if (topKREEsTemp.size() == 0) {
                break;
            }
            topKREEs[i] = topKREEsTemp.poll();
            topKREEScores[i] = topKREEs[i].getInterestingnessScore();
            // update counters
            interestingness.updateCounter(topKREEs[i].getPredicateSet());
        }
        // maintain the current top-K REEs in the temp max heap
        topKREEsTemp.clear();
        for (int i = 0; i < this.K; i++) {
            if (this.topKREEs[i] != null) {
                topKREEsTemp.add(this.topKREEs[i]);
            }
        }
    }

    private double getKthInterestingnessScore() {
        return this.topKREEScores[this.K - 1];
    }

    private void addInvalidX(List<Message> messages) {
        if (messages == null) {
            return;
        }
        for (Message message : messages) {
            PredicateSet kkps = message.getCurrentSet(); //new PredicateSet();
            if (message.getCurrentSupp() < this.support) {
                this.invalidX.add(kkps.getBitset());
            }
            for (Predicate invalidRHS : message.getInvalidRHSs()) {
                PredicateSet temp = new PredicateSet(kkps);
                temp.add(invalidRHS);
                this.invalidX.add(temp.getBitset());
                // add invalid RHS that should be removed
                if (this.invalidXRHSs.containsKey(kkps.getBitset())) {
                    this.invalidXRHSs.get(kkps.getBitset()).add(invalidRHS);
                } else {
                    ArrayList<Predicate> tArr = new ArrayList<>();
                    tArr.add(invalidRHS);
                    this.invalidXRHSs.put(kkps.getBitset(), tArr);
                }
            }
            if (this.if_conf_filter == 1) {

                HashMap<Predicate, Long> allRHSSupports = message.getAllCurrentRHSsSupport();
                for (Map.Entry<Predicate, Long> entry : allRHSSupports.entrySet()) {
                    if (entry.getValue() * 1.0 / message.getCurrentSupp() < this.conf_filter_thr) {
                        if (this.invalidXRHSs.containsKey(kkps.getBitset())) {
                            this.invalidXRHSs.get(kkps.getBitset()).add(entry.getKey());
                        } else {
                            ArrayList<Predicate> tArr = new ArrayList<>();
                            tArr.add(entry.getKey());
                            this.invalidXRHSs.put(kkps.getBitset(), tArr);
                        }
                    }
                }
            }
        }
    }

    private void addValidXRHSs(List<Message> messages) {
        if (messages == null) {
            return;
        }
        for (Message message : messages) {
            PredicateSet kkps = message.getCurrentSet(); //new PredicateSet();
            for (Predicate validRHS : message.getValidRHSs()) {
                PredicateSet temp = new PredicateSet(kkps);
                temp.add(validRHS);
                if (this.validXRHSs.containsKey(temp.getBitset())) {
                    this.validXRHSs.get(temp.getBitset()).add(validRHS);
                } else {
                    ArrayList<Predicate> arr = new ArrayList<>();
                    arr.add(validRHS);
                    this.validXRHSs.put(temp.getBitset(), arr);
                }
                // add valid RHSs -- new ADDED, should be re-test and re-think !!!
                if (this.validXRHSs.containsKey(kkps.getBitset())) {
                    this.validXRHSs.get(kkps.getBitset()).add(validRHS);
                } else {
                    ArrayList<Predicate> arr = new ArrayList<>();
                    arr.add(validRHS);
                    this.validXRHSs.put(kkps.getBitset(), arr);
                }
            }
        }
    }

    private HashMap<IBitSet, Double> getCandidateSupportRatios(List<Message> messages) {
        HashMap<IBitSet, Double> results = new HashMap<>();
        for (Message message : messages) {
            ArrayList<Predicate> kk = message.getCurrent();
            ArrayList<Predicate> candRHSs = message.getCandidateRHSs();
            ArrayList<Long> candSupports = message.getCandidateSupports();
            PredicateSet kkps = new PredicateSet();
            for (Predicate p : kk) {
                kkps.add(p);
            }
            for (int i = 0; i < message.getCandidateRHSs().size(); i++) {
                Predicate candRHS = message.getCandidateRHSs().get(i);
                Long supp = message.getCandidateSupports().get(i);
                PredicateSet temp = new PredicateSet(kkps);
                temp.add(candRHS);
                if (!results.containsKey(temp.getBitset())) {
                    results.put(temp.getBitset(), supp * 1.0 / this.allCount);
                }

            }
        }
        return results;
    }

    /*
        only retrieve the support of the current X
     */
    private HashMap<PredicateSet, Double> getCandidateXSupportRatios(List<Message> messages) {
        HashMap<PredicateSet, Double> results = new HashMap<>();
        if (messages == null) {
            return results;
        }
        for (Message message : messages) {
            PredicateSet kkps = message.getCurrentSet(); //new PredicateSet();
            if (!results.containsKey(kkps)) {
                // results.put(kkps, message.getCurrentSupp() * 1.0 / this.allCount);
                results.put(kkps, message.getCurrentSupp() * 1.0 / this.maxOneRelationNum);
            }

            // consider X + p_0 in message, check their interestingness upper bound later.
            for (Map.Entry<Predicate, Long> entry : message.getAllCurrentRHSsSupport().entrySet()) {
                Predicate rhs = entry.getKey();
                if (message.getValidRHSs().contains(rhs) || message.getInvalidRHSs().contains(rhs)) {
                    continue;
                }
                PredicateSet kkkps = new PredicateSet(kkps);
                kkkps.add(rhs);

                if (!results.containsKey(kkkps)) {
                    if (rhs.isConstant()) {
                        //results.put(kkkps, entry.getValue() * maxOneRelationNum * 1.0 / this.allCount);
                        results.put(kkkps, entry.getValue() * maxOneRelationNum * 1.0 / this.maxOneRelationNum);
                    } else {
                        // results.put(kkkps, entry.getValue() * 1.0 / this.allCount);
                        results.put(kkkps, entry.getValue() * 1.0 / this.maxOneRelationNum);
                    }
                }
            }
        }
        return results;
    }

    private void getCandidateXSupportRatios(HashMap<PredicateSet, Double> results, List<Message> messages) {
        if (messages == null) {
            return;
        }
        for (Message message : messages) {
            PredicateSet kkps = message.getCurrentSet(); //new PredicateSet();
            if (!results.containsKey(kkps)) {
                // results.put(kkps, message.getCurrentSupp() * 1.0 / this.allCount);
                results.put(kkps, message.getCurrentSupp() * 1.0 / this.allCount);
            }

            // consider X + p_0 in message, check their interestingness upper bound later.
            for (Map.Entry<Predicate, Long> entry : message.getAllCurrentRHSsSupport().entrySet()) {
                Predicate rhs = entry.getKey();
                if (message.getValidRHSs().contains(rhs) || message.getInvalidRHSs().contains(rhs)) {
                    continue;
                }
                PredicateSet kkkps = new PredicateSet(kkps);
                kkkps.add(rhs);

                if (!results.containsKey(kkkps)) {
                    if (rhs.isConstant()) {
                        // results.put(kkkps, entry.getValue() * maxOneRelationNum * 1.0 / this.allCount);
                        results.put(kkkps, entry.getValue() * maxOneRelationNum * 1.0 / this.allCount);
                    } else {
                        // results.put(kkkps, entry.getValue() * 1.0 / this.allCount);
                        results.put(kkkps, entry.getValue() * 1.0 / this.allCount);
                    }
                }
            }
        }
    }


    public DenialConstraintSet getTopKREEs() {
        DenialConstraintSet rees = new DenialConstraintSet();
        for (int i = 0; i < this.K; i++) {
            rees.add(this.topKREEs[i]);
        }
        rees.setTotalTpCount(allCount*allCount);
        return rees;
    }

    /**
     * deal with skewness
     * split heavy work units
     */
    public int solveSkewness(ArrayList<WorkUnit> tasks, ArrayList<WorkUnit> tasks_new) {
        HashMap<String, ArrayList<int[]>> recordComs = new HashMap<>();
        int[] _pids = new int[this.maxTupleNum + 1];
        String[] keys = new String[this.maxTupleNum + 1];
        int wid = 0;
        for (wid = 0; wid < tasks.size(); wid++) {
            if (tasks_new.size() > MIN_NUM_WORK_UNITS) {
                break;
            }
            WorkUnit task = tasks.get(wid);
            if (task.getRHSs().size() <= 0) {
                continue;
            }
            // collect the number of pids for each tid
            int tupleNum = 0;
            for (int i = 0; i < _pids.length; i++) _pids[i] = 0;
            // only consider current, because RHS is <t_0, t_1>
            for (Predicate p : task.getCurrrent()) {
                // index1 : pid
                tupleNum = Math.max(tupleNum, p.getIndex1());
                int numOfPid = p.getOperand1().getColumn().getPliSections().size();
                _pids[p.getIndex1()] = numOfPid;
                keys[p.getIndex1()] = p.getOperand1().getColumn().getTableName();
                tupleNum = Math.max(tupleNum, p.getIndex2());
                _pids[p.getIndex2()] = p.getOperand2().getColumn().getPliSections().size();
                keys[p.getIndex2()] = p.getOperand2().getColumn().getTableName();
            }
            String comKey = "";
            for (int i = 0; i <= tupleNum; i++) comKey += keys[i] + "-";
            if (!recordComs.containsKey(comKey)) {
                // enumerate all combinations
                ArrayList<int[]> combinations = new ArrayList<>();
//                int[] oneCom = new int[tupleNum + 1];
                int[] oneCom = new int[this.maxTupleNum + 1];
                enumCombinations(oneCom, combinations, _pids, 0, tupleNum + 1);
                recordComs.put(comKey, combinations);
            }
            // split work units
            ArrayList<int[]> allCombinations = recordComs.get(comKey);
            for (int[] combination : allCombinations) {
                WorkUnit task_new = new WorkUnit(task, combination);
                task_new.setTransferData();
                tasks_new.add(task_new);
            }
        }

        return wid;
    }


    public ArrayList<WorkUnit> solveSkewness(ArrayList<WorkUnit> tasks) {
        ArrayList<WorkUnit> tasks_new = new ArrayList<>();
        HashMap<String, ArrayList<int[]>> recordComs = new HashMap<>();
        int[] _pids = new int[this.maxTupleNum + 1];
        String[] keys = new String[this.maxTupleNum + 1];
        int wid = 0;
        for (wid = 0; wid < tasks.size(); wid++) {
            //if (tasks_new.size() > MIN_NUM_WORK_UNITS) {
            //    break;
            // }
            WorkUnit task = tasks.get(wid);
            // collect the number of pids for each tid
            int tupleNum = 0;
            for (int i = 0; i < _pids.length; i++) _pids[i] = 0;
            // only consider current, because RHS is <t_0, t_1>
            for (Predicate p : task.getCurrrent()) {
                // index1 : pid
                tupleNum = Math.max(tupleNum, p.getIndex1());
                int numOfPid = p.getOperand1().getColumn().getPliSections().size();
                _pids[p.getIndex1()] = numOfPid;
                keys[p.getIndex1()] = p.getOperand1().getColumn().getTableName();
                tupleNum = Math.max(tupleNum, p.getIndex2());
                _pids[p.getIndex2()] = p.getOperand2().getColumn().getPliSections().size();
                keys[p.getIndex2()] = p.getOperand2().getColumn().getTableName();
            }
            String comKey = "";
            for (int i = 0; i <= tupleNum; i++) comKey += keys[i] + "-";
            if (!recordComs.containsKey(comKey)) {
                // enumerate all combinations
                ArrayList<int[]> combinations = new ArrayList<>();
                int[] oneCom = new int[tupleNum + 1];
                enumCombinations(oneCom, combinations, _pids, 0, tupleNum + 1);
                recordComs.put(comKey, combinations);
            }
            // split work units
            ArrayList<int[]> allCombinations = recordComs.get(comKey);
            for (int[] combination : allCombinations) {
                // t0 and t1, do not consider t0 >= t1
                if (combination[0] > combination[1]) {
                    continue;
                }
                WorkUnit task_new = new WorkUnit(task, combination);
                task_new.setTransferData();
                tasks_new.add(task_new);
            }
        }

        return tasks_new;
    }

    private void enumCombinations(int[] currentPIDs, ArrayList<int[]> results, int[] _pids, int script, int tupleNum) {
        if (script >= tupleNum) {
            int[] com = currentPIDs.clone();
            results.add(com);
            return;
        }
        for (int i = 0; i < _pids[script]; i++) {
            currentPIDs[script] = i;
            enumCombinations(currentPIDs, results, _pids, script + 1, tupleNum);

        }
    }

    // split work units according to their RHSs according to different types of tuple ID
    public ArrayList<WorkUnit> partitionWorkUnitsByRHSs(ArrayList<WorkUnit> tasks) {
        ArrayList<WorkUnit> tasks_new = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            WorkUnit task = tasks.get(i);
            // split RHS predicates
            ArrayList<PredicateSet> rhss_group = this.splitRHSsByTIDs(task.getRHSs());
            for (PredicateSet subRHSs : rhss_group) {
                WorkUnit subTask = new WorkUnit(task);
                subTask.resetRHSs(subRHSs);
                tasks_new.add(subTask);
            }
        }
        return tasks_new;
    }

//    public ArrayList<WorkUnit> solveSkewness(ArrayList<WorkUnit> tasks, HashMap<String, Long> tupleNumberRelations) {
//        ArrayList<WorkUnit> tasks_new = new ArrayList<>();
//        ArrayList<Long> minSupps = new ArrayList<>();
//        long avg = 0;
//        for (WorkUnit task : tasks) {
//            if (task.getCurrrent().size() <= 0 || task.getRHSs().size() <= 0) {
//                continue;
//            }
//            task.setMinPredicateSupportT0T1(tupleNumberRelations);
//            minSupps.add(task.getMinPredicateSupportT0T1());
//            avg += task.getMinPredicateSupportT0T1();
//        }
//        avg = (long)(avg * 1.0 / tasks.size()) + 1;
//        for (int i = 0; i < tasks.size(); i++) {
//            WorkUnit task = tasks.get(i);
//            if (task.getMinPredicateSupportT0T1() <= avg) {
//                continue;
//            }
//            int part = Math.min((int)(task.getMinPredicateSupportT0T1() * 1.0 / (avg + 1)), MAX_WORK_UNITS_PARTITION) + 1;
//            if (part < 1) {
//                part = 1;
//            }
//            // split heavy work unit
//            long start = task.getTidT0Start();
//            long end = task.getTidT0End();
//            long num = end - start;
//
//            int step = (int)(num / part) + 1;
//            for (int batch_id = 0; batch_id < part; batch_id++) {
//                long batch_start = batch_id * step;
//                long batch_end = Math.min((batch_id + 1) * step, num);
//                // copy and split
//                // split RHSs
//                ArrayList<PredicateSet> rhss_group = this.splitRHSs(task.getRHSs());
//                for (PredicateSet subRHSs : rhss_group) {
//                    WorkUnit subTask = new WorkUnit(task);
//                    subTask.setTidT0Start(batch_start + start);
//                    subTask.setTidT0End(batch_end + start);
//                    subTask.resetRHSs(subRHSs);
//                    tasks_new.add(subTask);
//                }
//            }
//        }
//        return tasks_new;
//    }

    private ArrayList<PredicateSet> splitRHSsByTIDs(PredicateSet ps) {
        ArrayList<PredicateSet> res = new ArrayList<>();
        HashMap<Integer, PredicateSet> tempMap = new HashMap<>();
        for (Predicate p : ps) {
            int t_0 = p.getIndex1();
            int t_1 = p.getIndex2();
            int key = MultiTuplesOrder.encode(new ImmutablePair<>(t_0, t_1));
            if (tempMap.containsKey(key)) {
                tempMap.get(key).add(p);
            } else {
                PredicateSet tempps = new PredicateSet();
                tempps.add(p);
                tempMap.put(key, tempps);
            }
        }
        for (Map.Entry<Integer, PredicateSet> entry : tempMap.entrySet()) {
            res.add(entry.getValue());
        }
        return res;
    }

    private ArrayList<PredicateSet> splitRHSs(PredicateSet ps) {
        ArrayList<PredicateSet> res = new ArrayList<>();
        int num = ps.size();
        int partition = num / MIN_NUM_WORK_UNITS + 1;
        ArrayList<Predicate> ps_arr = new ArrayList<>();
        for (Predicate p : ps) {
            ps_arr.add(p);
        }
        for (int i = 0; i < partition; i++) {
            int begin = i * MIN_NUM_WORK_UNITS;
            int end = Math.min((i + 1) * MIN_NUM_WORK_UNITS, num);
            if (begin >= end) {
                break;
            }
            PredicateSet temp = new PredicateSet();
            for (int z = begin; z < end; z++) {
                temp.add(ps_arr.get(z));
            }
            res.add(temp);
        }
        return res;
    }

//    public ArrayList<WorkUnit> solveSkewness_bak(ArrayList<WorkUnit> tasks) {
//        ArrayList<WorkUnit> tasks_new = new ArrayList<>();
//        if (tasks.size() >= MIN_NUM_WORK_UNITS) {
//            return tasks;
//        }
//        // get the average cost of work units
//        int cost = 0;
//        for (WorkUnit task : tasks) {
//            cost += task.getRHSs().size();
//        }
//        cost = (int)(cost * 1.0 / tasks.size());
//
//        for (WorkUnit task : tasks) {
//            if (task.getRHSs().size() <= cost) {
//                tasks_new.add(task);
//                continue;
//            } else {
//                int c = 1;
//                WorkUnit tt = new WorkUnit(task.getCurrrent());
//                for (Predicate p : task.getRHSs()) {
//                    if (c % cost == 0 || c == task.getRHSs().size()) {
//                        tt.addRHS(p);
//                        tasks_new.add(tt);
//                        tt = new WorkUnit(task.getCurrrent());
//                    } else {
//                        tt.addRHS(p);
//                    }
//                    c++;
//                }
//            }
//        }
//        return tasks_new;
//    }


    // integrate messages
    List<Message> integrateMessages(List<Message> messages) {
        List<Message> messages_new = new ArrayList<>();
        HashMap<PredicateSet, ArrayList<Message>> messageGroups = new HashMap<>();
        // set key
        for (Message message : messages) {
            message.transformCurrent();
            PredicateSet key = message.getCurrentSet();
            if (messageGroups.containsKey(key)) {
                messageGroups.get(key).add(message);
            } else {
                ArrayList<Message> tt = new ArrayList<>();
                tt.add(message);
                messageGroups.put(key, tt);
            }
        }

//        logger.info(">>>>message groups: {}", messageGroups.keySet());
        // integration
        for (Map.Entry<PredicateSet, ArrayList<Message>> entry : messageGroups.entrySet()) {
//            logger.info(">>>>merge : {} | {}", entry.getKey(), entry.getValue().size());
//            if (entry.getValue().size() == 1) {
//                messages_new.add(entry.getValue().get(0));
//            }
            Message mesg = entry.getValue().get(0);
            for (int i = 1; i < entry.getValue().size(); i++) {
                // mesg.mergeAllCurrentRHSsSupport(entry.getValue().get(i).getAllCurrentRHSsSupport());
                mesg.mergeMessage(entry.getValue().get(i));
            }
            mesg.updateMessage(this.support, this.confidence, this.maxOneRelationNum);
            messages_new.add(mesg);
        }
        return messages_new;
    }

    public void writeToFile(HashMap<PredicateSet, Long> ps_rewards) {
        if (ps_rewards.size() == 0) {
            return;
        }
        // save sequence to file
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(this.RL_code_path + "/model/sequence_" + this.table_name + ".txt"));
            // transform to sequence string
            int count = 0;
            String sequence = "";
            for (Map.Entry<PredicateSet, Long> entry : ps_rewards.entrySet()) {
                PredicateSet Psel = entry.getKey();
                long reward = entry.getValue();
                for (int rm_id = 0; rm_id < Psel.size(); rm_id = rm_id + 1) {
                    int next_p = 0;
                    int i = 0;
                    for (Predicate p : Psel) {
                        if (i == rm_id) {
                            next_p = this.allExistPredicates.indexOf(p);
                            i = i + 1;
                            continue;
                        }
                        sequence += String.valueOf(this.allExistPredicates.indexOf(p));
                        sequence += " ";
                        i = i + 1;
                    }
                    sequence = sequence.substring(0, sequence.length() - 1); // remove last " "
                    sequence += ",";
                    sequence += String.valueOf(next_p);
                    sequence += ",";
                    sequence += String.valueOf(reward);
                    sequence += "\n";
                    count = count + 1;
                }
            }
            sequence = sequence.substring(0, sequence.length() - 1); // remove last "\n"
            logger.info("#### there are {} Psel with {} sequences to train RL model", ps_rewards.size(), count);
            out.write(String.valueOf(this.allExistPredicates.size()));
            out.write("\n");
            out.write(sequence);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // put sequence file to HDFS
        Process proc;
        try {
            String RL_model_path = this.RL_code_path + "/model/";
            String shell_code_path = this.RL_code_path + "/put_sequence.sh";
            String[] argv = new String[]{"sh", shell_code_path, RL_model_path, this.table_name};
            proc = Runtime.getRuntime().exec(argv);
            // obtain the result with the input/output stream
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(proc.getInputStream())));
            BufferedReader err = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(proc.getErrorStream())));
            String line = null;
            while ((line = in.readLine()) != null) {
                logger.info("put sequence file to HDFS info: {}", line);
            }
            while ((line = err.readLine()) != null) {
                logger.info("put sequence file to HDFS error: {}", line);
            }
            in.close();
            err.close();
            proc.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void writeToFile(String sequence) {
        // save sequence to file
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(this.RL_code_path + "/model/sequence_" + this.table_name + "_N" + this.N + ".txt"));
            out.write(String.valueOf(this.allExistPredicates.size()));
            out.write("\n");
            out.write(sequence);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // put sequence file to HDFS
        Process proc;
        try {
            String RL_model_path = this.RL_code_path + "/model/";
            String shell_code_path = this.RL_code_path + "/put_sequence.sh";
            String[] argv = new String[]{"sh", shell_code_path, RL_model_path, this.table_name, String.valueOf(this.N)};
            proc = Runtime.getRuntime().exec(argv);
            // obtain the result with the input/output stream
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(proc.getInputStream())));
            BufferedReader err = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(proc.getErrorStream())));
            String line = null;
            while ((line = in.readLine()) != null) {
                logger.info("put sequence file to HDFS info: {}", line);
            }
            while ((line = err.readLine()) != null) {
                logger.info("put sequence file to HDFS error: {}", line);
            }
            in.close();
            err.close();
            proc.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * for offline RL training: Java calls Python to train the RL model, then put it to HDFS
     */
    public int trainRLModel(HashMap<PredicateSet, Long> ps_rewards) {
        if (ps_rewards.size() == 0) {
            return 0;
        }

        // transform to sequence string
        int count = 0;
        String sequence = "";
        for (Map.Entry<PredicateSet, Long> entry : ps_rewards.entrySet()) {
            PredicateSet Psel = entry.getKey();
            long reward = entry.getValue();
            for (int rm_id = 0; rm_id < Psel.size(); rm_id = rm_id + 1) {
                int next_p = 0;
                int i = 0;
                for (Predicate p : Psel) {
                    if (i == rm_id) {
                        next_p = this.allExistPredicates.indexOf(p);
                        i = i + 1;
                        continue;
                    }
                    sequence += String.valueOf(this.allExistPredicates.indexOf(p));
                    sequence += " ";
                    i = i + 1;
                }
                sequence = sequence.substring(0, sequence.length() - 1); // remove last " "
                sequence += ",";
                sequence += String.valueOf(next_p);
                sequence += ",";
                sequence += String.valueOf(reward);
                sequence += ";";
                count = count + 1;
            }
        }
        sequence = sequence.substring(0, sequence.length() - 1); // remove last ";"

        logger.info("#### there are {} Psel with {} sequences to train RL model", ps_rewards.size(), count);

        // train RL model
        logger.info("#### the sequences for RL training is: {}", sequence);
        useRL(sequence, 1, 1, "", this.allExistPredicates.size(),
                this.PI_path, this.RL_code_path, this.learning_rate, this.reward_decay, this.e_greedy,
                this.replace_target_iter, this.memory_size, this.batch_size, this.table_name, this.N);

        // save RL model to HDFS
        putLoadModelToFromHDFS(this.RL_code_path, 1, this.table_name);

        return 1;
    }

    /**
     * train RL model, or use RL model to predict next_p or whether expand
     */
    public String useRL(String sequence, int ifTrain, int ifInitTrain, String legal_nextP_indices,
                        int allPredicatesNum, String python_path, String RL_code_path,
                        float lr, float rd, float eg, int rtr, int ms, int bs,
                        String table_name, int N_num) {
        String predicted_results = "";
        Process proc;
        try {
            String python_code_path;
            if (ifTrain == 1) {
                python_code_path = RL_code_path + "/train.py";
            } else {
                python_code_path = RL_code_path + "/predict.py";
            }
            String model_path = RL_code_path + "/model/" + table_name + "_N" + N_num + "/model.ckpt";
            String[] argv = new String[]{python_path, python_code_path,
                    "-model_path", model_path,
                    "-init_train", String.valueOf(ifInitTrain),
                    "-predicate_num", String.valueOf(allPredicatesNum),
                    "-sequence", sequence,
                    "-legal_nextP_indices", legal_nextP_indices,
                    "-learning_rate", String.valueOf(lr),
                    "-reward_decay", String.valueOf(rd),
                    "-e_greedy", String.valueOf(eg),
                    "-replace_target_iter", String.valueOf(rtr),
                    "-memory_size", String.valueOf(ms),
                    "-batch_size", String.valueOf(bs)};
            proc = Runtime.getRuntime().exec(argv);
            // obtain the result with the input/output stream
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(proc.getInputStream())));
            BufferedReader err = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(proc.getErrorStream())));
            String line = null;
            while ((line = in.readLine()) != null) {
                logger.info("train/predict RL info: {}", line);
                if (ifTrain != 1) {
                    predicted_results = line;
                }
            }
            while ((line = err.readLine()) != null) {
                logger.info("train/predict RL error: {}", line);
            }
            in.close();
            err.close();
            proc.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return predicted_results;
    }

    public void putLoadModelToFromHDFS(String RL_code_path, int ifPut, String table_name) {
        Process proc;
        try {
            String RL_model_path = RL_code_path + "/model/";
            String shell_code_path;
            if (ifPut == 1) {
                shell_code_path = RL_code_path + "/put_model.sh";
            } else {
                shell_code_path = RL_code_path + "/load_model.sh ";
            }
            String[] argv = new String[]{"sh", shell_code_path, RL_model_path, table_name};
            proc = Runtime.getRuntime().exec(argv);
            // obtain the result with the input/output stream
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(proc.getInputStream())));
            BufferedReader err = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(proc.getErrorStream())));
            String line = null;
            while ((line = in.readLine()) != null) {
                logger.info("put/load model ckpt to/from HDFS info: {}", line);
            }
            while ((line = err.readLine()) != null) {
                logger.info("put/load model ckpt to/from HDFS error: {}", line);
            }
            in.close();
            err.close();
            proc.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public String transformSequence(ArrayList<Predicate> allSinglePredicates,
                                    ArrayList<PredicateSet> Psel,
                                    ArrayList<Predicate> next_p,
                                    ArrayList<Long> reward) {
        String res = "";
        for (int i = 0; i < Psel.size(); i = i + 1) {
            for (Predicate p : Psel.get(i)) {
                res += String.valueOf(allSinglePredicates.indexOf(p));
                res += " ";
            }
            // remove last " "
            res = res.substring(0, res.length() - 1);
            if (next_p == null) { // for predicting next predicate with maximum predicated reward; input: Psel, null, null
                res += ";";
                continue;
            }
            res += ",";
            res += String.valueOf(allSinglePredicates.indexOf(next_p.get(i)));
            if (reward == null) { // for check whether Psel can be expanded with next_p; input: Psel, next_p, null
                res += ";";
                continue;
            }
            res += ",";
            res += String.valueOf(reward.get(i));
            res += ";";
        }
        // remove last ";"
        if (res.length() > 0) // in case that Psel is empty
            res = res.substring(0, res.length() - 1);
        return res;
    }

    public String getLegalNextPIndices(ArrayList<Predicate> allSinglePredicates,
                                       ArrayList<Integer> highSelectivityPredicateIndices,
                                       ArrayList<HashSet<Integer>> Psel_indices_set) {
        String result = "";
        for (int i = 0; i < Psel_indices_set.size(); i = i + 1) {
            boolean ifExist = false;
            for (int index = 0; index < allSinglePredicates.size(); index = index + 1) {
                if (Psel_indices_set.get(i).contains(allSinglePredicates.get(index).getIndex1()) &&
                        highSelectivityPredicateIndices.contains(index)) {
                    ifExist = true;
                    result += String.valueOf(index);
                    result += " ";
                    continue;
                }
                if (!allSinglePredicates.get(index).isConstant() &&
                        Psel_indices_set.get(i).contains(allSinglePredicates.get(index).getIndex2()) &&
                        highSelectivityPredicateIndices.contains(index)) {
                    ifExist = true;
                    result += String.valueOf(index);
                    result += " ";
                }
            }
            if (ifExist) {
                result = result.substring(0, result.length() - 1); // remove last " "
            }
            result += ";";
        }
        result = result.substring(0, result.length() - 1); // remove last ";"
        return result;
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
            if (rhs.getIndex1() == 0 && rhs.getIndex2() == 2) {
                return true;
            }
        }
        return false;
    }

    private void deduplicateSequence(ArrayList<PredicateSet> Psel,
                                     ArrayList<PredicateSet> RHSs,
                                     ArrayList<HashSet<Integer>> Psel_indices_set) {
        HashMap<IBitSet, Integer> counts = new HashMap<>();
        for (PredicateSet ps : Psel) {
            IBitSet key = ps.getBitset();
            if (counts.containsKey(key)) {
                counts.put(key, counts.get(key) + 1);
            } else {
                counts.put(key, 1);
            }
        }
        ArrayList<Integer> removeIndex = new ArrayList<>();
        for (int i = 0; i < Psel.size(); i = i + 1) {
            IBitSet key = Psel.get(i).getBitset();
            if (counts.get(key) > 1) {
                removeIndex.add(i);
                counts.put(key, counts.get(key) - 1);
            }
        }
        for (int i = removeIndex.size() - 1; i >= 0; i = i - 1) {
            int rm_idx = removeIndex.get(i);
            Psel.remove(rm_idx);
            RHSs.remove(rm_idx);
            Psel_indices_set.remove(rm_idx);
        }
    }

    private void pruneSequence(ArrayList<PredicateSet> Psel,
                               ArrayList<PredicateSet> RHSs,
                               ArrayList<HashSet<Integer>> Psel_indices_set) {
        ArrayList<Integer> removeIndex = new ArrayList<>();
        for (int i = 0; i < Psel.size(); i = i + 1) {
            IBitSet key = Psel.get(i).getBitset();
            // remove invalid Psel
            if (this.invalidX.contains(key)) {
                removeIndex.add(i);
                continue;
            }
            // removed invalid RHSs
            if (this.invalidXRHSs.containsKey(key)) {
                for (Predicate invalidRHS : this.invalidXRHSs.get(key)) {
                    RHSs.get(i).remove(invalidRHS);
                }
            }
            // check 0 RHSs
            if (RHSs.get(i).size() <= 0) {
                removeIndex.add(i);
                continue;
            }
            // remove valid RHSs
            if (this.validXRHSs.containsKey(key)) {
                for (Predicate validRHS : this.validXRHSs.get(key)) {
                    RHSs.get(i).remove(validRHS);
                }
            }
            // check 0 RHSs
            if (RHSs.get(i).size() <= 0) {
                removeIndex.add(i);
            }
        }
        for (int i = removeIndex.size() - 1; i >= 0; i = i - 1) {
            int rm_idx = removeIndex.get(i);
            Psel.remove(rm_idx);
            RHSs.remove(rm_idx);
            Psel_indices_set.remove(rm_idx);
        }
    }


    /******************************************************************************************
     * local test
     */

    public void levelwiseRuleDiscoveryLocal() {

        this.table_name = "";
        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long) inputLight.getLineCounts()[i]);
            this.table_name += inputLight.getNames().get(i);
            this.table_name += "_";
        }
        this.table_name = this.table_name.substring(0, this.table_name.length() - 1); // remove last "_"
        logger.info("#### table_name: {}", this.table_name);

//        if (table_name.contains("Property_Features")) {
//            removePropertyFeatureCPredicates(this.allPredicates);
//        }

        this.prepareAllPredicatesMultiTuples();

        List<Predicate> tmp_allPredicates = new ArrayList<>();
        for (Predicate p : this.allPredicates) {
            tmp_allPredicates.add(p);
        }
        // remove constant predicates of enumeration type for RHS
        // removeEnumPredicates(tmp_allPredicates);
        ArrayList<Predicate> applicationRHSs = this.applicationDrivenSelection(tmp_allPredicates);

        // remove predicates that are irrelevant to RHSs
        if (this.maxTupleNum <= 2) {
            filterIrrelevantPredicates(applicationRHSs, this.allPredicates);
        }

        // remove constant predicates of enumeration type for X
        removeEnumPredicates(this.allPredicates, this.allExistPredicates);

        int cpsize = 0;
        int psize = 0;
        for (Predicate p : this.allPredicates) {
            if (p.isConstant()) {
                cpsize++;
            } else {
                psize++;
            }
        }
        logger.info("#### after filtering, there are {} predicates, constant size: {}, non-constant size: {}", this.allPredicates.size(), cpsize, psize);

        // 1. initialize the 1st level combinations
        Lattice lattice = new Lattice(this.maxTupleNum);
        // only for test application-driven methods
//        ArrayList<Predicate> applicationRHSs = this.applicationDrivenSelection(this.allPredicates);
//        ArrayList<Predicate> applicationRHSs = new ArrayList<>();
////        for (Predicate p : this.allPredicates) {
////            // if p is a constant, only consider t_0
////            if (p.isConstant() && p.getIndex1() == 1) {
////                continue;
////            }
////            applicationRHSs.add(p);
////        }
//        int ccount = 0;
//        for(int i = 0; i < this.allPredicates.size(); i=i+1) {
//            if (this.allPredicates.get(i).getOperand1().getColumn().getTableName().contains("Author2Paper")) {
//                continue;
//            }
//            applicationRHSs.add(this.allPredicates.get(i));
//            ccount ++;
//            if (ccount == 4) {
//                break;
//            }
//        }

        lattice.initialize(this.allPredicates, this.maxTupleNum, applicationRHSs);

        // if there exist trained model. Since Psel could be empty initially, at level 0, leading to no model trained.
        boolean ifExistModel = true;

        int level = 0;

        // for offline RL
        String sequenceResults = "";
        int count_seq = 0;
        boolean ifReachN = false;

        while (lattice != null && level <= MAX_CURRENT_PREDICTES) {
            // collect a set of work units
//            logger.info("Search level : {} with lattice {}", level, lattice.printCurrents());
            logger.info("Search level : {} with lattice", level);

            ArrayList<WorkUnit> workUnits_ = lattice.generateWorkUnits();
            workUnits_ = this.mergeWorkUnits(workUnits_);
            // solve skewness
//            workUnits = this.solveSkewness(workUnits);
            for (WorkUnit task : workUnits_) {
                task.setTransferData();
            }

            // batch mode
            ArrayList<WorkUnit> workUnits_init = workUnits_;
            HashMap<PredicateSet, Double> currentSupports = new HashMap<>();
            String option = "original";

            // use MEM to remove workunits that have been computed
            if (ifRL == 1 && this.ifOnlineTrainRL == 1) {
                ArrayList<WorkUnit> removeWu = new ArrayList<>();
                for (WorkUnit wu : workUnits_init) {
                    if (this.MEM.contains(wu.getCurrrent()))
                        removeWu.add(wu);
                }
                for (WorkUnit wu : removeWu) {
                    workUnits_init.remove(wu);
                }
            }

            while (workUnits_init.size() != 0) {
                // solve skewness
                // ArrayList<WorkUnit> workUnits = this.solveSkewness(workUnits_init);
                ArrayList<WorkUnit> workUnits = new ArrayList<>();
                int remainStartSc = this.solveSkewness(workUnits_init, workUnits);
                // prepare data shipment
                //this.dataTransfer(workUnits);
                // validate all work units
                List<Message> messages = null;
                if (workUnits.size() > 0) {
                    //messages = this.runLocal(workUnits);
                    messages = this.runLocal_new(workUnits);
                    logger.info("Integrate messages ...");
                    messages = this.integrateMessages(messages);
                }

                // for offline RL
                if (this.ifRL == 1 && this.ifOnlineTrainRL == 0 && this.ifOfflineTrainStage == 1 && messages != null) {
                    for (Message msg : messages) {
                        // method 1-2
//                        if (msg.getCurrentSet().size() <= 1) {
//                            continue;
//                        }
//                        long X_supp = msg.getCurrentSupp();
//                        if (X_supp == 0) {
//                            continue;
//                        }
//                        this.ps_rewards.put(msg.getCurrentSet(), X_supp - this.support);

                        // method-3
                        String Psel_sequence = "";
                        for (Predicate p : msg.getCurrentSet()) {
                            Psel_sequence += this.allExistPredicates.indexOf(p);
                            Psel_sequence += " ";
                        }
                        Psel_sequence = Psel_sequence.substring(0, Psel_sequence.length() - 1); // remove last " "
                        for (Map.Entry<Predicate, Long> entry : msg.getAllCurrentRHSsSupport().entrySet()) {
                            sequenceResults += Psel_sequence;
                            sequenceResults += ",";
                            sequenceResults += this.allExistPredicates.indexOf(entry.getKey());
                            sequenceResults += ",";
                            if (entry.getValue() - this.support > 0) {
                                sequenceResults += "1\n";
                            } else {
                                sequenceResults += "-1\n";
                            }
                            count_seq = count_seq + 1;
                            if (count_seq >= this.N) {
                                ifReachN = true;
                                break;
                            }
                        }
                        if (ifReachN) {
                            break;
                        }
                    }
                    if (ifReachN) {
                        break;
                    }
                }

                // maintain top-K max heap of REEs
                // logger.info("Returned Messages from all workers");
                if (messages != null) {
                    for (Message message : messages) {
                        // logger.info("message : {}", message);
                        DenialConstraintSet rees = this.transformRule(message);
                        // print all rules
                        for (DenialConstraint ree : rees) {
                            if (ree != null) {
                                logger.info("Valid REE values ###### are : {}", ree.toString());
                            }
                        }
                        this.maintainTopKRules(rees);
                        // transform Current to currentSet
                        message.transformCurrent();
                    }
                }
                // collect supports of candidate rules
//            HashMap<IBitSet, Double> candSupports = this.getCandidateSupportRatios(messages);
                this.getCandidateXSupportRatios(currentSupports, messages);
                // collect invalid X
                this.addInvalidX(messages);
                // collective valid X -> p_0
                this.addValidXRHSs(messages);
                // prune meanineless work units
                if (option.equals("original")) {
                    workUnits_init = this.pruneXWorkUnits(interestingness, this.getKthInterestingnessScore(), currentSupports, workUnits_init, remainStartSc, this.topKOption);
                } else {
                    ArrayList<WorkUnit> remainTasks = new ArrayList<>();
                    for (int tid = remainStartSc; tid < workUnits_init.size(); tid++) {
                        remainTasks.add(workUnits_init.get(tid));
                    }
                    workUnits_init = remainTasks;
                }

            }

            if (this.ifRL == 1 && this.ifOnlineTrainRL == 0 && this.ifOfflineTrainStage == 1 && ifReachN) {
                break;
            }

//            workUnits = this.solveSkewness(workUnits);
//            // prepare data shipment
////            this.dataTransfer(workUnits);
//            // validate all work units
//            List<Message> messages = this.runLocal(workUnits);
//            logger.info("Integrate messages ...");
//            messages = this.integrateMessages(messages);
//            // maintain top-K max heap of REEs
//            for (Message message : messages) {
//                DenialConstraintSet rees = this.transformRule(message);
//                this.maintainTopKRules(rees);
//                message.transformCurrent();
//            }
//            // collect supports of candidate rules
////            HashMap<IBitSet, Double> candSupports = this.getCandidateSupportRatios(messages);
//            HashMap<PredicateSet, Double> currentSupports = this.getCandidateXSupportRatios(messages);
//            // collect invalid X
//            this.addInvalidX(messages);
//            // collect valid X -> p_0
//            this.addValidXRHSs(messages);


            // go to next lattice
            // prune using support and interestingness UB
            // prune invalid lattice nodes
            lattice.pruneLattice(invalidX, invalidXRHSs, validXRHSs, allPredicates);
            // prune with interestingness UB
//            if (option.equals("original")) {
//                lattice.pruneXInterestingnessUB(interestingness, this.getKthInterestingnessScore(), currentSupports);
//            } else if (option.equals("anytime")) {
//                lattice.pruneXInterestingnessUB(interestingness, this.getKthInterestingnessScore(), currentSupports, null);
//            }


            lattice.setAllLatticeVertexBits(lattice.getLatticeLevel());
            Lattice nextLattice = lattice.generateNextLatticeLevel(this.allPredicates, this.allExistPredicates, this.invalidX, this.invalidXRHSs, this.validXRHSs,
                    interestingness, this.getKthInterestingnessScore(), currentSupports, predicateProviderIndex, option, null,
                    this.ifRL, this.ifOnlineTrainRL, this.ifOfflineTrainStage, ifExistModel, this.PI_path, this.RL_code_path,
                    this.learning_rate, this.reward_decay, this.e_greedy, this.replace_target_iter, this.memory_size, this.batch_size, this.table_name, this.N, this.predicateDQNHashIDs, this.topKOption);
            // pruning
            nextLattice.removeInvalidLatticeAndRHSs(lattice);
            if (nextLattice == null || nextLattice.size() == 0) {
                break;
            }
            lattice = nextLattice;
            level++;
        }

        // offline train RL model
        long beginOfflineTrainRLTime = System.currentTimeMillis();
        if (this.ifRL == 1 && this.ifOnlineTrainRL == 0 && this.ifOfflineTrainStage == 1) {
            // method-1
//            trainRLModel(this.ps_rewards);

            // method-2
//            writeToFile(this.ps_rewards);

            // method-3
            writeToFile(sequenceResults.substring(0, sequenceResults.length() - 1));
        }
        long offlineTrainRLTime = System.currentTimeMillis() - beginOfflineTrainRLTime;
        logger.info("#### finish offline training RL model, using time: {}", offlineTrainRLTime);
    }

    private boolean test2(WorkUnit task) {
        PredicateSet X = task.getCurrrent();
        PredicateSet Y = task.getRHSs();

        HashSet<String> X_dict = new HashSet<>();
        X_dict.add("user_info.t0.city == user_info.t1.city");
        X_dict.add("user_info.t1.row_id == user_info.t2.row_id");

        if (X.size() != 2) {
            return false;
        }
        for (Predicate p : X) {
            if (!X_dict.contains(p.toString().trim())) {
                return false;
            }
        }
        return true;
    }


    private boolean test3(WorkUnit task) {
        PredicateSet X = task.getCurrrent();
        PredicateSet Y = task.getRHSs();

        HashSet<String> X_dict = new HashSet<>();
        X_dict.add("airports.t0.iso_region == airports.t1.iso_region");
        X_dict.add("airports.t1.iso_country == US");

        if (X.size() != 2) {
            return false;
        }
        for (Predicate p : X) {
            if (!X_dict.contains(p.toString().trim())) {
                return false;
            }
        }
        return true;
    }


    private boolean test1(WorkUnit task) {
        PredicateSet X = task.getCurrrent();
        PredicateSet Y = task.getRHSs();

        // test 1: user_info.t0.city == user_info.t1.city  user_info.t0.sn == user_info.t1.sn  user_info.t0.gender == user_info.t1.gender ]
        //                         -> {  user_info.t0.name == user_info.t1.name }
        HashSet<String> X_dict = new HashSet();
        X_dict.add("user_info.t0.city == user_info.t1.city");
        X_dict.add("user_info.t0.sn == user_info.t1.sn");
        X_dict.add("user_info.t0.gender == user_info.t1.gender");
        HashSet<String> Y_dict = new HashSet<>();
        Y_dict.add("user_info.t0.name == user_info.t1.name");

        if (X.size() != 3) {
            return false;
        }

        for (Predicate p : X) {
            if (!X_dict.contains(p.toString().trim())) {
                return false;
            }
        }
        boolean f = false;
        for (Predicate p : Y) {
            if (Y_dict.contains(p.toString().trim())) {
                f = true;
            }
        }
        return f;
    }

    public List<Message> runLocal(ArrayList<WorkUnit> workUnits) {

        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long) inputLight.getLineCounts()[i]);
        }

        List<Message> ruleMessages = new ArrayList<>();

        for (WorkUnit task : workUnits) {
            // validate candidate rules in workers
//             logger.info("Work Unit : {}".format(task.toString()));

            if (task.getCurrrent().size() == 0) {
                continue;
            }
//             if (test2(task)) {
            if (true) {
                // test 1: user_info.t0.city == user_info.t1.city  user_info.t0.sn == user_info.t1.sn  user_info.t0.gender == user_info.t1.gender ]
                //                         -> {  user_info.t0.name == user_info.t1.name }

                logger.info("Work Unit : {}".format(task.toString()));

//                 MultiTuplesRuleMining multiTuplesRuleMining = new MultiTuplesRuleMining(this.maxTupleNum,
//                         this.inputLight, this.support, this.confidence, maxOneRelationNum, allCount, tupleNumberRelations);
//
//                 ArrayList<Predicate> current = new ArrayList<>();
//                 for (Predicate p : task.getCurrrent()) {
//                     current.add(p);
//                 }
//                 List<Message> messages = multiTuplesRuleMining.validation_new(current, task.getRHSs(), task.getPids());


                MultiTuplesRuleMiningOpt multiTuplesRuleMining = new MultiTuplesRuleMiningOpt(this.maxTupleNum,
                        this.inputLight, this.support, this.confidence, maxOneRelationNum, allCount, tupleNumberRelations);

                ArrayList<Predicate> current = new ArrayList<>();
                for (Predicate p : task.getCurrrent()) {
                    current.add(p);
                }
                List<Message> messages = multiTuplesRuleMining.validation(current, task.getRHSs(), task.getPids());


                ruleMessages.addAll(messages);

            }
        }

        return ruleMessages;

    }

    public List<Message> runLocal_new(ArrayList<WorkUnit> workUnits) {

        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long) inputLight.getLineCounts()[i]);
        }

        List<Message> ruleMessages = new ArrayList<>();

        ArrayList<WorkUnits> unitSets = new ArrayList<>();
        for (WorkUnit workUnit : workUnits) {
            WorkUnits cluster = new WorkUnits();
            cluster.addUnit(workUnit);
            unitSets.add(cluster);
        }

        // set AllCount of each work unit
        for (WorkUnit task : workUnits) {
            task.clearData();
        }
        for (WorkUnits set : unitSets) {
            set.setAllCount(this.allCount);
        }

        for (WorkUnits unitSet : unitSets) {
            // validate candidate rules in workers
//             logger.info("Work Unit : {}".format(task.toString()));

            Map<PredicateSet, List<Predicate>> validConsRuleMap = validConstantRule;
            Map<PredicateSet, Map<String, Predicate>> constantXMap = new HashMap<>();
            PredicateSet sameSet = unitSet.getSameSet();
            for (PredicateSet set : validConsRuleMap.keySet()) {
                PredicateSet tupleX = new PredicateSet();
                Map<String, Predicate> constantX = new HashMap<>();
                for (Predicate p : set) {
                    if (p.isConstant()) {
                        constantX.put(p.getOperand1().toString_(0), p);
                    } else {
                        tupleX.add(p);
                    }
                }

                if (tupleX.size() > 0) {
                    constantXMap.putIfAbsent(tupleX, constantX);
                }

            }


            List<WorkUnit> units = unitSet.getUnits();
//            WorkUnits newTask = new WorkUnits();
            for(PredicateSet tuplePs : constantXMap.keySet()) {
                if(sameSet.size() > 0) {
                    if(!tuplePs.containsPS(sameSet)) {
                        continue;
                    }
                }
                for (WorkUnit unit : units) {
                    PredicateSet tupleX = new PredicateSet();
                    PredicateSet constantX = new PredicateSet();
                    for (Predicate p : unit.getCurrrent()) {
                        if (p.isConstant()) {
                            constantX.add(p);
                        } else {
                            tupleX.add(p);
                        }
                    }


                    if (tupleX.containsPS(tuplePs)) {
                        Map<String, Predicate> constantsMap = constantXMap.get(tuplePs);
                        boolean iscont = true;
                        for (Predicate p : constantX) {
                            if (!constantsMap.containsKey(p.getOperand1().toString_(0))) {
                                iscont = false;
                                break;
                            }
                        }

                        if (iscont) {
                            PredicateSet lhs = new PredicateSet();
                            lhs.addAll(tuplePs);
                            for (Predicate p : constantsMap.values()) {
                                lhs.add(p);
                            }
                            List<Predicate> rhs = validConsRuleMap.get(lhs);

                            for (Predicate p : rhs) {
                                if (unit.getRHSs().containsPredicate(p)) {
                                    unit.getRHSs().remove(p);
                                    logger.info(">>>> test cut: {}", p);
                                }
                            }
                        }
                    }
                }

//                if(unit.getRHSs().size() > 0) {
//                    newTask.addUnit(unit);
//                }
            }

            Predicate pBegin = null;
            for (Predicate p : unitSet.getSameSet()) {
                if (!p.isML() && !p.isConstant()) {
                    pBegin = p;
                    break;
                }
            }

//             if (test2(task)) {
            if (true) {
//                if (! test3(unitSet.getUnits().get(0))) {
//                    continue;
//                }
                // test 1: user_info.t0.city == user_info.t1.city  user_info.t0.sn == user_info.t1.sn  user_info.t0.gender == user_info.t1.gender ]
                //                         -> {  user_info.t0.name == user_info.t1.name }

//                logger.info("Work Unit : {}".format(task.toString()));

//                 MultiTuplesRuleMining multiTuplesRuleMining = new MultiTuplesRuleMining(this.maxTupleNum,
//                         this.inputLight, this.support, this.confidence, maxOneRelationNum, allCount, tupleNumberRelations);
//
//                 ArrayList<Predicate> current = new ArrayList<>();
//                 for (Predicate p : task.getCurrrent()) {
//                     current.add(p);
//                 }
//                 List<Message> messages = multiTuplesRuleMining.validation_new(current, task.getRHSs(), task.getPids());


                MultiTuplesRuleMiningOpt multiTuplesRuleMining = new MultiTuplesRuleMiningOpt(this.maxTupleNum,
                        this.inputLight, this.support, this.confidence, maxOneRelationNum, allCount, tupleNumberRelations);

                List<Message> messages = multiTuplesRuleMining.validationMap1(unitSet, pBegin);


                ruleMessages.addAll(messages);

            }
        }

        return ruleMessages;

    }



    /*****************************************************************************************************************************

     *****************************************************************************************************************************/

    public void computeRewardAndSaveMEMLocal(ArrayList<PredicateSet> Psel,
                                             ArrayList<Predicate> next_p,
                                             ArrayList<Long> reward,
                                             ArrayList<PredicateSet> RHSs) {
        // in case that there exists same workunit, i.e., {Psel, next_p}
        ArrayList<Boolean> visited = new ArrayList<>();
        for (int i = 0; i < Psel.size(); i = i + 1) {
            visited.add(false);
        }

//        Map<PredicateSet, List<DenialConstraint>> rulePS = new HashMap<>();
//        for (DenialConstraint dc: topKREEsTemp) {
//            PredicateSet lhs = dc.getPredicateSet();
//            PredicateSet keyPredicate = new PredicateSet();
//            for (Predicate p : lhs) {
//                if(!p.isConstant() && !dc.getRHS().equals(p)) {
//                    keyPredicate.add(p);
//                }
//            }
//            rulePS.putIfAbsent(keyPredicate, new ArrayList<>());
//            rulePS.get(keyPredicate).add(dc);
//        }
        // generate WorkUnits
        ArrayList<WorkUnit> workUnits_ = new ArrayList<>();
        for (int i = 0; i < Psel.size(); i++) {
            WorkUnit workUnit = new WorkUnit();
//            PredicateSet tuplePredicate = new PredicateSet();
//            PredicateSet constPredicate = new PredicateSet();
            for (Predicate p : Psel.get(i)) {
                workUnit.addCurrent(p);
//                if (!p.isConstant()) {
//                    tuplePredicate.add(p);
//                } else {
//                    constPredicate.add(p);
//                }
            }
            workUnit.addCurrent(next_p.get(i));
//            if (!next_p.get(i).isConstant()) {
//                tuplePredicate.add(next_p.get(i));
//            } else {
//                constPredicate.add(next_p.get(i));
//            }
            for (Predicate p : RHSs.get(i)) {
                if (!this.validRHS(p)) {
                    continue;
                }
                workUnit.addRHS(p);
            }
//            if(rulePS.containsKey(tuplePredicate)) {
//                List<DenialConstraint> dcList = rulePS.get(tuplePredicate);
//                for(DenialConstraint dc : dcList) {
//
//                }
//            }

            this.MEM.add(workUnit.getCurrrent());
            workUnits_.add(workUnit);
        }

        ArrayList<WorkUnit> workUnits_init = workUnits_;
        workUnits_init = this.mergeWorkUnits(workUnits_init);


        for (WorkUnit task : workUnits_init) {
            task.setTransferData();
        }

        HashMap<PredicateSet, Double> currentSupports = new HashMap<>();
        String option = "original";
        while (workUnits_init.size() != 0) {
            // solve skewness
            ArrayList<WorkUnit> workUnits = new ArrayList<>();
            int remainStartSc = this.solveSkewness(workUnits_init, workUnits);
            // validate all work units
            List<Message> messages = null;
            if (workUnits.size() > 0) {
                messages = this.runLocal(workUnits);
                logger.info("Integrate messages ...");
                messages = this.integrateMessages(messages);
            }

            // update reward for RL
            if (messages != null) {
                for (Message msg : messages) {
                    if (msg.getCurrentSupp() == 0) {
                        continue;
                    }
                    for (int tmp_i = 0; tmp_i < workUnits_.size(); tmp_i = tmp_i + 1) {
                        if (msg.getCurrentSet().equals(workUnits_.get(tmp_i).getCurrrent()) && !visited.get(tmp_i)) {
                            reward.set(tmp_i, msg.getCurrentSupp() - this.support);
                            visited.set(tmp_i, true);
                        }
                    }
                }
            }

            // maintain top-K max heap of REEs
            if (messages != null) {
                for (Message message : messages) {
                    DenialConstraintSet rees = this.transformRule(message);
                    // print all rules
                    for (DenialConstraint ree : rees) {
                        if (ree != null) {
                            logger.info("Valid REE values ###### are : {}", ree.toString());
                        }
                    }
                    this.maintainTopKRules(rees);
                    // transform Current to currentSet
                    message.transformCurrent();
                }
            }
            // collect supports of candidate rules
            this.getCandidateXSupportRatios(currentSupports, messages);
            // collect invalid X
            this.addInvalidX(messages);
            // collective valid X -> p_0
            this.addValidXRHSs(messages);
            // prune meanineless work units
            if (option.equals("original")) {
                workUnits_init = this.pruneXWorkUnits(interestingness, this.getKthInterestingnessScore(), currentSupports, workUnits_init, remainStartSc, this.topKOption);
            } else {
                ArrayList<WorkUnit> remainTasks = new ArrayList<>();
                for (int tid = remainStartSc; tid < workUnits_init.size(); tid++) {
                    remainTasks.add(workUnits_init.get(tid));
                }
                workUnits_init = remainTasks;
            }
        }

        // remove Psel without reward, since some workunits with high support X are not computed
        for (int i = visited.size() - 1; i >= 0; i = i - 1) {
            if (visited.get(i)) {
                continue;
            }
            Psel.remove(i);
            next_p.remove(i);
            RHSs.remove(i);
            reward.remove(i);
        }
    }

    private static Logger logger = LoggerFactory.getLogger(ParallelRuleDiscoverySampling.class);

    /**************************************************************************************************
     * local test
     */
}
