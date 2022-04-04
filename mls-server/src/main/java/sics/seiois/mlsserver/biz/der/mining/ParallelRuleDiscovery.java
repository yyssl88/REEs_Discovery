package sics.seiois.mlsserver.biz.der.mining;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraint;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraintSet;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.input.InputLight;
import sics.seiois.mlsserver.biz.der.metanome.input.ParsedColumn;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.utils.*;
import sics.seiois.mlsserver.model.SparkContextConfig;
import sics.seiois.mlsserver.service.impl.PredicateSetAssist;

import java.util.*;

public class ParallelRuleDiscovery {
    // predicate set
    // each predicate attaches corresponding data in ParsedColumn
    private List<Predicate> allPredicates;
    private int K; // retrieve top-K rules
    private int k_score; // the current k-th interestingness score
    private int maxTupleNum; // the maximum number of tuples in a rule
    private long support;
    private float confidence;
    private long maxOneRelationNum;
    private long allCount;
    private InputLight inputLight;

    public static int MIN_NUM_WORK_UNITS = 200;

    public static int BATCH_SIZE = 200;

    // max number of partition of Lattice
    public static int NUM_LATTICE = 80;
    public static int MAX_CURRENT_PREDICTES = 4;

    public static int MAX_WORK_UNITS_PARTITION = 20;

    // generate all predicate set including different tuple ID pair
    private static final PredicateProviderIndex predicateProviderIndex = PredicateProviderIndex.getInstance();

    private PriorityQueue<DenialConstraint> topKREEsTemp;
    private DenialConstraint[] topKREEs;
    private Double[] topKREEScores;

    private HashSet<IBitSet> invalidX;
    private HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs;

    private HashMap<IBitSet, ArrayList<Predicate>> validXRHSs;

    private int ifPrune; // 1: use prune; 0: do not use prune

    // interestingness
    Interestingness interestingness;

    public ParallelRuleDiscovery(List<Predicate> predicates, int K, int maxTupleNum, long support,
                                 float confidence, long maxOneRelationNum, Input input, long allCount,
                                 float w_1, float w_2, float w_3, float w_4, float w_5, int ifPrune) {
        this.allPredicates = predicates;
        this.K = K;
        this.maxTupleNum = maxTupleNum;
        this.support = support;
        this.confidence = confidence;
        this.maxOneRelationNum = maxOneRelationNum;
        this.allCount = allCount;
        this.inputLight = new InputLight(input);

        this.ifPrune = ifPrune;

        // set support for each predicate;
        HashMap<String, HashMap<Integer, Long>> statistic = new HashMap<>();
        for (Predicate p : this.allPredicates) {
            // statistic
            // op1
            ParsedColumn<?> col1 = p.getOperand1().getColumn();
            if (! statistic.containsKey(col1.toStringData())) {
                HashMap<Integer, Long> temp = new HashMap<>();
                for (int i = 0; i < col1.getValueIntSize(); i++) {
                     if (! temp.containsKey(col1.getValueInt(i))) {
                         temp.put(col1.getValueInt(i), 1L);
                     } else {
                         temp.put(col1.getValueInt(i), temp.get(col1.getValueInt(i)) + 1);
                     }
                }
                statistic.put(col1.toStringData(), temp);
            }

            // op2
            ParsedColumn<?> col2 = p.getOperand2().getColumn();
            if (! statistic.containsKey(col2.toStringData())) {
                HashMap<Integer, Long> temp = new HashMap<>();
                for (int i = 0; i < col2.getValueIntSize(); i++) {
                    if (! temp.containsKey(col2.getValueInt(i))) {
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
                } else if (o1.getInterestingnessScore() > o2.getInterestingnessScore()){
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
            this.topKREEScores[i] = 0.0;
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

    public void levelwiseRuleDiscovery(String taskId, SparkSession spark, SparkContextConfig sparkContextConfig) {
        // 1. initialize the 1st level combinations

        this.prepareAllPredicatesMultiTuples();


        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long)inputLight.getLineCounts()[i]);
        }

        logger.info("Parallel Mining with Predicate size {} and Predicates {}", this.allPredicates.size(), this.allPredicates);

        Lattice lattice = new Lattice(this.maxTupleNum);
//        lattice.initialize(this.allPredicates, this.maxTupleNum);

        // only for test application-driven methods
        // RHSs predicate: <t_0>, <t_0, t_1>, <t_0, t_2>
        ArrayList<Predicate> applicationRHSs = new ArrayList<>();
        // add all predicates to RHSs
        int t1 = 0, t2 = 2;
        for (Predicate p : this.allPredicates) {
            if (p.isConstant() && p.getIndex1() == 1) {
                continue;
            } else if (p.isConstant() && p.getIndex1() == 0) {
                applicationRHSs.add(p);
            } else {
                // for non-constant predicates, also add <t_0, t_2>
                applicationRHSs.add(p);
                Predicate newp = predicateProviderIndex.getPredicate(p, t1, t2);
                applicationRHSs.add(newp);
            }
        }
//        int ccount = 0;
//        for(int i = 0; i < this.allPredicates.size(); i=i+2) {
//            if (this.allPredicates.get(i).getOperand1().getColumn().getTableName().contains("Author2Paper")) {
//                continue;
//            }
//            if (this.allPredicates.get(i).getOperand1().getColumn().getName().equals("iso_region")) {
//                continue;
//            }
//            applicationRHSs.add(this.allPredicates.get(i));
//            ccount ++;
//            if (ccount == 3) {
//                break;
//            }
//        }
        lattice.initialize(this.allPredicates, this.maxTupleNum, applicationRHSs);

        int level = 0;

        String option = "original";
        if (this.ifPrune == 0) {
            option = "none";
        }

        while (lattice != null && level <= MAX_CURRENT_PREDICTES) {
            // print lattice
            // collect a set of work units
            logger.info("Search level : {}", level);
            level ++;
            ArrayList<WorkUnit> workUnits_init = lattice.generateWorkUnits();
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
//                    messages = this.run(workUnits, taskId, spark, sparkContextConfig, tupleNumberRelations);
                    for (WorkUnit task : workUnits) {
                        task.initIndex(allPredicates, true);
                    }
                    messages = this.run(workUnits, taskId, spark, tupleNumberRelations);
                    logger.info("Integrate messages ...");
                    messages = this.integrateMessages(messages);
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
                    workUnits_init = this.pruneXWorkUnits(interestingness, this.getKthInterestingnessScore(), currentSupports, workUnits_init, remainStartSc);
                } else {
                    ArrayList<WorkUnit> remainTasks = new ArrayList<>();
                    for (int tid = remainStartSc; tid < workUnits_init.size(); tid++) {
                        remainTasks.add(workUnits_init.get(tid));
                    }
                    workUnits_init = remainTasks;
                }

            }


            Lattice nextLattice = runNextLattices(lattice, this.allPredicates, this.invalidX, this.invalidXRHSs, this.validXRHSs,
                    interestingness, this.getKthInterestingnessScore(), currentSupports, predicateProviderIndex, option, null, spark);

            if (nextLattice == null || nextLattice.size() == 0) {
                break;
            }
            lattice = nextLattice;
        }
    }

    /*
        prune work units using interestingness scores and supports
     */
    private ArrayList<WorkUnit> pruneXWorkUnits(Interestingness interestingness, double KthScore, HashMap<PredicateSet, Double> supp_ratios,
                                                ArrayList<WorkUnit> tasks, int remainedStartSc) {
        ArrayList<WorkUnit> prunedTasks = new ArrayList<>();
        for (int tid = remainedStartSc; tid < tasks.size(); tid++) {
            WorkUnit task = tasks.get(tid);

            if (supp_ratios.containsKey(task.getCurrrent())) {
               // check support
               long suppOne = (long)(supp_ratios.get(task.getCurrrent()) * this.maxOneRelationNum);
               if (suppOne < support) {
                   continue;
               }
               // check interestingness UB
                double ub = interestingness.computeUB(suppOne * 1.0 / this.maxOneRelationNum, 1.0, task.getCurrrent(), null, "");
               if (ub < KthScore) {
                   continue;
               }
            }
            prunedTasks.add(task);
        }
        return prunedTasks;
    }

    public Lattice runNextLattices(Lattice lattice, List<Predicate> allPredicates, HashSet<IBitSet> invalidX,
                                   HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs,
                                   HashMap<IBitSet, ArrayList<Predicate>> validXRHSs,
                                   Interestingness interestingness, double KthScore,
                                   HashMap<PredicateSet, Double> suppRatios,
                                   PredicateProviderIndex predicateProviderIndex,
                                   String option, ArrayList<LatticeVertex> partialRules, SparkSession spark) {

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        BroadcastLattice broadcastLattice = new BroadcastLattice(allPredicates, invalidX, invalidXRHSs, validXRHSs,
                interestingness, KthScore, suppRatios, predicateProviderIndex, option);

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
            lattice.pruneXInterestingnessUB(interestingness, KthScore, suppRatios, "");
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

            Lattice latticeWorker = task.generateNextLatticeLevel(bLattice.getAllPredicates(), null, bLattice.getInvalidX(),bLattice.getInvalidRHSs(), bLattice.getValidXRHSs(),
                    bLattice.getInterestingness(), bLattice.getKthScore(), bLattice.getSuppRatios(), bLattice.getPredicateProviderIndex(), bLattice.getOption(), null,
                    0, 0, 0, false, "", "", 0, 0, 0, 0, 0, 0, "", 0, null, null);
            return latticeWorker;

        }).aggregate(null, new ILatticeAggFunction(), new ILatticeAggFunction());

        // logger.info("After generating lattices for next level : {}", nextLattice.printCurrents());

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
                             SparkContextConfig sparkContextConfig, HashMap<String, Long> tupleNumberRelations)  {

        return this.run(workUnits, taskId, spark, tupleNumberRelations);
    }

    public List<Message> run(ArrayList<WorkUnit> workUnits, String taskId, SparkSession spark, HashMap<String, Long> tupleNumberRelations)  {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Broadcast<List<Predicate>> broadAllPredicate = sc.broadcast(allPredicates);
        BroadcastObj broadcastObj = new BroadcastObj(this.maxTupleNum, this.inputLight, this.support, this.confidence,
                this.maxOneRelationNum, tupleNumberRelations);
        // broadcast data
        // ... left for future
//        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Broadcast<BroadcastObj> scInputLight = sc.broadcast(broadcastObj);

        // broadcast provideIndex, i.e., all predicates
        PredicateSetAssist psAssist = new PredicateSetAssist();
        psAssist.setIndexProvider(PredicateSet.indexProvider);//在这里设置，这个值肯定是确定了的
        psAssist.setBf(PredicateSet.bf);
        psAssist.setTaskId(taskId);
        Broadcast<PredicateSetAssist> bcpsAssist = sc.broadcast(psAssist);
//        logger.info("####level:{}, Before dig indexProvider size:{}, list:{}", 0,
//                PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());


        // print all work units
//        for (WorkUnit task : workUnits) {
//            logger.info("### Work Unit : {}", task);
//        }

        // set allCount of each work unit
        for (WorkUnit task : workUnits) {
            task.setAllCount(this.allCount);
        }

        List<Message> ruleMessages = new ArrayList<>();
        for (int wid = 0; wid < workUnits.size(); wid += BATCH_SIZE) {
            int begin = wid;
            int end = Math.min(wid + BATCH_SIZE, workUnits.size());
            ArrayList<WorkUnit> subTasks = new ArrayList<>();
            for (int z = begin; z < end; z++) {
                subTasks.add(workUnits.get(z));
            }
            //List<Message> ruleMessages = sc.parallelize(workUnits, workUnits.size()).map(task -> {
            List<Message> ruleMessagesSub = sc.parallelize(subTasks, subTasks.size()).map(task -> {
                List<Predicate> predicatesList = broadAllPredicate.getValue();
                // share PredicateSet static instance, i.e., providerIndex
                logger.info(">>>>show predicate value:{}", predicatesList.get(0).getOperand1().getColumnLight().getValuesIntPartitions());

//            logger.info("####level:{}, In workers XXXXXX indexProvider size:{}, list:{}", 0,
//                    PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());

                //synchronized (BroadcastObj.class) {
                    PredicateSetAssist assist = bcpsAssist.getValue();
                    PredicateSet.indexProvider = assist.getIndexProvider();
                    PredicateSet.bf = assist.getBf();
                    String taskid = assist.getTaskId();
                //}

//            logger.info("####level:{}, In workers indexProvider size:{}, list:{}", 0,
//                    PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());
//
//                // print
//                logger.info("***** Work Unit, {}", task);

                if (task.getCurrentIndexs().size() == 0) {
                    return null;
                }
//                if (task.getCurrrent().size() == 0) {
//                    return null;
//                }
                // retrieve data
//                task.retrieveTransferData();
                task.setTransferData(predicatesList);

                BroadcastObj bobj = scInputLight.getValue();
                // validate candidate rules in workers
                MultiTuplesRuleMining multiTuplesRuleMining = new MultiTuplesRuleMining(bobj.getMax_num_tuples(),
                        bobj.getInputLight(), bobj.getSupport(), bobj.getConfidence(), bobj.getMaxOneRelationNum(),
                        task.getAllCount(), bobj.getTupleNumberRelations());

                ArrayList<Predicate> current = new ArrayList<>();
                for (Predicate p : task.getCurrrent()) {
                    current.add(p);
                }
                // List<Message> messages = multiTuplesRuleMining.validation(current, task.getRHSs());
                List<Message> messages = multiTuplesRuleMining.validation_new(current, task.getRHSs(), task.getPids());
                return messages;
            }).aggregate(null, new IMessageAggFunction(), new IMessageAggFunction());

            ruleMessages.addAll(ruleMessagesSub);
        }
        return ruleMessages;

    }

    private void prepareAllPredicatesMultiTuples() {
        HashMap<String, ParsedColumnLight<?>> colsMap = new HashMap<>();
        for (Predicate p : this.allPredicates) {
            String k = p.getOperand1().getColumn().toStringData();
            if (! colsMap.containsKey(k)) {
                ParsedColumnLight<?> col = new ParsedColumnLight<>(p.getOperand1().getColumn());
                colsMap.put(k, col);
            }
            k = p.getOperand2().getColumn().toStringData();
            if (! colsMap.containsKey(k)) {
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
        for (int i = 0; i < message.getValidRHSs().size(); i++) {
            Predicate rhs = message.getValidRHSs().get(i);
            double conf = message.getConfidences().get(i);
            long supp = message.getSupports().get(i);
            PredicateSet ps = new PredicateSet(currentX);
            ps.addRHS(rhs);
            DenialConstraint ree = new DenialConstraint(ps);
            // set support and confidence
            ree.setSupport(supp);
            ree.setConfidence(conf);
//            logger.info("Transform rule {} with support {} and confidence {}", ree, supp, conf);
            // compute the interestingness score of each REE rule
            double score = this.computeInterestingness(ree);
            ree.setInterestingnessScore(score);
            rees.add(ree);
        }
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
//            this.invalidX.add(kkps.getBitset());
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
                if (! results.containsKey(temp.getBitset())) {
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
            if (! results.containsKey(kkps)) {
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

                if (! results.containsKey(kkkps)) {
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
            if (! results.containsKey(kkps)) {
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

                if (! results.containsKey(kkkps)) {
                    if (rhs.isConstant()) {
                        // results.put(kkkps, entry.getValue() * maxOneRelationNum * 1.0 / this.allCount);
                        results.put(kkkps, entry.getValue() * maxOneRelationNum * 1.0 / this.maxOneRelationNum);
                    } else {
                        // results.put(kkkps, entry.getValue() * 1.0 / this.allCount);
                        results.put(kkkps, entry.getValue() * 1.0 / this.maxOneRelationNum);
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
        return rees;
    }

    /** deal with skewness
        split heavy work units
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
            if (! recordComs.containsKey(comKey)) {
                // enumerate all combinations
                ArrayList<int[]> combinations = new ArrayList<>();
                int[] oneCom = new int[tupleNum + 1];
                enumCombinations(oneCom, combinations, _pids, 0, tupleNum + 1);
                recordComs.put(comKey, combinations);
            }
            // split work units
            ArrayList<int[]> allCombinations = recordComs.get(comKey);
            for (int[] combination : allCombinations) {
                WorkUnit task_new = new WorkUnit(task, combination);
//                task_new.setTransferData();
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
            if (! recordComs.containsKey(comKey)) {
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
//                task_new.setTransferData();
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
        // integration
        for (Map.Entry<PredicateSet, ArrayList<Message>> entry : messageGroups.entrySet()) {
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


    /******************************************************************************************
     * local test
     */

    public void levelwiseRuleDiscoveryLocal() {

        this.prepareAllPredicatesMultiTuples();

        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long)inputLight.getLineCounts()[i]);
        }

        // 1. initialize the 1st level combinations
        Lattice lattice = new Lattice(this.maxTupleNum);
        // only for test application-driven methods
        ArrayList<Predicate> applicationRHSs = new ArrayList<>();
        for (Predicate p : this.allPredicates) {
            // if p is a constant, only consider t_0
            if (p.isConstant() && p.getIndex1() == 1) {
                continue;
            }
            applicationRHSs.add(p);
        }
//        int ccount = 0;
//        for(int i = 0; i < this.allPredicates.size(); i=i+2) {
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

        int level = 0;
        while (lattice != null && level <= MAX_CURRENT_PREDICTES) {
            // collect a set of work units
//            logger.info("Search level : {} with lattice {}", level, lattice.printCurrents());
            logger.info("Search level : {} with lattice", level);
            level ++;
            ArrayList<WorkUnit> workUnits_ = lattice.generateWorkUnits();
            // solve skewness
//            workUnits = this.solveSkewness(workUnits);
            for (WorkUnit task : workUnits_) {
                task.setTransferData();
            }

            // batch mode
            ArrayList<WorkUnit> workUnits_init = workUnits_;
            HashMap<PredicateSet, Double> currentSupports = new HashMap<>();
            String option = "original";
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
                    messages = this.runLocal(workUnits);
                    logger.info("Integrate messages ...");
                    messages = this.integrateMessages(messages);
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
                    workUnits_init = this.pruneXWorkUnits(interestingness, this.getKthInterestingnessScore(), currentSupports, workUnits_init, remainStartSc);
                } else {
                    ArrayList<WorkUnit> remainTasks = new ArrayList<>();
                    for (int tid = remainStartSc; tid < workUnits_init.size(); tid++) {
                        remainTasks.add(workUnits_init.get(tid));
                    }
                    workUnits_init = remainTasks;
                }

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
//            String option = "original";
            if (option.equals("original")) {
//            this.pruneLattice(invalidX, validXRHSs);
                lattice.pruneXInterestingnessUB(interestingness, this.getKthInterestingnessScore(), currentSupports, "");
            } else if (option.equals("anytime")) {
//            this.pruneLattice(invalidX, validXRHSs);
                lattice.pruneXInterestingnessUB(interestingness, this.getKthInterestingnessScore(), currentSupports, "");
            }


            lattice.setAllLatticeVertexBits(lattice.getLatticeLevel());

            Lattice nextLattice = lattice.generateNextLatticeLevel(this.allPredicates, null, this.invalidX, this.invalidXRHSs, this.validXRHSs,
                    interestingness, this.getKthInterestingnessScore(), currentSupports, predicateProviderIndex, option, null,
                    0, 0, 0, false, "", "", 0, 0, 0, 0, 0, 0, "", 0, null, null);

            // pruning
            nextLattice.removeInvalidLatticeAndRHSs(lattice);
            if (nextLattice == null || nextLattice.size() == 0) {
                break;
            }
            lattice = nextLattice;
        }
    }

    private WorkUnit testWorkUnit1() {
        WorkUnit workUnit = new WorkUnit();
        HashSet<String> X_dict = new HashSet<>();
        for (Predicate p : this.allPredicates) {
            if (p.toString().trim().equals("airports.t0.iso_region == airports.t1.iso_region")) {
                workUnit.addCurrent(predicateProviderIndex.getPredicate(p, 0, 2));
            }
            if (p.toString().trim().equals("airports.t0.iata_code == airports.t1.iata_code")) {
                workUnit.addCurrent(predicateProviderIndex.getPredicate(p, 1, 2));
            }
            if (p.toString().trim().equals("airports.t0.municipality == airports.t1.municipality")) {
                workUnit.addCurrent(predicateProviderIndex.getPredicate(p, 0, 1));
            }
            if (p.toString().trim().equals("airports.t0.longitude_deg == airports.t1.longitude_deg")) {
                workUnit.addCurrent(predicateProviderIndex.getPredicate(p, 0, 2));
            }
        }
        X_dict.add("airports.t0.iso_region == airports.t2.iso_region");
        X_dict.add("airports.t1.iata_code == airports.t2.iata_code");
        X_dict.add("airports.t0.municipality == airports.t1.municipality");
        X_dict.add("airports.t0.longitude_deg == airports.t2.longitude_deg");
        HashSet<String> Y_dict = new HashSet<>();
        Y_dict.add("airports.t0.iso_region == airports.t1.iso_region");
        for (Predicate p : this.allPredicates) {
//            if (X_dict.contains(p.toString().trim())) {
//                workUnit.addCurrent(p);
//            }
            if (Y_dict.contains(p.toString().trim())) {
                workUnit.addRHS(p);
            }
        }
        return workUnit;
    }

    public void testRunLocal()  {

        this.prepareAllPredicatesMultiTuples();
        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long)inputLight.getLineCounts()[i]);
        }

        List<Message> ruleMessages = new ArrayList<>();

        ArrayList<WorkUnit> workUnits = new ArrayList<>();
        workUnits.add(testWorkUnit1());

        for (WorkUnit task : workUnits) {
            // validate candidate rules in workers
//             logger.info("Work Unit : {}".format(task.toString()));
            task.setTransferData();
            if (task.getCurrrent().size() == 0) {
                continue;
            }
//            if (test3(task)) {
             if (true) {
                // test 1: user_info.t0.city == user_info.t1.city  user_info.t0.sn == user_info.t1.sn  user_info.t0.gender == user_info.t1.gender ]
                //                         -> {  user_info.t0.name == user_info.t1.name }

                logger.info("Work Unit : {}".format(task.toString()));

                MultiTuplesRuleMining multiTuplesRuleMining = new MultiTuplesRuleMining(this.maxTupleNum,
                        this.inputLight, this.support, this.confidence, maxOneRelationNum, allCount, tupleNumberRelations);

                ArrayList<Predicate> current = new ArrayList<>();
                for (Predicate p : task.getCurrrent()) {
                    current.add(p);
                }
                List<Message> messages = multiTuplesRuleMining.validation_new(current, task.getRHSs(), task.getPids());
                logger.info("Supp : {}, Conf : {}".format(String.valueOf(messages.get(0).getCurrentSupp()), messages.get(0).getConfidences()));
                ruleMessages.addAll(messages);

            }
        };


    }

    private boolean test3(WorkUnit task) {
        PredicateSet X = task.getCurrrent();
        PredicateSet Y = task.getRHSs();
        HashSet<String> X_dict = new HashSet<>();
        X_dict.add("airport.t0.iso_region == airport.t2.iso_region");
        X_dict.add("airport.t1.iata_code == airport.t2.iata_code");
        X_dict.add("airport.t0.municipality == airport.t2.municipality");
        X_dict.add("airport.t0.longitude_deg == airport.t2.longitude_deg");
        HashSet<String> Y_dict = new HashSet<>();
        Y_dict.add("airport.t0.iso_region == airport.t1.iso_region");

        if (X.size() != 4) {
            return false;
        }

        for (Predicate p : X) {
            if (! X_dict.contains(p.toString().trim())) {
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
            if (! X_dict.contains(p.toString().trim())) {
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
            if (! X_dict.contains(p.toString().trim())) {
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

    public List<Message> runLocal(ArrayList<WorkUnit> workUnits)  {

        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long)inputLight.getLineCounts()[i]);
        }

        List<Message> ruleMessages = new ArrayList<>();

         for (WorkUnit task : workUnits) {
            // validate candidate rules in workers
//             logger.info("Work Unit : {}".format(task.toString()));

             if (task.getCurrrent().size() == 0) {
                 continue;
             }
             if (test3(task)) {
//             if (true) {
                 // test 1: user_info.t0.city == user_info.t1.city  user_info.t0.sn == user_info.t1.sn  user_info.t0.gender == user_info.t1.gender ]
                 //                         -> {  user_info.t0.name == user_info.t1.name }

                 logger.info("Work Unit : {}".format(task.toString()));

                 MultiTuplesRuleMining multiTuplesRuleMining = new MultiTuplesRuleMining(this.maxTupleNum,
                         this.inputLight, this.support, this.confidence, maxOneRelationNum, allCount, tupleNumberRelations);

                 ArrayList<Predicate> current = new ArrayList<>();
                 for (Predicate p : task.getCurrrent()) {
                     current.add(p);
                 }
                 List<Message> messages = multiTuplesRuleMining.validation_new(current, task.getRHSs(), task.getPids());
                 ruleMessages.addAll(messages);

             }
        };

        return ruleMessages;

    }

    private static Logger logger = LoggerFactory.getLogger(ParallelRuleDiscovery.class);

    /**************************************************************************************************
     * local test
     */
}
