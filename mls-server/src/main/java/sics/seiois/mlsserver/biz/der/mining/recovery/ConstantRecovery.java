package sics.seiois.mlsserver.biz.der.mining.recovery;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraint;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraintSet;
import sics.seiois.mlsserver.biz.der.metanome.input.InputLight;
import sics.seiois.mlsserver.biz.der.metanome.input.ParsedColumn;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.Message;
import sics.seiois.mlsserver.biz.der.mining.MultiTuplesRuleMining;
import sics.seiois.mlsserver.biz.der.mining.MultiTuplesRuleMiningOpt;
import sics.seiois.mlsserver.biz.der.mining.utils.*;
import sics.seiois.mlsserver.model.SparkContextConfig;
import sics.seiois.mlsserver.service.impl.PredicateSetAssist;

import java.util.*;

public class ConstantRecovery {
    private ArrayList<REETemplate> reeTemplates;
    private ArrayList<Predicate> allPredicates;
    private ArrayList<Predicate> realConstantPredicates;
    private int maxTupleNum;
    private InputLight inputLight;
    private long support;
    private float confidence;
    private long maxOneRelationNum;
    private long allCount;
    private DenialConstraintSet REEsResults;
    // generate all predicate set including different tuple ID pair
    private static final PredicateProviderIndex predicateProviderIndex = PredicateProviderIndex.getInstance();

    public static int MIN_NUM_WORK_UNITS = 200;
    public static int BATCH_SIZE = 200;

    private HashSet<IBitSet> invalidX;
    private HashMap<IBitSet, ArrayList<Predicate>> invalidXRHSs;

    private HashMap<IBitSet, ArrayList<Predicate>> validXRHSs;

    private Map<PredicateSet, List<Predicate>> validConstantRule = new HashMap<>();

    private int if_cluster_workunits;


    public DenialConstraintSet getREEsResults() {
        return this.REEsResults;
    }

    public ConstantRecovery(DenialConstraintSet rees, ArrayList<Predicate> allRealPredicates,
                            int maxTupleNum, InputLight inputLight, long support, float confidence,
                            long maxOneRelationNum, long allCount, int if_cluster_workunits) {
        this.REEsResults = new DenialConstraintSet();
        this.reeTemplates = new ArrayList<>();
        this.transformREETemplates(rees, this.REEsResults);
        this.allPredicates = allRealPredicates;
        this.setPRedicatesSupport();
        this.maxTupleNum = maxTupleNum;
        this.inputLight = inputLight;
        this.support = support;
        this.confidence = confidence;
        this.maxOneRelationNum = maxOneRelationNum;
        this.allCount = allCount;
        this.if_cluster_workunits = if_cluster_workunits;

        // invalid predicate combinations
        this.invalidX = new HashSet<>();
        this.invalidXRHSs = new HashMap<>();
        // valid X and RHSs
        this.validXRHSs = new HashMap<>();

        this.realConstantPredicates = new ArrayList<>();

        // update predicates
        HashMap<String, ParsedColumnLight<?>> colsMap = new HashMap<>();
        this.prepareAllPredicatesMultiTuples(colsMap);
        // update REETemplates
        for (REETemplate reeTemplate : this.reeTemplates) {
            reeTemplate.update(predicateProviderIndex, this.allPredicates, colsMap);
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

    private void setPRedicatesSupport() {
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

    }

    private void prepareAllPredicatesMultiTuples(HashMap<String, ParsedColumnLight<?>> colsMap) {
//        HashMap<String, ParsedColumnLight<?>> colsMap = new HashMap<>();
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
        Set<Predicate> allRealConstantPredicates = new HashSet<>();
        // insert parsedColumnLight for each predicate
        PredicateSet ps = new PredicateSet();
        for (Predicate p : this.allPredicates) {
            // set columnLight
            Predicate p_newN = predicateProviderIndex.getPredicate(p, p.getIndex1(), p.getIndex2());
            String k = p_newN.getOperand1().getColumn().toStringData();
            p_newN.getOperand1().setColumnLight(colsMap.get(k));
            k = p_newN.getOperand2().getColumn().toStringData();
            p_newN.getOperand2().setColumnLight(colsMap.get(k));
            ps.add(p_newN);
            // add the constant template
            if (p.isConstant()) {
                Predicate p_newT = predicateProviderIndex.getPredicate(p, p.getIndex1(), p.getIndex2(),
                        Predicate.CONSTANT_TEMPLATE_VALUE, Predicate.CONSTANT_TEMPLATE_INT, "", true);

                k = p_newT.getOperand1().getColumn().toStringData();
                p_newT.getOperand1().setColumnLight(colsMap.get(k));
                k = p_newT.getOperand2().getColumn().toStringData();
                p_newT.getOperand2().setColumnLight(colsMap.get(k));
                ps.add(p_newT);

            }
            for (int t1 = 0; t1 < maxTupleNum; t1++) {
                if (p.isConstant()) {
                    Predicate p_new = predicateProviderIndex.getPredicate(p, t1, t1);
                    k = p_new.getOperand1().getColumn().toStringData();
                    p_new.getOperand1().setColumnLight(colsMap.get(k));
                    k = p_new.getOperand2().getColumn().toStringData();
                    p_new.getOperand2().setColumnLight(colsMap.get(k));
                    ps.add(p_new);
                    // this.realConstantPredicates.add(p_new); // duplication
                    allRealConstantPredicates.add(p_new);

                    // add the constant template
                    Predicate p_newT = predicateProviderIndex.getPredicate(p, p.getIndex1(), p.getIndex2(),
                            Predicate.CONSTANT_TEMPLATE_VALUE, Predicate.CONSTANT_TEMPLATE_INT, "", true);

                    k = p_newT.getOperand1().getColumn().toStringData();
                    p_newT.getOperand1().setColumnLight(colsMap.get(k));
                    k = p_newT.getOperand2().getColumn().toStringData();
                    p_newT.getOperand2().setColumnLight(colsMap.get(k));
                    ps.add(p_newT);

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

        for (Predicate p : allRealConstantPredicates) {
            this.realConstantPredicates.add(p);
        }

    }


    private void transformREETemplates(DenialConstraintSet rees, DenialConstraintSet reesFinal) {
        HashSet<String> dups = new HashSet<>();
        int nonConstantRuleSize = 0;
        for (DenialConstraint ree : rees) {
            ArrayList<Predicate> cTemplates = new ArrayList<>();
            ArrayList<Predicate> nonCPredicates = new ArrayList<>();
            PredicateSet ps = new PredicateSet();
            boolean ifContainConstant = false;
            for (Predicate p : ree.getPredicateSet()) {
                if (p.isConstant()) {
                    ifContainConstant = true;
                    Predicate cpTemplate = predicateProviderIndex.getPredicate(p, p.getIndex1(), p.getIndex2(),
                            Predicate.CONSTANT_TEMPLATE_VALUE, Predicate.CONSTANT_TEMPLATE_INT, "", true);
                    // p.changeToConstantTemplate();
                    ps.add(cpTemplate);
                    cTemplates.add(cpTemplate);
                } else {
                    ps.add(p);
                    nonCPredicates.add(p);
                }
            }
            if (ifContainConstant == false) {
                reesFinal.add(ree);
                nonConstantRuleSize++;
                continue;
            }
            reesFinal.add(ree); // keep all rules from sample data.
            String key = ps.toString() + " -> " + ree.getRHS().toString();
            if (! dups.contains(key)) {
                REETemplate reeTemplate = new REETemplate(cTemplates, nonCPredicates, ree.getRHS());
                this.reeTemplates.add(reeTemplate);
                dups.add(key);
            }
        }
        logger.info("#### nonConstant REEs num: {}", nonConstantRuleSize);
    }


    private ArrayList<WorkUnit> generateWorkUnits() {
        ArrayList<WorkUnit> workUnits = new ArrayList<>();
        for (REETemplate reeTemplate : this.reeTemplates) {
            ArrayList<WorkUnit> workunits = reeTemplate.generateWorkUnits(this.realConstantPredicates);
            if (workunits == null) {
                continue;
            }
            workUnits.addAll(workunits);
        }
        return workUnits;
    }

    /** deal with skewness
     split heavy work units
     */

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
                task_new.setTransferData();
                tasks_new.add(task_new);
            }
        }

        return wid;
    }


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
            ree.removeRHS();
//            logger.info("Transform rule {} with support {} and confidence {}", ree, supp, conf);
            // compute the interestingness score of each REE rule
            rees.add(ree);
        }
        // remove RHSs in X
        rees.removeRHS();
        return rees;
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

    private ArrayList<WorkUnit> pruneXWorkUnits(HashMap<PredicateSet, Double> supp_ratios,
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
            }
            prunedTasks.add(task);
        }
        return prunedTasks;
    }


    public void recovery(String taskId, SparkSession spark, SparkContextConfig sparkContextConfig) {
//        this.prepareAllPredicatesMultiTuples();

        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long)inputLight.getLineCounts()[i]);
        }

        ArrayList<WorkUnit> workUnits_init = this.generateWorkUnits();
        // merge work units by X
        workUnits_init = this.mergeWorkUnits(workUnits_init);
        for (WorkUnit task : workUnits_init) {
            task.setTransferData();
        }
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
                messages = this.run_new(workUnits, taskId, spark, sparkContextConfig, tupleNumberRelations);
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
                            this.REEsResults.add(ree);
                            validConstantRule.putIfAbsent(message.getCurrentSet(), new ArrayList<>());
                            validConstantRule.get(message.getCurrentSet()).add(ree.getRHS());
                            logger.info("Valid REE values ###### are : {}", ree.toString());
                        }
                    }
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
            workUnits_init = this.pruneXWorkUnits(currentSupports, workUnits_init, remainStartSc);
        }
    }

    public List<Message> run(ArrayList<WorkUnit> workUnits, String taskId, SparkSession spark,
                             SparkContextConfig sparkContextConfig, HashMap<String, Long> tupleNumberRelations)  {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        BroadcastObj broadcastObj = new BroadcastObj(this.maxTupleNum, this.inputLight, this.support, this.confidence,
                this.maxOneRelationNum, tupleNumberRelations, this.realConstantPredicates);
        // broadcast data
        // ... left for future

        Broadcast<BroadcastObj> scInputLight = sc.broadcast(broadcastObj);

        // broadcast provideIndex, i.e., all predicates
        PredicateSetAssist psAssist = new PredicateSetAssist();
        psAssist.setIndexProvider(PredicateSet.indexProvider);//在这里设置，这个值肯定是确定了的
        psAssist.setBf(PredicateSet.bf);
        psAssist.setTaskId(taskId);
        Broadcast<PredicateSetAssist> bcpsAssist = sc.broadcast(psAssist);

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

                // share PredicateSet static instance, i.e., providerIndex

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

                if (task.getCurrrent().size() == 0) {
                    return null;
                }

                // retrieve data
                task.retrieveTransferData();

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

//                // validate candidate rules in workers
//                MultiTuplesRuleMiningConstantRecovery multiTuplesRuleMining = new MultiTuplesRuleMiningConstantRecovery(bobj.getMax_num_tuples(),
//                        bobj.getInputLight(), bobj.getSupport(), bobj.getConfidence(), bobj.getMaxOneRelationNum(),
//                        task.getAllCount(), bobj.getTupleNumberRelations());
//
//                ArrayList<Predicate> constantTemplates = new ArrayList<>();
//                ArrayList<Predicate> nonConstantPredicates = new ArrayList<>();
//                for (Predicate p : task.getCurrrent()) {
//                    if (p.isConstant()) {
//                        constantTemplates.add(p);
//                    } else {
//                        nonConstantPredicates.add(p);
//                    }
//                }
//                Predicate rhs = null;
//                for (Predicate p : task.getRHSs()) {
//                    rhs = p;
//                }
//
//                REETemplate reeTemplate = new REETemplate(constantTemplates, nonConstantPredicates, rhs);
//                reeTemplate.fillRealConstantPredicates(bobj.getAllRealCosntantPredicates());
//                ArrayList<Predicate> cps = new ArrayList<>();
//                List<Message> messages = multiTuplesRuleMining.validation(reeTemplate, task.getPids(), cps);
//                return messages;
            }).aggregate(null, new IMessageAggFunction(), new IMessageAggFunction());

            ruleMessages.addAll(ruleMessagesSub);
        }
        return ruleMessages;

    }

    public List<Message> run_new(ArrayList<WorkUnit> workUnits, String taskId,
                             SparkSession spark, SparkContextConfig sparkContextConfig, HashMap<String, Long> tupleNumberRelations) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        BroadcastObj broadcastObj = new BroadcastObj(this.maxTupleNum, this.inputLight, this.support, this.confidence,
                this.maxOneRelationNum, tupleNumberRelations);
        // broadcast data
        // ... left for future

        broadcastObj.setValidConstantRule(validConstantRule);
        Broadcast<BroadcastObj> scInputLight = sc.broadcast(broadcastObj);

        PredicateSetAssist psAssist = new PredicateSetAssist();
        psAssist.setIndexProvider(PredicateSet.indexProvider);//在这里设置，这个值肯定是确定了的
        psAssist.setBf(PredicateSet.bf);
        psAssist.setTaskId(taskId);
        Broadcast<PredicateSetAssist> bcpsAssist = sc.broadcast(psAssist);

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

    public void recoveryLocal() {
//        this.prepareAllPredicatesMultiTuples();

        HashMap<String, Long> tupleNumberRelations = new HashMap<>();
        for (int i = 0; i < inputLight.getNames().size(); i++) {
            tupleNumberRelations.put(inputLight.getNames().get(i), (long)inputLight.getLineCounts()[i]);
        }

        ArrayList<WorkUnit> workUnits_init = this.generateWorkUnits();
        // merge work units by X
        workUnits_init = this.mergeWorkUnits(workUnits_init);
        for (WorkUnit task : workUnits_init) {
            task.setTransferData();
        }
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
                messages = this.runLocal_new(workUnits);
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
                            this.REEsResults.add(ree);
                            logger.info("Valid REE values ###### are : {}", ree.toString());
                        }
                    }
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
            workUnits_init = this.pruneXWorkUnits(currentSupports, workUnits_init, remainStartSc);
        }
    }

    public List<Message> runLocal(ArrayList<WorkUnit> workUnits, HashMap<String, Long> tupleNumberRelations)  {

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
            for (WorkUnit task : subTasks) {

                // share PredicateSet static instance, i.e., providerIndex

//            logger.info("####level:{}, In workers XXXXXX indexProvider size:{}, list:{}", 0,
//                    PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());


//            logger.info("####level:{}, In workers indexProvider size:{}, list:{}", 0,
//                    PredicateSet.indexProvider.size(), PredicateSet.indexProvider.toString());
//
//                // print
                logger.info("***** Work Unit, {}", task);

                if (task.getCurrrent().size() == 0) {
                    return null;
                }

                // retrieve data
                task.retrieveTransferData();

                MultiTuplesRuleMining multiTuplesRuleMining = new MultiTuplesRuleMining(this.maxTupleNum,
                        this.inputLight, this.support, this.confidence, maxOneRelationNum, allCount, tupleNumberRelations);

                ArrayList<Predicate> current = new ArrayList<>();
                for (Predicate p : task.getCurrrent()) {
                    current.add(p);
                }
                List<Message> messages = multiTuplesRuleMining.validation_new(current, task.getRHSs(), task.getPids());
                ruleMessages.addAll(messages);

//                // validate candidate rules in workers
//                MultiTuplesRuleMiningConstantRecovery multiTuplesRuleMining = new MultiTuplesRuleMiningConstantRecovery(this.maxTupleNum,
//                        this.inputLight, this.support, this.confidence, this.maxOneRelationNum,
//                        task.getAllCount(), tupleNumberRelations);
//
//                ArrayList<Predicate> constantTemplates = new ArrayList<>();
//                ArrayList<Predicate> nonConstantPredicates = new ArrayList<>();
//                for (Predicate p : task.getCurrrent()) {
//                    if (p.isConstant()) {
//                        constantTemplates.add(p);
//                    } else {
//                        nonConstantPredicates.add(p);
//                    }
//                }
//                Predicate rhs = null;
//                for (Predicate p : task.getRHSs()) {
//                    rhs = p;
//                }
//
//                REETemplate reeTemplate = new REETemplate(constantTemplates, nonConstantPredicates, rhs);
//                reeTemplate.fillRealConstantPredicates(this.realConstantPredicates);
//                ArrayList<Predicate> cps = new ArrayList<>();
//                List<Message> messages = multiTuplesRuleMining.validation(reeTemplate, task.getPids(), cps);
//                ruleMessages.addAll(messages);
                // return messages;
            } // ).aggregate(null, new IMessageAggFunction(), new IMessageAggFunction());

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

    private static Logger logger = LoggerFactory.getLogger(ConstantRecovery.class);

}
