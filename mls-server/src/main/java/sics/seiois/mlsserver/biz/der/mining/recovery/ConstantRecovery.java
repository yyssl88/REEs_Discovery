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


    public DenialConstraintSet getREEsResults() {
        return this.REEsResults;
    }

    public ConstantRecovery(DenialConstraintSet rees, ArrayList<Predicate> allRealPredicates,
                            int maxTupleNum, InputLight inputLight, long support, float confidence,
                            long maxOneRelationNum, long allCount) {
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
                continue;
            }
            String key = ps.toString() + " -> " + ree.getRHS().toString();
            if (! dups.contains(key)) {
                REETemplate reeTemplate = new REETemplate(cTemplates, nonCPredicates, ree.getRHS());
                this.reeTemplates.add(reeTemplate);
                dups.add(key);
            }
        }
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
                messages = this.run(workUnits, taskId, spark, sparkContextConfig, tupleNumberRelations);
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
                messages = this.runLocal(workUnits, tupleNumberRelations);
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

    private static Logger logger = LoggerFactory.getLogger(ConstantRecovery.class);

}
