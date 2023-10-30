package sics.seiois.mlsserver.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.client.model.mls.ColumnInfo;
import com.sics.seiois.client.model.mls.JoinInfo;
import com.sics.seiois.client.model.mls.JoinInfos;
import com.sics.seiois.client.model.mls.ModelPredicate;
import com.sics.seiois.client.model.mls.TableInfo;
import com.sics.seiois.client.model.mls.TableInfos;

import de.metanome.algorithm_integration.Operator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import org.tensorflow.Session;
import sics.seiois.mlsserver.biz.ConstantPredicateGenerateMain;
import sics.seiois.mlsserver.biz.der.metanome.REEFinderEvidSet;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraint;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraintSet;
import sics.seiois.mlsserver.biz.der.metanome.input.InputLight;
import sics.seiois.mlsserver.biz.der.metanome.pojo.RuleResult;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.ParallelRuleDiscoverySampling;
import sics.seiois.mlsserver.biz.der.mining.model.DQNMLP;
import sics.seiois.mlsserver.biz.der.mining.model.MLPFilterClassifier;
import sics.seiois.mlsserver.biz.der.mining.recovery.ConstantRecovery;
import sics.seiois.mlsserver.enums.RuleFinderTypeEnum;
import sics.seiois.mlsserver.model.*;
import sics.seiois.mlsserver.utils.RuntimeParamUtil;
import sics.seiois.mlsserver.utils.helper.ErRuleFinderHelper;

public class EvidenceGenerateMain {

    private static final Logger logger = LoggerFactory.getLogger(EvidenceGenerateMain.class);
    public final static String ORC_TYPE_SUFFIX = "orc";
    public final static String CSV_TYPE_SUFFIX = "csv";
    public static final String MLS_TMP_HOME = "/tmp/rulefind/";
    private static final String CONFIG_PATH = "predict.conf";
    private static final StringBuffer timeInfo = new StringBuffer();
    private static final int NORECORD = 0;


    public static void main(String[] args) throws Exception {
        // 本地文件 /tmp/rulefind/{taskId}/{taskId}_req.json
        String requestJsonFileName = args[0];
        logger.info("#### rulefinder request param file name:{}", requestJsonFileName);

        SparkSession spark = SparkSession.builder().appName("evidenceset").getOrCreate();
        File tableJsonFile = new File(requestJsonFileName);
        String reqJson = FileUtils.readFileToString(tableJsonFile, "utf-8");
        logger.info("#### reqJson:{}", reqJson);

        RuleDiscoverExecuteRequest request = JSONObject.parseObject(reqJson).toJavaObject(RuleDiscoverExecuteRequest.class);
        String taskId = request.getTaskId();
        String runtimeParam = request.getOtherParam();
        TableInfos tableInfos = request.getTableInfos();

        Set<String> usefulCol = new HashSet<>();
        for(TableInfo tableInfo : tableInfos.getTableInfoList()) {
            for (ColumnInfo columnInfo : tableInfo.getColumnList()) {
                if(!columnInfo.getSkip()) {
                    usefulCol.add(columnInfo.getColumnName());
                }
            }
        }
        if(usefulCol.size() <= NORECORD) {
            spark.close();
            return;
        }
        spark.conf().set("taskId", taskId);
        spark.conf().set("runtimeParam", runtimeParam);

        logger.info(">>>>show run time params: {}", runtimeParam);
        PredicateConfig.getInstance(CONFIG_PATH).setRuntimeParam(runtimeParam);

        FileSystem hdfs = FileSystem.get(new Configuration());
        //对该taskId的临时目录进行清理
        if (hdfs.exists(new Path(MLS_TMP_HOME + taskId))) {
            hdfs.delete(new Path(MLS_TMP_HOME + taskId), true);
        }

        //根据配置文件是否需要走分布式方案
        PredicateConfig config = PredicateConfig.getInstance(CONFIG_PATH);
        JoinInfos joinInfos = null;

        joinInfos = DataDealerOnSpark.getJoinTablesOnly(tableInfos, taskId);
        generateESAndRuleDig(request, joinInfos, spark, config);

        spark.close();
    }

    /**
     * 生成ES并且进行分布式规则挖掘
     * @throws Exception
     */
    private static void generateESAndRuleDig(RuleDiscoverExecuteRequest request, JoinInfos joinInfos,
                                             SparkSession spark,
                                             PredicateConfig config) throws Exception {

        logger.info("#### generateESAndRuleDig start.joinInfos.size={}", joinInfos.getJoinInfoList().size());
        FileSystem hdfs = FileSystem.get(new Configuration());
        String taskId = request.getTaskId();
        TableInfos tableInfos = request.getTableInfos();
        List<ModelPredicate> modelPredicateList = request.getModelPredicats();

        //join表转成map
        Map<String, List<JoinInfo>> tblNum2List = new HashMap<>();
        for (JoinInfo joinTable : joinInfos.getJoinInfoList()) {
            int num = joinTable.getTableNumber();
            tblNum2List.computeIfAbsent(String.valueOf(num), k -> new ArrayList<>()).add(joinTable);
        }

        //开始清空规则输出目录，对上次执行输出的结果清空
        Path outputPath = new Path(request.getResultStorePath());
        hdfs.delete(outputPath, true);

        //第一步1-单表数据准备。tableInfos 里面如果是orc格式的表转成csv格式的表，并且在spark环境创建了虚拟表
        prepareData(taskId, tableInfos, hdfs, spark);

        /************* CR ER 规则发现主入口***************/
        List<RuleResult> finalRules = new ArrayList<>();
        if(RuleFinderTypeEnum.ER_ONLY.getValue().equals(request.getType())) {
            finalRules = generateErRule(request, joinInfos, spark, hdfs, config);
        } else if(RuleFinderTypeEnum.CR_ONLY.getValue().equals(request.getType())) {
            finalRules = generateCrRule(request, joinInfos, spark, hdfs, config);
        } else {
            //如果ER规则发现参数不为空，进行ER规则发现。ER有异常也继续往下走不影响主流程
            if(request.getErRuleConfig() != null) {
                try {
                    finalRules = generateErRule(request, joinInfos, spark, hdfs, config);
                } catch (Exception e) {
                    logger.error("#### erRuleFinder exception", e);
                }
            }
            //进行CR规则发现
            List<RuleResult> crRules = generateCrRule(request, joinInfos, spark, hdfs, config);
            finalRules.addAll(crRules);
        }

        logger.info("####[规则发现完成,数量:{}, 保存到HDFS开始]", finalRules.size());
        writeRuleToHDFS(hdfs, finalRules, PredicateConfig.MLS_TMP_HOME + taskId + "/allrule.txt");
//        writeRuleToHDFS(request, hdfs, finalRules);
        logger.info("####[规则发现完成,数量:{}, 保存到HDFS结束]", finalRules.size());
    }

    private static List<RuleResult> generateErRule(RuleDiscoverExecuteRequest request, JoinInfos joinInfos, SparkSession spark, FileSystem hdfs,
                                                   PredicateConfig config) throws Exception {
        logger.info("#### erRuleFinder start!tableInfos={}", request.getTableInfos().toString());
        String eidName = ErRuleFinderHelper.EID_NAME_PREFIX + request.getErRuleConfig().getEidName();
        request.getErRuleConfig().setEidName(eidName);

        //根据标注数据获取新的tableInfos
        TableInfos markedTableInfos = ErRuleFinderHelper.getMarkedTableInfos(spark, hdfs, request);
        logger.info("#### erRuleFinder gen tableInfos success! new tableInfos={}", markedTableInfos.toString());

        /***************ER规则发现结束*************************/
        //进行规则发现
        return generateRule(request, markedTableInfos, joinInfos, hdfs, spark, config, true, eidName);
    }

    private static List<RuleResult> generateCrRule(RuleDiscoverExecuteRequest request, JoinInfos joinInfos, SparkSession spark, FileSystem hdfs,
                                                   PredicateConfig config) throws Exception {
        logger.info("#### crRuleFinder start!tableInfos={}", request.getTableInfos().toString());
        TableInfos tableInfos = request.getTableInfos();

        return generateRule(request, tableInfos, joinInfos, hdfs, spark, config, false, null);
    }

    private static void prepareData(String taskId, TableInfos tableInfos, FileSystem hdfs, SparkSession spark) throws IOException {
        boolean deleteTmpData = false;
        for (TableInfo tableInfo : tableInfos.getTableInfoList()) {
            String tablePath = tableInfo.getTableDataPath();
            String newTablePath = PredicateConfig.MLS_TMP_HOME + taskId + "/" + tableInfo.getTableName() + "/" + tableInfo.getTableName() + ".csv";

            if (!tablePath.endsWith(".csv")) {
                spark.read().format("orc").load(tablePath).write().format("csv").option("header", "true").save(newTablePath);
                tableInfo.setTableDataPath(newTablePath);
                deleteTmpData = false;
            }
        }
        for (int i = 0; i < tableInfos.getTableInfoList().size(); i++) {
            String tablename = tableInfos.getTableInfoList().get(i).getTableName();
            String tableLocation = tableInfos.getTableInfoList().get(i).getTableDataPath();
            if (tableLocation.endsWith(CSV_TYPE_SUFFIX)) {
                //创建表
                logger.info("****a:");
                spark.read().format("com.databricks.spark.csv").option("header", "true").load(tableLocation).createOrReplaceTempView(tablename);
            } else {
                logger.info("****b:");
                spark.read().format("orc").load(tableLocation).createOrReplaceTempView(tablename);
            }
            logger.info("****表格" + tablename);
        }

        if (deleteTmpData) {
            for (TableInfo tableInfo : tableInfos.getTableInfoList()) {
                hdfs.delete(new Path(tableInfo.getTableDataPath()), true);
            }
        }
    }

    private static List<RuleResult> generateRule(RuleDiscoverExecuteRequest request, TableInfos tableInfos, JoinInfos joinInfos, FileSystem hdfs,
                                     SparkSession spark, PredicateConfig config, boolean erRuleFinderFlag, String eidName) throws Exception {
        String taskId = request.getTaskId();
        long startTime = System.currentTimeMillis();
        List<String> constantPredicate = generateConstant(taskId, tableInfos, spark, config, erRuleFinderFlag, eidName);
        logger.info("#### constantPredicate size: {}", constantPredicate.size());
        logger.info("#### constantPredicate: {}", constantPredicate);

        // randomly select 5 constant predicates to participate in rule mining
//        List<String> selected_constantPredicate = new ArrayList<>();
//        selectConstantPredicates(constantPredicate, selected_constantPredicate, 5);
//        logger.info("#### selected_constantPredicate size: {}", selected_constantPredicate.size());
//        logger.info("#### selected_constantPredicate: {}", selected_constantPredicate);

        SparkContextConfig sparkContextConfig = new SparkContextConfig(config, request, tableInfos);

        String algOption = RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "algOption");
        logger.info("####predicateConfig:{}", config.toString());
        //(1)索引方式生成ES关键逻辑
        REEFinderEvidSet reeFinderEvidSet = new REEFinderEvidSet();
        // set alg option
        reeFinderEvidSet.setAlgOption(algOption);
        List<RuleResult> allRules = new ArrayList<>();
        spark.conf().set("RuleCount", 0 + "->" + allRules.size());

        reeFinderEvidSet.generateInput(taskId, tableInfos, joinInfos, spark, constantPredicate, config, eidName);

        // write experimental results
        long inputTime = System.currentTimeMillis() - startTime;
        timeInfo.append("input data time: ").append(inputTime).append("\n");

        if(!config.isExecuteOnlyConstantPred()){
            logger.info("####predicate_type!=3，生成多行规则");
            if (algOption.equals("constantRecovery")) {
                allRules.addAll(generateMultiTupleRuleSamplingConstantRecovery(request, reeFinderEvidSet, tableInfos, hdfs, spark, config));
            }
            else {
                allRules.addAll(generateMultiTupleRule(request, reeFinderEvidSet, tableInfos, hdfs, spark, config));
//                allRules.addAll(generateMultiTupleRuleSampling(request, reeFinderEvidSet, tableInfos, hdfs, spark, config));
            }
        }

        String outputResultFile = RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "outputResultFile");

        //打印规则
//        for(RuleResult rule : allRules) {
//            logger.info("####[输出规则]:" + rule.getPrintString());
//        }

        long runningTime = System.currentTimeMillis() - startTime;
        timeInfo.append("total running time: ").append(runningTime).append("\n");

        String outTxtPath = PredicateConfig.MLS_TMP_HOME + taskId + "/rule_all/" + outputResultFile; // "experiment_results";
        FSDataOutputStream outTxt = hdfs.create(new Path(outTxtPath));
        outTxt.writeBytes(timeInfo.toString());
        outTxt.close();

        return allRules;
    }


    private static List<RuleResult> generateMultiTupleRuleSampling(RuleDiscoverExecuteRequest request, REEFinderEvidSet reeFinderEvidSet,
                                                           TableInfos tableInfos, FileSystem hdfs, SparkSession spark,
                                                           PredicateConfig config) throws Exception {

        logger.info("#### generateMultiTupleRuleSampling start----------------------------------------------------");
        SparkContextConfig sparkContextConfig = new SparkContextConfig(config, request, tableInfos);
        // collect all predicates, including ML, constant and other traditional predicates.
        // for paper: do not consider comparison predicates, i.e., <, >, <=, >=
        ArrayList<Predicate> allPredicates = new ArrayList<>();
        int ccount = 0;
        for (Predicate p : reeFinderEvidSet.getPredicateBuilder().getPredicates()) {
            if (p.isML() || p.getOperator() == Operator.EQUAL) {
                allPredicates.add(p);
            }
            ccount++;
        }
        // add constant predicates
        for (Predicate p : reeFinderEvidSet.getConstantPredicateBuilder().getPredicates()) {
            if (p.getOperator() == Operator.EQUAL) {
                allPredicates.add(p);
            }
        }

        int maxOneRelationNum = reeFinderEvidSet.getInput().getMaxTupleOneRelation();
        int allCount = reeFinderEvidSet.getInput().getAllCount();
        int maxTupleNum = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"maxTuplePerREE"));
//        int K = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "topK"));
        long support = (long)(sparkContextConfig.getCr() * allCount * allCount);
        double confidence = sparkContextConfig.getFtr();
//        int ifPrune = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "ifPrune"));

        // parameters of interestingness model
//        float w_supp = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"objectiveFea1"));
//        float w_conf = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"objectiveFea2"));
//        float w_diver = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"objectiveFea3"));
//        float w_succ = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"objectiveFea4"));
//        float w_sub = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"subjectiveFea"));

        // for RL
        int ifRL = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"ifRL"));

        // use confidence to filter work units
        int if_conf_filter = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "ifConfFilter"));
        float conf_filter_thr = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"confFilterThr"));

        int if_cluster_workunits = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "ifClusterWorkunits"));

        int filter_enum_number = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "filterEnumNumber"));

        String outputResultFile = RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "outputResultFile");

        ParallelRuleDiscoverySampling parallelRuleDiscoverySampling;
        if (ifRL == 0) {
            parallelRuleDiscoverySampling = new ParallelRuleDiscoverySampling(allPredicates, 10000, maxTupleNum,
                    support, (float)confidence, maxOneRelationNum, reeFinderEvidSet.getInput(), allCount,
                    1, 1, 1, 1, 1, 0, if_conf_filter, conf_filter_thr, if_cluster_workunits, filter_enum_number, false);
        } else {
            int ifOnlineTrainRL = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"ifOnlineTrainRL"));
            int ifOfflineTrainStage = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"ifOfflineTrainStage"));
            String PI_path = String.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"PIPath")); // python interpreter path
            String RL_code_path = String.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"RLCodePath")); // PredicateAssc code path
            int N = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"N"));
            int DeltaL = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"deltaL"));
            // parameters for RL model
            float learning_rate = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"learningRate"));
            float reward_decay = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"rewardDecay"));
            float e_greedy = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"eGreedy"));
            int replace_target_iter = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"replaceTargetIter"));
            int memory_size = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"memorySize"));
            int batch_size = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"batchSize"));

            parallelRuleDiscoverySampling = null;
//            new ParallelRuleDiscoverySampling(allPredicates, 10000, maxTupleNum,
//                    support, (float)confidence, maxOneRelationNum, reeFinderEvidSet.getInput(), allCount,
//                    1, 1, 1, 1, 1, 0, if_conf_filter, conf_filter_thr, if_cluster_workunits, filter_enum_number,
//                    ifRL, ifOnlineTrainRL, ifOfflineTrainStage, PI_path, RL_code_path, N, DeltaL,
//                    learning_rate, reward_decay, e_greedy, replace_target_iter, memory_size, batch_size);
        }

        // rule discovery
        String taskId = request.getTaskId();
        long startMineTime = System.currentTimeMillis();
        parallelRuleDiscoverySampling.levelwiseRuleDiscovery(taskId, spark, sparkContextConfig);
        long duringTime = System.currentTimeMillis() - startMineTime;
        // Get top-K rules
        DenialConstraintSet rees = parallelRuleDiscoverySampling.getTopKREEs();

        timeInfo.append("REE Discovery Time : ").append(duringTime).append("\n");
        logger.info("#### REE Discovery Time: {}", duringTime);

        // save rules
        for (DenialConstraint ree : rees) {
            if (ree == null) {
                continue;
            }
            timeInfo.append("Rule : ").append(ree.toString()).append("\n");
        }

        String outTxtPath = PredicateConfig.MLS_TMP_HOME + taskId + "/rule_all/" +  outputResultFile; //"experiment_results";
        FSDataOutputStream outTxt = hdfs.create(new Path(outTxtPath));
        outTxt.writeBytes(timeInfo.toString());
        outTxt.close();

        logger.info("#### REEs display----------------------------------------------------");
        int topk = 1;
        for (DenialConstraint ree : rees) {
            if (ree == null) {
                continue;
            }
            logger.info("TOP {}. {}", topk, ree.toString());
            topk ++;
        }

        List<RuleResult> ruleResults = new ArrayList<>();

        // final print
        int reeIndex = 0;
        for (DenialConstraint ree : rees) {
            if (ree == null) {
                continue;
            }
            reeIndex++;
            RuleResult rule = new RuleResult();
            rule.setID(reeIndex);
            rule.setRowSize(rees.getTotalTpCount());
            rule.setSupport(ree.getSupport());
            rule.setUnsupport(ree.getViolations());
            rule.setInnerRule(ree.toInnerString());
            rule.setRule(ree.toREEString());
            rule.setCheckSQL(ree.toChekSQLStr());
            rule.setCorrectSQL(ree.toCorrectSQLStr());

            ruleResults.add(rule);
        }

        return ruleResults;
    }

    public static DenialConstraint parseOneREE(String rule, REEFinderEvidSet reeFinderEvidSet) {
        String[] pstrings = rule.split(",")[0].split("->");
        String[] xStrings = pstrings[0].split("\\^");
        String[] xString = xStrings[xStrings.length - 1].trim().split("  ");
        PredicateSet ps = new PredicateSet();
        for (int i = 0; i < xString.length; i++) {
            ps.add(PredicateBuilder.parsePredicateString(reeFinderEvidSet.getInput(), xString[i].trim()));
        }
        String rhs = pstrings[pstrings.length - 1].trim();
        ps.addRHS(PredicateBuilder.parsePredicateString(reeFinderEvidSet.getInput(), rhs.substring(0, rhs.length() - 1)));
        DenialConstraint ree = new DenialConstraint(ps);
        ree.removeRHS();
        return ree;
    }

    private static DenialConstraintSet loadREEs(String taskId, String outputResultFile, FileSystem hdfs, REEFinderEvidSet reeFinderEvidSet) throws Exception {
        DenialConstraintSet rees = new DenialConstraintSet();
        String inputTxtPath = PredicateConfig.MLS_TMP_HOME + "constantRecovery/" + taskId + "/rule_all/" +  outputResultFile; //"experiment_results";
        FSDataInputStream inputTxt = hdfs.open(new Path(inputTxtPath));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        String line;
        String valid_prefix = "Rule : ";
        while ((line = bReader.readLine()) != null) {
            if (line.length() <= valid_prefix.length() || (!line.startsWith(valid_prefix))) {
                continue;
            } else {
                DenialConstraint ree = parseOneREE(line.trim(), reeFinderEvidSet);
                if (ree != null) {
                    rees.add(ree);
                }
            }
        }
        logger.info("#### load {} rees", rees.size());
        for (DenialConstraint ree : rees) {
            logger.info(ree.toString());
        }
        return rees;
    }

    private static List<RuleResult> generateMultiTupleRuleSamplingConstantRecovery(RuleDiscoverExecuteRequest request, REEFinderEvidSet reeFinderEvidSet,
                                                                   TableInfos tableInfos, FileSystem hdfs, SparkSession spark,
                                                                   PredicateConfig config) throws Exception {

        logger.info("#### generateMultiTupleRuleSamplingConstantRecovery start----------------------------------------------------");
        SparkContextConfig sparkContextConfig = new SparkContextConfig(config, request, tableInfos);
        // collect all predicates, including ML, constant and other traditional predicates.
        // for paper: do not consider comparison predicates, i.e., <, >, <=, >=
        ArrayList<Predicate> allPredicates = new ArrayList<>();
        int ccount = 0;
        for (Predicate p : reeFinderEvidSet.getPredicateBuilder().getPredicates()) {
            if (p.isML() || p.getOperator() == Operator.EQUAL) {
                allPredicates.add(p);
            }
            ccount++;
        }
//        int countConstantUB = 2;
//        int countC = 0;
        for (Predicate p : reeFinderEvidSet.getConstantPredicateBuilder().getPredicates()) {
            if (p.getOperator() == Operator.EQUAL) {
                allPredicates.add(p);
//                countC ++;
//                if (countC >= countConstantUB) {
//                    break;
//                }
            }
        }

        int maxOneRelationNum = reeFinderEvidSet.getInput().getMaxTupleOneRelation();
        int allCount = reeFinderEvidSet.getInput().getAllCount();
        int maxTupleNum = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"maxTuplePerREE"));
        int if_cluster_workunits = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "ifClusterWorkunits"));
//        int K = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "topK"));
        long support = (long)(sparkContextConfig.getCr() * allCount * allCount);
        double confidence = sparkContextConfig.getFtr();
//        int ifPrune = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "ifPrune"));
        String outputResultFile = RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "outputResultFile");

        // load from HDFS
        DenialConstraintSet reesStart = loadREEs(request.getTaskId(), outputResultFile, hdfs, reeFinderEvidSet);

        InputLight inputLight = new InputLight(reeFinderEvidSet.getInput());
        ConstantRecovery constantRecovery = new ConstantRecovery(reesStart, allPredicates, maxTupleNum, inputLight,
                support, (float)confidence, maxOneRelationNum, allCount, if_cluster_workunits);
        String taskId = request.getTaskId();
        long startMineTime = System.currentTimeMillis();
        constantRecovery.recovery(taskId, spark, sparkContextConfig);
        long duringTime = System.currentTimeMillis() - startMineTime;
        // Get all rules
        DenialConstraintSet rees = constantRecovery.getREEsResults();

        timeInfo.append("REE Discovery Time : ").append(duringTime).append("\n");
        logger.info("#### REE Discovery Time: {}", duringTime);

        // save rules
        for (DenialConstraint ree : rees) {
            if (ree == null) {
                continue;
            }
            timeInfo.append("Rule : ").append(ree.toString()).append("\n");
        }

        String outTxtPath = PredicateConfig.MLS_TMP_HOME + taskId + "/rule_all/" +  outputResultFile; //"experiment_results";
        FSDataOutputStream outTxt = hdfs.create(new Path(outTxtPath));
        outTxt.writeBytes(timeInfo.toString());
        outTxt.close();

        logger.info("#### REEs display----------------------------------------------------");
        int topk = 1;
        for (DenialConstraint ree : rees) {
            if (ree == null) {
                continue;
            }
            logger.info("TOP {}. {}", topk, ree.toString());
            topk ++;
        }

        List<RuleResult> ruleResults = new ArrayList<>();

        // final print
        int reeIndex = 0;
        for (DenialConstraint ree : rees) {
            if (ree == null) {
                continue;
            }
            reeIndex++;
            RuleResult rule = new RuleResult();
            rule.setID(reeIndex);
            rule.setRowSize(rees.getTotalTpCount());
            rule.setSupport(ree.getSupport());
            rule.setUnsupport(ree.getViolations());
            rule.setInnerRule(ree.toInnerString());
            rule.setRule(ree.toREEString());
            rule.setCheckSQL(ree.toChekSQLStr());
            rule.setCorrectSQL(ree.toCorrectSQLStr());

            ruleResults.add(rule);
        }

        return ruleResults;
    }

    /*
        for sampling and top-K algorithm
     */
    private static List<RuleResult> generateMultiTupleRule(RuleDiscoverExecuteRequest request, REEFinderEvidSet reeFinderEvidSet,
                                                           TableInfos tableInfos, FileSystem hdfs, SparkSession spark,
                                                           PredicateConfig config) throws Exception {

        logger.info("#### generateMultiTupleRule start----------------------------------------------------{}", spark.conf().get("runtimeParam"));
        SparkContextConfig sparkContextConfig = new SparkContextConfig(config, request, tableInfos);
        // collect all predicates, including ML, constant and other traditional predicates.
        // for paper: do not consider comparison predicates, i.e., <, >, <=, >=
        List<Predicate> allPredicates = new ArrayList<>();
        int ccount = 0;
        for (Predicate p : reeFinderEvidSet.getPredicateBuilder().getPredicates()) {
            if (p.isML() || p.getOperator() == Operator.EQUAL) {
                allPredicates.add(p);
            }
            ccount++;
        }
        logger.info("#### non-constant predicates size: {}", allPredicates.size());
        for (Predicate p : reeFinderEvidSet.getConstantPredicateBuilder().getPredicates()) {
            if (p.getOperator() == Operator.EQUAL) {
                allPredicates.add(p);
            }
        }
        logger.info("#### allPredicates size: {}", allPredicates.size());

        logger.info("#### allPredicates: {}", allPredicates);

//        writeAllPredicatesToFile(allPredicates, reeFinderEvidSet.getInput().getName());

        int maxOneRelationNum = reeFinderEvidSet.getInput().getMaxTupleOneRelation();
        int allCount = reeFinderEvidSet.getInput().getAllCount();
        int maxTupleNum = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"maxTuplePerREE"));
        int K = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "topK"));
        long support = (long)(sparkContextConfig.getCr() * allCount * allCount);
        double confidence = sparkContextConfig.getFtr();
        int ifPrune = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "ifPrune"));

        // parameters of interestingness model
        float w_supp = 0; // Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"objectiveFea1"));
        float w_conf = 0; // Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"objectiveFea2"));
        float w_diver = 0; // Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"objectiveFea3"));
        float w_succ = 0; // Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"objectiveFea4"));
        float w_sub = 0; // Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"subjectiveFea"));

        // whether use RL to prune
        int ifRL = 0; // Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"ifRL"));

        // use confidence to filter work units
        int if_conf_filter = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "ifConfFilter"));
        float conf_filter_thr = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"confFilterThr"));

        int if_cluster_workunits = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "ifClusterWorkunits"));

        int filter_enum_number = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "filterEnumNumber"));

        String outputResultFile = RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "outputResultFile");

        // whether use DQN to prune
        boolean ifDQN = Boolean.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "ifDQN"));

        // top-K rule interestingness discovery option
        String topKOption = RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "topKOption");
        String tokenToIDFile = RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "tokenToIDFile");
        String interestingnessModelFile = RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "interestingnessModelFile");
        String filterRegressionFile = RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "filterRegressionFile");

        boolean useConfHeuristic = Boolean.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "useConfHeuristic"));

        ParallelRuleDiscoverySampling parallelRuleDiscovery = new ParallelRuleDiscoverySampling(allPredicates, K, maxTupleNum,
                support, (float)confidence, maxOneRelationNum, reeFinderEvidSet.getInput(), allCount,
                w_supp, w_conf, w_diver, w_succ, w_sub, ifPrune, if_conf_filter, conf_filter_thr, if_cluster_workunits, filter_enum_number,
                topKOption, tokenToIDFile, interestingnessModelFile, filterRegressionFile, hdfs, useConfHeuristic);

//            int ifOnlineTrainRL = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"ifOnlineTrainRL"));
//            int ifOfflineTrainStage = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"ifOfflineTrainStage"));
//            String PI_path = String.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"PIPath")); // python interpreter path
//            String RL_code_path = String.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"RLCodePath")); // PredicateAssc code path
//            int N = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"N"));
//            int DeltaL = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"deltaL"));
//            // parameters for RL model
//            float learning_rate = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"learningRate"));
//            float reward_decay = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"rewardDecay"));
//            float e_greedy = Float.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"eGreedy"));
//            int replace_target_iter = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"replaceTargetIter"));
//            int memory_size = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"memorySize"));
//            int batch_size = Integer.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"),"batchSize"));
//
//            parallelRuleDiscovery = new ParallelRuleDiscoverySampling(allPredicates, K, maxTupleNum,
//                    support, (float)confidence, maxOneRelationNum, reeFinderEvidSet.getInput(), allCount,
//                    w_supp, w_conf, w_diver, w_succ, w_sub, ifPrune, if_conf_filter, conf_filter_thr, if_cluster_workunits, filter_enum_number,
//                    ifRL, ifOnlineTrainRL, ifOfflineTrainStage, PI_path, RL_code_path, N, DeltaL,
//                    learning_rate, reward_decay, e_greedy, replace_target_iter, memory_size, batch_size);

        // rule discovery
        String taskId = request.getTaskId();
        long startMineTime = System.currentTimeMillis();
        parallelRuleDiscovery.levelwiseRuleDiscovery(taskId, spark, sparkContextConfig);
        long duringTime = System.currentTimeMillis() - startMineTime;
        // Get top-K rules
//        DenialConstraintSet rees = parallelRuleDiscovery.getTopKREEs();
        ArrayList<DenialConstraint> rees = parallelRuleDiscovery.getTopKREEs_new();

        // sort REEs with the same scores
//        rees.sort(new Comparator<DenialConstraint>() {
//            @Override
//            public int compare(DenialConstraint o1, DenialConstraint o2) {
//                if (o1.getInterestingnessScore() < o2.getInterestingnessScore()) {
//                    return 1;
//                } else if (o1.getInterestingnessScore() > o2.getInterestingnessScore()) {
//                    return -1;
//                } else {
//                    return o1.toString().compareTo(o2.toString());
//                }
//            });
//        }

        timeInfo.append("REE Discovery Time : ").append(duringTime).append("\n");
        logger.info("#### REE Discovery Time: {}", duringTime);

        // save rules
        for (DenialConstraint ree : rees) {
            if (ree == null) {
                continue;
            }
            timeInfo.append("Rule : ").append(ree.toString()).append(", supp: ").append(ree.getSupport()).append(", conf:").append(ree.getConfidence()).append(", score:").append(ree.getInterestingnessScore()).append("\n");

//            timeInfo.append("Rule : ").append(ree.toREEString()).append(", supp: ").append(ree.getSupport()).append(", conf:").append(ree.getConfidence()).append(", succ:").append(1.0 / ree.getPredicateSet().size()).append(", score:").append(ree.getInterestingnessScore()).append("\n");
        }

        String outTxtPath = PredicateConfig.MLS_TMP_HOME + taskId + "/rule_all/" +  outputResultFile; //"experiment_results";
        FSDataOutputStream outTxt = hdfs.create(new Path(outTxtPath));
        outTxt.writeBytes(timeInfo.toString());
        outTxt.close();

        logger.info("#### REEs display----------------------------------------------------");
        int topk = 1;
        for (DenialConstraint ree : rees) {
            if (ree == null) {
                continue;
            }
            logger.info("TOP {}. {}", topk, ree.toString());
            topk ++;
        }

        List<RuleResult> ruleResults = new ArrayList<>();

        // final print
        int reeIndex = 0;
        for (DenialConstraint ree : rees) {
            if (ree == null) {
                continue;
            }
            reeIndex++;
            RuleResult rule = new RuleResult();
            rule.setID(reeIndex);
//            rule.setRowSize(rees.getTotalTpCount());
            rule.setSupport(ree.getSupport());
            rule.setUnsupport(ree.getViolations());
            rule.setInnerRule(ree.toInnerString());
            rule.setRule(ree.toREEString());
            rule.setCheckSQL(ree.toChekSQLStr());
            rule.setCorrectSQL(ree.toCorrectSQLStr());

            ruleResults.add(rule);
        }

        return ruleResults;
    }


    public static List<RuleResult> getAllRule(RuleDiscoverExecuteRequest request, List<RuleResult> allRuleRlt) {
        String taskId = request.getTaskId();
        String outRulePath = PredicateConfig.MLS_TMP_HOME + taskId + "/allrule.txt";

        logger.info("######ALL RULE PATH: " + outRulePath);
        long startMinCoverTime = System.currentTimeMillis();
        List<RuleResult> finalRuleRlt = new ArrayList<>();
        for(RuleResult rule : allRuleRlt) {
            finalRuleRlt.add(rule);
        }

        long endMinCoverTime = System.currentTimeMillis();

        long mincoverTime = endMinCoverTime - startMinCoverTime;
        timeInfo.append("mincover time: ").append(mincoverTime).append("\n");

        return finalRuleRlt;
    }


    private static List<String> generateConstant(String taskId, TableInfos tableInfos, SparkSession spark, PredicateConfig config,
                                                 boolean erRuleFinderFlag, String eidName) {
        if (!config.isExecuteOnlyNonConstantPred()) {
            List<String> allTablePredicates = new ArrayList<>();
            for (TableInfo tableInfo : tableInfos.getTableInfoList()) {
                List<String> tablePredicate = null;
                List<Row> constantRow = ConstantPredicateGenerateMain.geneConstantList(spark, tableInfo, config, erRuleFinderFlag, eidName);

                logger.info("####{},{} generate constant row {} 条", taskId, tableInfo.getTableName(), constantRow.size());
                tablePredicate = ConstantPredicateGenerateMain.genePredicateList(constantRow, tableInfo, config);
                logger.info("####{},{} generate constant predicate {} 条", taskId, tableInfo.getTableName(), tablePredicate.size());
                logger.info("####{},{} generate constant predicate {}", taskId, tableInfo.getTableName(), tablePredicate.toString());
                allTablePredicates.addAll(tablePredicate);
            }
            return allTablePredicates;
        }
        return new ArrayList<>();
    }

    public static void writeAllPredicatesToFile(List<Predicate> allPredicates, String table_name) {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("/opt/allPredicates_" + table_name + ".txt"));
            for (Predicate p : allPredicates) {
                out.write(p.toString());
                out.write("\n");
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeRuleToHDFS(FileSystem hdfs, List<RuleResult> finalRR, String path) throws IOException {
//        logger.info("######PATH: " + path);
        FSDataOutputStream outTxt = hdfs.create(new Path(path));
        for (RuleResult rule : finalRR) {
            //备份的结果只要保留REE格式即可
            outTxt.write(rule.getPrintREEs().getBytes("UTF-8"));
            outTxt.writeBytes("\n");
        }
        outTxt.close();
    }
}
