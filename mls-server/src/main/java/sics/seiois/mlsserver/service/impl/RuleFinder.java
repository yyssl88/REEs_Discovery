package sics.seiois.mlsserver.service.impl;

import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.client.model.mls.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sics.seiois.mlsserver.biz.mock.RuleFindRequestMock;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import sics.seiois.mlsserver.mlpredicate.ResourceManager;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.utils.MlsConstant;

public class RuleFinder {

    private static final Logger logger = LoggerFactory.getLogger(RuleFinder.class);

    public static final String MLS_REQUEST_FILE_NAME_SUFFIX = "_req.json";
    public static final String MLS_CONFIG_FILE_NAME_SUFFIX = "/conf/predict.conf";

    public static String numOfProcessors = "40";

    public static boolean doRuleDiscovery(RuleDiscoverExecuteRequest command) {
        return RuleFinder.doRuleDiscoveryJava(command);
    }

    public static boolean doRuleDiscoveryJava(RuleDiscoverExecuteRequest request) {

        String taskId = request.getTaskId();
        String localFileDir = PredicateConfig.MLS_TMP_HOME + taskId;
        TableInfos tableInfos = request.getTableInfos();

        try {
            //创建一个本地文件夹存放本地文件
            File localDir = new File(localFileDir);
            if (!localDir.exists()) {
                localDir.mkdirs();
            }

            //将传入的request参数序列化，存入本地文件 /tmp/rulefind/{taskId}/{taskId}_req.json
            String requestJsonFileName = taskId + MLS_REQUEST_FILE_NAME_SUFFIX;
            String requestJsonFilePath = localFileDir + File.separator + requestJsonFileName;
            File reqJsonFile = new File(requestJsonFilePath);
            if (reqJsonFile.exists()) {
                reqJsonFile.delete();
                reqJsonFile.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(reqJsonFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fileWriter);
            bw.write(request.toString());
            bw.close();

            //***第一步: 提交到spark生成谓词集合(包括字符串，数字，常数谓词等)和evidence set集合。
            logger.info("####{} [第一步]提交到spark生成evidenceset", taskId);
            //获取classpath执行路径，需要拷贝jar包以及相关文件到spark节点去
            String fpath = getPath();

            // 从数据库中读取mls相关的配置项，并写入配置文件
            String configFilePath = fpath.split(":")[1] + MLS_CONFIG_FILE_NAME_SUFFIX;
//            File configFile = new File(configFilePath);
//            if (configFile.exists()) {
//                configFile.delete();
//            }
            PredicateConfig predicateConfig = new PredicateConfig(configFilePath, true);

            Map<String, String> sparkClintConfMap = readFileAndSetSparkParam(MlsConstant.getSparkClientConfigPath());

            // configure spark environment
            // 16 env
            sparkClintConfMap.put("spark.driver.memory","30G");
            sparkClintConfMap.put("spark.executor.memory","20G");
            sparkClintConfMap.put("spark.executor.cores","1");
//            sparkClintConfMap.put("spark.executor.cores","1");
//             sparkClintConfMap.put("spark.executor.instances", "10");
            sparkClintConfMap.put("spark.executor.instances",numOfProcessors);
            sparkClintConfMap.put("spark.network.timeout", "300");
//            sparkClintConfMap.put("spark.executor.instances","4");

//            // 64 env
//            sparkClintConfMap.put("spark.driver.memory","8G");
//            sparkClintConfMap.put("spark.executor.memory","4G");
//            sparkClintConfMap.put("spark.executor.cores","1");
//            sparkClintConfMap.put("spark.executor.instances",numOfProcessors);

            logger.info("####{} getSparkClientConfig:{}", taskId, sparkClintConfMap);
            Map<String, String> conf = new HashMap<>();
            conf.put(PredicateConfig.SPARK_HOME, PredicateConfig.getSparkHome());
            conf.put("HADOOP_USER_NAME","spark");

            SparkLauncher sparkLauncher = new SparkLauncher(conf);
            for (String key : sparkClintConfMap.keySet()) {
                sparkLauncher.setConf(key, sparkClintConfMap.get(key));
            }
            logger.info("#### extraJavaOptions:{}", sparkClintConfMap.get("spark.executor.extraJavaOptions"));

            sparkLauncher.setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            sparkLauncher.setConf("spark.kryo.registrationRequired", "false");
            sparkLauncher.setConf("spark.kryo.registrator", "sics.seiois.mlsserver.service.impl.MyKyroRegistrator");
            sparkLauncher.setConf("spark.kryoserializer.buffer.max", "1550m");
//            sparkLauncher.setConf("spark.yarn.queue", RuntimeParamUtil.canGet(request.getOtherParam(),"queueName") ? RuntimeParamUtil.getRuntimeParam(request.getOtherParam(),"queueName"):predicateConfig.getQueueName());
//            sparkLauncher.setConf("spark.executor.cores", predicateConfig.getExecutorCore());
//            sparkLauncher.setConf("spark.executor.memory", predicateConfig.getExecutorMem());
//            sparkLauncher.setConf("spark.driver.memory", predicateConfig.getDriverMem());
//            sparkLauncher.setConf("spark.driver.memory","20G");
//            sparkLauncher.setConf("spark.executor.memory","3G");
//            sparkLauncher.setConf("spark.executor.memoryoverhead","4G");
//            sparkLauncher.setConf("spark.executor.cores","1");
////            sparkLauncher.setConf("spark.executor.instances",numOfProcessors);
//            sparkLauncher.setConf("spark.network.timeout", "360");
            sparkLauncher.setConf("spark.executor.heartbeatInterval", "30");
//            sparkLauncher.setConf("spark.executor.instances", "15");

            String path = "hdfs:///data/models/resources.zip";

            // 第二种方法，用jars参数加载jar包
            String jarPath = fpath+"/lib/fastjson-1.2.52.jar" + "," +
                    fpath + "/lib/seiois-common-0.1.1-SNAPSHOT.jar" + "," +
                    fpath + "/lib/seiois-client-0.1.1-SNAPSHOT.jar" + "," +
                    fpath + "/lib/libtensorflow-1.15.0.jar" + "," +
                    fpath + "/lib/libtensorflow_jni-1.15.0.jar" + "," +
                    fpath + "/lib/hanlp-portable-1.7.8.jar" + "," +
                    fpath + "/lib/algorithm_integration-1.2.jar" + "," +
                    fpath + "/lib/backend-1.2.jar" + "," +
                    fpath + "/lib/java-lsh-RELEASE.jar" + "," +
                    fpath + "/lib/trove4j-3.0.3.jar" + "," +
                    fpath + "/lib/backend-1.2.jar" + "," +
                    fpath + "/lib/stanford-corenlp-3.9.2.jar" + "," +
                    fpath + "/lib/mls-guava-0.24.jar" + "," +
                    fpath + "/lib/algorithm_integration-1.2.jar" + "," +
                    fpath + "/lib/java-lsh-RELEASE.jar" + "," +
                    fpath + "/lib/opencsv-2.3.jar" + ",";

            sparkLauncher.setAppName("ruleFinder")
                    .setConf("spark.debug.maxToStringFields", "100")
                    .setMaster("yarn")
                    .setAppResource(fpath + "/lib/mls-server-0.1.1.jar")
                    .setMainClass("sics.seiois.mlsserver.service.impl.EvidenceGenerateMain")
                    .addAppArgs(requestJsonFileName)
                    .addFile(requestJsonFilePath)
                    .addFile(fpath + "/conf/predict.conf")
                    // TODO: 优化路径
                    .addSparkArg("--archives", ResourceManager.getArchivesFilePath())
                    .addSparkArg("--jars", jarPath)
                    .setDeployMode("cluster");
       /*     if(predicateConfig.isDistributeBranchFlag()) {
                sparkLauncher.addFile(fpath + "/conf/ml_predicates.txt");
//                        sparkLauncher.addFile("python.tar.gz");
            }
*/
            SparkAppHandle handler = sparkLauncher.startApplication();

            while (!"FINISHED".equalsIgnoreCase(handler.getState().toString()) && !"FAILED".equalsIgnoreCase(handler.getState().toString()) && !"KILLED".equalsIgnoreCase(handler.getState().toString())) {
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    logger.error("####{} [第一步]spark handle exception", taskId, e);
                }
            }

            if (!"FINISHED".equalsIgnoreCase(handler.getState().toString())) {
                logger.error("####{} [第一步]spark handle Failed : id={}, state={}", taskId, handler.getAppId(), handler.getState());
                return false;
            }
        } catch (Exception e) {
            logger.error("####{} [规则发现异常]rule discover spark execute exception", taskId, e);
            return false;
        }

        return true;
    }

	public static String getPath() {
        //获取classpath执行路径，需要拷贝jar包以及相关文件到spark节点去
        String fpath = RuleFinder.class.getResource("").getPath();
        fpath = fpath.substring(0, fpath.indexOf("lib") - 1);
        return fpath;
    }


    public static void main(String[] args) {
        Map<String, String> argsMap = convert(args);
        String cr = argsMap.get("support"); //覆盖率
        String ftr =argsMap.get("confidence");//容错率
        String taskID = argsMap.get("taskID");
        String dataset = argsMap.get("dataset");
        String highSelectivityRatio = argsMap.get("highSelectivityRatio");
        String interestingness = argsMap.get("interestingness");
        String queueName = argsMap.get("queueName");
        String skipEnum = argsMap.get("skipEnum");

        // Arguments of top-K algorithms
        String topK = argsMap.get("topK");
        String round = argsMap.get("round"); // for AnyTime Top-k
        String maxTuplePerREE = argsMap.get("maxTuplePerREE");
        String ifPrune = argsMap.get("ifPrune");
        String objectiveFea1 = argsMap.get("objectiveFea1");
        String objectiveFea2 = argsMap.get("objectiveFea2");
        String objectiveFea3 = argsMap.get("objectiveFea3");
        String objectiveFea4 = argsMap.get("objectiveFea4");
        String subjectiveFea = argsMap.get("subjectiveFea");
        String outputResultFile = argsMap.get("outputResultFile");
        String algOption = argsMap.get("algOption");
        String numOfProcessors_ = argsMap.get("numOfProcessors");
        String mlOption = argsMap.get("MLOption");

        // for RL
        String ifRL = argsMap.get("ifRL");
        String ifOnlineTrainRL = argsMap.get("ifOnlineTrainRL");
        String ifOfflineTrainStage = argsMap.get("ifOfflineTrainStage");
        String PI_path = argsMap.get("PIPath");
        String RL_code_path = argsMap.get("RLCodePath");
        String N = argsMap.get("N");
        String DeltaL = argsMap.get("deltaL");
        String learning_rate = argsMap.get("learningRate");
        String reward_decay = argsMap.get("rewardDecay");
        String e_greedy = argsMap.get("eGreedy");
        String replace_target_iter = argsMap.get("replaceTargetIter");
        String memory_size = argsMap.get("memorySize");
        String batch_size = argsMap.get("batchSize");

        String if_conf_filter = argsMap.get("ifConfFilter");
        String conf_filter_thr = argsMap.get("confFilterThr");

        String if_cluster_workunits = argsMap.get("ifClusterWorkunits");

        numOfProcessors = numOfProcessors_;

        StringBuffer otherParam = new StringBuffer();
        otherParam.append("highSelectivityRatio=" + highSelectivityRatio + ";");
        otherParam.append("interestingness=" + interestingness + ";");
        otherParam.append("queueName=" + queueName + ";");
        otherParam.append("skipEnum=" + skipEnum + ";");
        otherParam.append("topK=" + topK + ";");
        otherParam.append("round=" + round + ";");
        otherParam.append("maxTuplePerREE=" + maxTuplePerREE + ";");
        otherParam.append("ifPrune=" + ifPrune + ";");
        otherParam.append("objectiveFea1=" + objectiveFea1 + ";");
        otherParam.append("objectiveFea2=" + objectiveFea2 + ";");
        otherParam.append("objectiveFea3=" + objectiveFea3 + ";");
        otherParam.append("objectiveFea4=" + objectiveFea4 + ";");
        otherParam.append("subjectiveFea=" + subjectiveFea + ";");
        otherParam.append("outputResultFile=" + outputResultFile + ";");
        otherParam.append("algOption=" + algOption + ";");
        otherParam.append("MLOption=" + mlOption + ";");
        otherParam.append("ifRL=" + ifRL + ";");
        otherParam.append("ifOnlineTrainRL=" + ifOnlineTrainRL + ";");
        otherParam.append("ifOfflineTrainStage=" + ifOfflineTrainStage + ";");
        otherParam.append("PIPath=" + PI_path + ";");
        otherParam.append("RLCodePath=" + RL_code_path + ";");
        otherParam.append("N=" + N + ";");
        otherParam.append("deltaL=" + DeltaL + ";");
        otherParam.append("learningRate=" + learning_rate + ";");
        otherParam.append("rewardDecay=" + reward_decay + ";");
        otherParam.append("eGreedy=" + e_greedy + ";");
        otherParam.append("replaceTargetIter=" + replace_target_iter + ";");
        otherParam.append("memorySize=" + memory_size + ";");
        otherParam.append("batchSize=" + batch_size + ";");

        otherParam.append("ifConfFilter=" + if_conf_filter + ";");
        otherParam.append("confFilterThr=" + conf_filter_thr + ";");

        otherParam.append("ifClusterWorkunits=" + if_cluster_workunits + ";");

//        RuleDiscoverExecuteRequest req = RuleFindRequestMock.mockRuleFindReqest(dataset);
        RuleDiscoverExecuteRequest req = RuleFindRequestMock.mockRuleFindReqest(dataset, taskID);
        req.setTaskId(taskID);
        req.setCr(cr);
        req.setFtr(ftr);

        req.setOtherParam(otherParam.toString());

        doRuleDiscoveryJava(req);
//        doSklearnPy(req);
    }

    public static Map<String, String> convert(String[] args) {
        logger.info("arguments : {}", args);
        Map<String, String> argsMap = new HashMap<>();
        for (String arg : args) {
            logger.info("argument : {}", arg);
            String[] name2value = arg.split("=");
            argsMap.put(name2value[0], name2value[1]);
        }
        return argsMap;
    }

    /**
     * 读取spark-client配置文件并且组装成一个map
     *
     * @param sparkClintConfigPath 文件路径
     * @throws Exception 异常
     * @author 苏阳华
     */
    public static Map<String, String> readFileAndSetSparkParam(String sparkClintConfigPath) {
        logger.info("sparkClientConfigPath={}", sparkClintConfigPath);
        Map<String, String> sparkClintConfMap = new HashMap<>();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(sparkClintConfigPath)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if(StringUtils.isNotEmpty(line)
                        && StringUtils.isNotEmpty(line.trim())
                        && !line.trim().startsWith("#")) {  //如果该行不为空并且不是以#开头(如果是#开头说明是注释)
                    String[] key2value = line.split(" ");
                    String key = key2value[0];
                    String value = "";
                    for (int i = 1; i < key2value.length; i++) {
                        value = value + " " + key2value[i];
                    }
                    sparkClintConfMap.put(key.trim(), value.trim());
                }
            }
        } catch (Exception e) {
            logger.error("read sparkClientConfig error,sparkClientConfigPath={}", MlsConstant.getSparkClientConfigPath(), e);
        }

        return sparkClintConfMap;
    }

}