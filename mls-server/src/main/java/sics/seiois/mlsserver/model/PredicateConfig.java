package sics.seiois.mlsserver.model;

import jodd.util.PropertiesUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.utils.properties.MlsPropertiesUtils;

import java.io.*;
import java.math.BigDecimal;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.GET;


/***
 *
 * @author liuyong
 *
 */
@Getter
public class PredicateConfig {

    private static final Logger logger = LoggerFactory.getLogger(PredicateConfig.class);

    private volatile  static PredicateConfig config = null;

    //  init value as default setting value
    @Setter
    private String configFilePath = PREDICATE_CONF_PATH;

    @Setter
    private boolean stringPredicateOperatorEQ = true;

    @Setter
    private boolean stringPredicateOperatorNEQ = true;

    @Setter
    private boolean numberPredicateOperatorEQ = true;

    @Setter
    private boolean numberPredicateOperatorNEQ = true;

    @Setter
    private boolean numberPredicateOperatorGTE = true;

    @Setter
    private boolean numberPredicateOperatorLTE = true;

    @Setter
    private boolean constantStringPredicateOperatorEQ = true;

    @Setter
    private boolean constantStringPredicateOperatorNEQ = true;

    @Setter
    private boolean constantNumberPredicateOperatorEQ = true;

    @Setter
    private boolean constantNumberPredicateOperatorNEQ = true;

    @Setter
    private boolean constantNumberPredicateOperatorGTE = true;

    @Setter
    private boolean constantNumberPredicateOperatorLTE = true;

    private Map<String, String> numberOperators = new HashMap<>();
    private Map<String, String> stringOperators = new HashMap<>();
    private Map<String, String> constantNumOperators = new HashMap<>();
    private Map<String, String> constantStrOperators = new HashMap<>();

    //常数过滤下限百分比
    private Float constantFilterRatio = 10.0f;
    //常数过滤上限百分比
    private Float constantHighFilterRatio = 100.0f;
	// 是否使用常数区间进行过滤
    private Boolean useConstantInterval = true;
    // 每个常数区间占的百分比
    private Integer constantInterval = 1;

    private String predicateType;

    //kmean算法区间数
    private Integer kmeansCluster;

    //kmean最大迭代数
    private Integer kmeansMaxIterNum;

    //对orc文件进行repartition的数量等于orc文件大小除以repartition_size
    private Integer repartition_size;

    /****************************以下是分布式引入的配置********************************/
    //是否走分布式分支。如果是,ES用索引构建,挖掘实现分布式
    @Getter
    private boolean distributeBranchFlag = false;

    //blocking中分批做笛卡尔积的规模
    @Getter
    @Setter
    private int blockingDescartes = 1000 ;

    //分布式切分粒度的条数,不能超过12000
    @Getter
    private int chunkLength = 100000;

    //每次内存计算的条数限制，建议在2,000,000以下
	@Getter
    private long bufferLength = 500;

    //任务数量
    @Getter
    private int batchNum;

    @Getter
    private String queueName;
    @Getter
    private String executorCore;
    @Getter
    private String executorMem;
    @Getter
    private String driverMem;
    //需要对ES进行分类的系数。总共分成2的groupSize的次数个分组。比如groupSize=4,会将ES分成16组
    @Getter
    @Setter
    private int groupSize;
	@Getter
    @Setter
    private long lineCount;

    //是否生成跨表的谓词.默认为true:生成跨表谓词
    @Getter
    @Setter
    private boolean crossColumnFlag;

    @Getter
    @Setter
    private int fpNumber;

    @Getter
    @Setter
    private String runtimeParam;

    @Getter
    @Setter
    private static final int exampleSize = 10;

    /****************************以下是常数变量********************************/

    //包含非常数谓词及常数谓词
    private final static String BASE_COLUMN_PREDICATE = "1";

    private final static String ONLY_NO_CONSTANT_PREDICATE = "2";
    private final static String ONLY_CONSTANT_PREDICATE = "3";
    //非常数谓词及常数谓词联合模式
    private final static String UNION_PREDICATE = "4";
    private final static String CROSS_COLUMN_PREDICATE = "9";
    public static final String SPARK_HOME = "SPARK_HOME";

    //代码中的常量定义
    public static final String MLS_TMP_HOME = "/tmp/rulefind/";
    public static final String MLS_ES_FOLDER = "/all_es/";
    public static final String MLS_CONSTANT_ES_FOLDER = "/constant_es/";
    public static final String MLS_FP_PLI_FOLDER = "/fp_pli/";
    public static final String MLS_CONSTANT_PS_FOLDER = "/constant_ps/";
    public static final String PREDICATE_CONF_PATH = "/opt/seiois/mls-server/conf/predict.conf";
    public static final String TBL_COL_DELIMITER = "__";
    public static final String JOIN_TABLE_DELIMITER = "_AND_";
    public static final String FP_COLUMN_SEPARATION_CHARACTER = "@#wqy%";


    //spark Home地址。在mls调用spark的时候需要传入spark Home
    private static String sparkHome;
    public static String getSparkHome() {
        return sparkHome;
    }
    //sklearn依赖的python虚拟环境Anacoda的bin地址
    private static String anaconda3BinPath;
    public static String getAnaconda3BinPath() {
        return anaconda3BinPath;
    }
    //sklearn依赖的python虚拟环境Anacoda的虚拟环境
    private String anaconda3EnvName;
    //sklearn生成谓词关系步骤的开关。在整体数据量较少的时候为false
    private static boolean sklearnSwitch;
    public static boolean getSklearnSwitch() {
        return sklearnSwitch;
    }
    //sklearn步骤的threshold配置参数。默认为0.0，设定值在[0,0.2]之间的浮点数
    private static float sklearnThreshold;
    public static float getSklearnThreshold() {
        return sklearnThreshold;
    }
    //sklearn步骤的topK配置参数。默认为90，设定值在[50,100]之间的整数
    private static int sklearnTopK;
    public static int getSklearnTopK() {
        return sklearnTopK;
    }
    //规则发现的minimize开关
    private static boolean ruleFindMinimizeSwitch;
    public static boolean getRuleFindMinimizeSwitch() {
        return ruleFindMinimizeSwitch;
    }
    //谓词重叠数据比率。默认为1，设定值为[0,1]的浮点数
    private static float ruleFindRatioEvidset;
    public static float getRuleFindRatioEvidset() {
        return ruleFindRatioEvidset;
    }
    //RHS包含数据占总数据的比率。默认-1代表取容错率的值，设定值为[0,1]的浮点数
    private static float rulefindRHSRate;
    public static float getRulefindRHSRate() {
        return rulefindRHSRate;
    }
    //sklearn步骤的scale配置参数。默认为100
    public static float sklearnScale;
    public static float getSklearnScale() { return sklearnScale; }
    //sklearn步骤的alpha配置参数。默认为0.001
    public static float sklearnAlpha;
    public static float getSklearnAlpha() { return sklearnAlpha; }
    //sklearn步骤的maxIter配置参数。默认为2000
    public static int sklearnMaxIter;
    public static int getSklearnMaxIter() { return sklearnMaxIter; }
    //关系跨表数
    public static int L;
    public static int getL() { return L; }

    //er规则发现是否打开
    public static boolean erSwitch;

    //sentenceBert 模型相似度的阈值。大于这个值默认为相似
    public static double sentenceBertSimiRate;

    //基于jaccard的blocking算法的阈值
    public static double jaccardBlockingRatio;

    //基于SentenceBert的blocking算法的阈值
    public static double sbertBlockingRatio;

    public static PredicateConfig getInstance(String configFilePath) {
        if (config == null) {
            synchronized (PredicateConfig.class) {
                if (config == null) {
                    config = new PredicateConfig(configFilePath, false);
                }
            }
        }
        return config;
    }

    public PredicateConfig() {
        File file =new File(getConfigFilePath());
        if(!file.exists()){
            initDataFromDB(getConfigFilePath());
        }

        initData();
    }

    //initData为true时，强制更新为数据库最新配置
    public PredicateConfig(String configFilePath, boolean initData) {
        setConfigFilePath(configFilePath);

        if(initData) {
            initDataFromDB(getConfigFilePath());
        }
        initData();
    }

    /**
     * 从数据库读取配置项，并写入配置文件中
     * @param configFilePath
     */
    public void initDataFromDB(String configFilePath) {
        // mls配置项从数据库读取
        try {
            Map<String, String> map = MlsPropertiesUtils.getMap();

            File file =new File(configFilePath);
            if(file.exists()){
                file.delete();
            }

            file.createNewFile();
            FileWriter fileWritter = new FileWriter(configFilePath);
            for (String key : map.keySet()) {
                fileWritter.write(key + "=" + map.get(key) + "\n");
                fileWritter.flush();
            }
            logger.info("#### PredicateConfig initDataFromDB and Write2File finished!map={}", map.toString());
        } catch (IOException e){
            e.printStackTrace();
            logger.error("#### PredicateConfig initDataFromDB and Write2File exception.", e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initData() {
        stringOperators.put("==", "=");
        numberOperators.put("==", "=");
        numberOperators.put(">=", ">=");
        numberOperators.put("<=", "<=");
        constantStrOperators.put("==", "=");
        constantNumOperators.put("==", "=");
        constantNumOperators.put(">=", ">=");
        constantNumOperators.put("<=", "<=");
        predicateType = UNION_PREDICATE;
        kmeansCluster = 10;
        kmeansMaxIterNum = 1000;

        Properties properties = new Properties();

        //InputStream inputStream = PropertiesUtil.class.getClassLoader().getResourceAsStream(configFilePath);

        // mls配置项从文件读取
        try {
            //逐渐过度到这个方法读取配置文件
            Configurations configs = new Configurations();
            PropertiesConfiguration config = configs.properties(getConfigFilePath());
            config.setThrowExceptionOnMissing(false);

            this.distributeBranchFlag = config.getBoolean("distributeBranchFlag",true);
            this.chunkLength = config.getInt("chunkLength",10000);
            this.bufferLength = config.getLong("bufferLength",1000000);
            this.queueName = config.getString("queueName","default");
            this.executorCore = config.getString("executorCore","1");
            this.executorMem = config.getString("executorMem","4G");
            this.driverMem = config.getString("driverMem","5G");
            this.batchNum = config.getInt("batchNum",3);
            this.crossColumnFlag = config.getBoolean("crossColumnFlag", true);
            this.fpNumber = config.getInt("fpNum", -1);
            this.erSwitch = config.getBoolean("erSwitch", false);
            this.sentenceBertSimiRate = config.getDouble("sentenceBertSimiRate", 0.95);
            this.jaccardBlockingRatio = config.getDouble("jaccardBlockingRatio", 0.75);
            this.sbertBlockingRatio = config.getDouble("sbertBlockingRatio", 0.75);

            //老的读取配置文件的方式，逐渐废弃
            properties.load(new FileInputStream(getConfigFilePath()));
        } catch (Exception e) {
            logger.error("load properties error", e);
        }


        Enumeration<?> enum1 = properties.propertyNames();

            while (enum1.hasMoreElements()) {
                String key = (String) enum1.nextElement();
                String value = properties.getProperty(key);
                switch (key) {
                    case "predicate_type":
                        predicateType = value;
                        break;
                    case "string_predicate_operator":
                        String[] spoArray = value.split(" ");
                        /* first set all flags to false */
                        setStringPredicateOperatorEQ(false);
                        setStringPredicateOperatorNEQ(false);
                        stringOperators.clear();
                        /* set flag to true independently */
                        for (String str : spoArray) {
                            switch (str) {
                                case "==":
                                    setStringPredicateOperatorEQ(true);
                                    stringOperators.put("==", "=");
                                    break;
                                case "<>":
                                    setStringPredicateOperatorNEQ(true);
                                    stringOperators.put("<>", "<>");
                            }
                        }
                        break;
                    case "number_predicate_operator":
                        String[] npoArray = value.split(" ");
                        /* first set all flags to false */
                        setNumberPredicateOperatorEQ(false);
                        setNumberPredicateOperatorNEQ(false);
                        setNumberPredicateOperatorGTE(false);
                        setNumberPredicateOperatorLTE(false);
                        numberOperators.clear();
                        /* set flag to true independently */
                        for (String str : npoArray) {
                            switch (str) {
                                case "==":
                                    setNumberPredicateOperatorEQ(true);
                                    numberOperators.put("==", "=");
                                    break;
                                case "<>":
                                    setNumberPredicateOperatorNEQ(true);
                                    numberOperators.put("<>", "<>");
                                    break;
                                case ">=":
                                    setNumberPredicateOperatorGTE(true);
                                    numberOperators.put(">=", ">=");
                                    break;
                                case "<=":
                                    setNumberPredicateOperatorLTE(true);
                                    numberOperators.put("<=", "<=");
                                    break;
                            }
                        }
                        break;

                    case "constant_string_predicate_operator":
                        String[] cspoArray = value.split(" ");
                        /* first set all flags to false */
                        setConstantStringPredicateOperatorEQ(false);
                        setConstantStringPredicateOperatorEQ(false);
                        constantStrOperators.clear();
                        /* set flag to true independently */
                        for (String str : cspoArray) {
                            switch (str) {
                                case "==":
                                    setConstantStringPredicateOperatorEQ(true);
                                    constantStrOperators.put("==", "=");
                                    break;
                                case "<>":
                                    setConstantStringPredicateOperatorEQ(true);
                                    constantStrOperators.put("<>", "<>");
                                    break;
                            }
                        }
                        break;

                    case "constant_number_predicate_operator":
                        String[] cnpoArray = value.split(" ");
                        /* first set all flags to false */
                        setConstantNumberPredicateOperatorEQ(false);
                        setConstantNumberPredicateOperatorNEQ(false);
                        setConstantNumberPredicateOperatorGTE(false);
                        setConstantNumberPredicateOperatorLTE(false);
                        constantNumOperators.clear();
                        /* set flag to true independently */
                        for (String str : cnpoArray) {
                            switch (str) {
                                case "==":
                                    setConstantNumberPredicateOperatorEQ(true);
                                    constantNumOperators.put("==", "=");
                                    break;
                                case "<>":
                                    setConstantNumberPredicateOperatorNEQ(true);
                                    constantNumOperators.put("<>", "<>");
                                    break;
                                case ">=":
                                    setConstantNumberPredicateOperatorGTE(true);
                                    constantNumOperators.put(">=", ">=");
                                    break;
                                case "<=":
                                    setConstantNumberPredicateOperatorLTE(true);
                                    constantNumOperators.put("<=", "<=");
                                    break;
                            }
                        }
                        break;
                    case "constant_filter_ratio":
                        String[] constantRatio = value.split(",");

                        this.constantFilterRatio = Float.parseFloat(constantRatio[0].trim());
                        if(constantRatio.length > 1) {
                            this.constantHighFilterRatio = Float.parseFloat(constantRatio[1].trim());
                        }
                        this.constantFilterRatio = Math.max(0.01f, this.constantFilterRatio);
                        break;
                    case "kmean_cluster_num":
                        this.kmeansCluster = Integer.parseInt(value);
                        break;
                    case "kmean_max_iterative":
                        this.kmeansMaxIterNum = Integer.parseInt(value);
                        break;
                    case "repartition_size":
                        this.repartition_size = Integer.parseInt(value);
                        break;
                    case "constantInterval":
                        this.constantInterval = Integer.parseInt(value);
                        break;
                    case "sparkHome":
                        this.sparkHome = value;
                        break;
                    case "anaconda3BinPath":
                        this.anaconda3BinPath = value;
                        break;
                    case "anaconda3EnvName":
                        this.anaconda3EnvName = value;
                        break;
                    case "sklearnSwitch":
                        this.sklearnSwitch = Boolean.parseBoolean(value);
                        break;
                    case "sklearnThreshold":
                        this.sklearnThreshold = Float.parseFloat(value);
                        break;
                    case "sklearnTopK":
                        this.sklearnTopK = Integer.parseInt(value);
                        break;
                    case "ruleFindMinimizeSwitch":
                        this.ruleFindMinimizeSwitch = Boolean.parseBoolean(value);
                        break;
                    case "ruleFindRatioEvidset":
                        this.ruleFindRatioEvidset = Float.parseFloat(value);
                        break;
                    case "rulefindRHSRate":
                        this.rulefindRHSRate = Float.parseFloat(value);
                        break;
                    case "sklearnScale":
                        this.sklearnScale = Float.parseFloat(value);
                        break;
                    case "sklearnAlpha":
                        this.sklearnAlpha = new BigDecimal(value).floatValue();
                        break;
                    case "sklearnMaxIter":
                        this.sklearnMaxIter = Integer.parseInt(value);
                        break;
                    case "L":
                        this.L = Integer.parseInt(value);
                        break;
                    case "groupSize":
                        this.groupSize = Integer.parseInt(value);
                        break;
                    default:
                        logger.info("parseConfig(): unknown config option: key=" + key + "value=" + value);
                        break;
                }
        }
    }


    public boolean isExecuteConstantPred(){
        return BASE_COLUMN_PREDICATE.equals(predicateType)
                || ONLY_CONSTANT_PREDICATE.equals(predicateType);
    }

    public boolean isExecuteNonConstantPred(){
        return BASE_COLUMN_PREDICATE.equals(predicateType)
                || ONLY_NO_CONSTANT_PREDICATE.equals(predicateType);
    }

    public boolean isExecuteUnionPred(){
        return UNION_PREDICATE.equals(predicateType);
    }

    /*
    predicate_type=3
    只生成常数谓词
     */
    public boolean isExecuteOnlyConstantPred(){
        return ONLY_CONSTANT_PREDICATE.equals(predicateType);
    }

    /*
    predicate_type=2
    只生成非常数谓词
     */
    public boolean isExecuteOnlyNonConstantPred(){
        return ONLY_NO_CONSTANT_PREDICATE.equals(predicateType);
    }

    public boolean isCrossColumnPredicate() {
        return CROSS_COLUMN_PREDICATE.equals(predicateType);
    }


}
