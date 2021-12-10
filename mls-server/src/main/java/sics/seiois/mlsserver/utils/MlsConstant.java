package sics.seiois.mlsserver.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Created by friendsyh on 2020/5/21.
 */
@Component
public class MlsConstant {

    private static final Logger logger = LoggerFactory.getLogger(MlsConstant.class);

    //spark-client的目镜
    public static final String SPARK_HOME = "SPARK_HOME";

    private static final String SPARK_CLINET_CONFIG_PATH = "/conf/spark-defaults.conf";

    public static final String RD_JOINTABLE_SPLIT_STR = "_AND_";

    public static final String ML_TABLE_SPLIT_STR = "_@#_";

//    //配置读取，spark home 的目录。加载的时候需要并且spark-client的配置文件也在{sparkHome}/conf/spark-env.sh 这个里面
//    private static String sparkHome;
//
//    //配置读取，Anaconda3 的bin目录的地址。默认为:/opt/Anaconda3/bin
//    private static String anaconda3BinPath;
//
//    //配置读取，Anaconda3 的python环境名称
//    private static String anaconda3EnvName;
//
//
//    //配置读取，规则发现sklearn步骤是否需要打开。默认为true:打开
//    private static boolean sklearnSwitch;
//
//    //配置读取，sklearn步骤的threshold配置参数。默认为0.0，设定值在[0,0.2]之间的浮点数
//    private static String sklearnThreshold;
//
//    //配置读取，sklearn步骤的topK配置参数。默认为90，设定值在[50,100]之间的整数
//    private static String sklearnTopK;
//
//    //配置读取，sklearn步骤的scale配置参数。默认为100
//    private static String sklearnScale;
//
//    //配置读取，规则发现的minimize开关
//    private static boolean ruleFindMinimizeSwitch;
//
//    //配置读取，谓词重叠数据比率。默认为1，设定值为[0,1]的浮点数
//    private static double ruleFindRatioEvidset;
//
//    //配置读取，RHS包含数据占总数据的比率。默认-1代表取容错率的值，设定值为[0,1]的浮点数
//    private static double rulefindRHSRate;
//
//
//    @PostConstruct
//    public void init(){
//    }
//
//    @Value("${sparkHome}")
//    public void setSparkHome(String sparkConf) {
//        sparkHome = sparkConf;
//    }
//
//    public static String getSparkHome() {
//        return sparkHome;
//    }
//
//    @Value("${sklearnThreshold}")
//    public void setSklearnThreshold(String threshold) {
//        sklearnThreshold = threshold;
//    }
//
//    public static String getSklearnThreshold() {
//        return sklearnThreshold;
//    }
//
//    @Value("${sklearnTopK}")
//    public void setSklearnTopK(String topK) {
//        sklearnTopK = topK;
//    }
//
//    public static String getSklearnTopK() {
//        return sklearnTopK;
//    }
//
//    @Value("${sklearnScale}")
//    public void setSklearnScale(String scale) {
//        sklearnScale = scale;
//    }
//
//    public static String getSklearnScale() {
//        return sklearnScale;
//    }
//
//    @Value("${sklearnSwitch}")
//    public void setSklearnSwitch(boolean skSwitch) {
//        sklearnSwitch = skSwitch;
//    }
//
//    public static boolean getSklearnSwitch() {
//        return sklearnSwitch;
//    }
//
    public static String getSparkClientConfigPath() {
        return "/usr/hdp/3.1.0.0-78/spark2" + SPARK_CLINET_CONFIG_PATH;
    }
//
//    @Value("${anaconda3BinPath}")
//    public void setAnaconda3BinPath(String anacodaPath) {
//        anaconda3BinPath = anacodaPath;
//    }
//
//    public static String getAnaconda3BinPath() {
//        return anaconda3BinPath;
//    }
//
//    @Value("${anaconda3EnvName}")
//    public void setAnaconda3EnvName(String anacodaName) {
//        anaconda3EnvName = anacodaName;
//    }
//
//    public static String getAnaconda3EnvName() {
//        return anaconda3EnvName;
//    }
//
//    @Value("${ruleFindMinimizeSwitch}")
//    public void setRuleFindMinimizeSwitch(boolean minimizeSwitch) {
//        ruleFindMinimizeSwitch = minimizeSwitch;
//    }
//
//    public static boolean getRuleFindMinimizeSwitch() {
//        return ruleFindMinimizeSwitch;
//    }
//
//    @Value("${ruleFindRatioEvidset}")
//    public void setRuleFindRatioEvidset(double ratioEvidset) {
//        ruleFindRatioEvidset = ratioEvidset;
//    }
//
//    public static double getRuleFindRatioEvidset() {
//        return ruleFindRatioEvidset;
//    }
//
//    @Value("${rulefindRHSRate}")
//    public void setRulefindRHSRate(double rhsRate) {
//        rulefindRHSRate = rhsRate;
//    }
//
//    public static double getRulefindRHSRate() {
//        return rulefindRHSRate;
//    }
}
