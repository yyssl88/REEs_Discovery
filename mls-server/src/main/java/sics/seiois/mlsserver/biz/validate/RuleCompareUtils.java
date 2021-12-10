package sics.seiois.mlsserver.biz.validate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sics.seiois.mlsserver.service.impl.EvidenceGenerateMain;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by suyh on 2020/12/30.
 */
public class RuleCompareUtils {

    private final static String PREDICAT_SPLIT_CHAR = ",";
    private static final Logger logger = LoggerFactory.getLogger(RuleCompareUtils.class);

    public static void main(String[] args) {
        String fullPath = "F:/tmp/sample.txt";
        String samplePath = "F:/tmp/orig.txt";
        String rltPaht = "D:/data/validate/order_compare_result.txt";
        String rltPaht1 = "F:/tmp/casorg/rule_result_mt20210323162542090551497/b730ab5272334560ac61fff07bca6cc8";

//        String fullPath = "D:/data/validate/adults_data_full.txt";
//        String samplePath = "D:/data/validate/adults_data_sample.txt";
//
//        String fullPath = "D:/data/validate/airports_full.txt";
//        String samplePath = "D:/data/validate/airports_sample.txt";
//
//        String fullPath = "D:/data/validate/hospital_full.txt";
//        String samplePath = "D:/data/validate/hospital_sample.txt";
//
//        String fullPath = "D:/data/validate/tax_10w_full.txt";
//        String samplePath = "D:/data/validate/tax_10w_sample.txt";
//
//        String fullPath = "D:/data/validate/tax_100w_full.txt";
//        String samplePath = "D:/data/validate/tax_100w_sample.txt";

//        readRuleFromORCFile(rltPaht1);
        List<String> rlt = compareRule(fullPath, samplePath);
        logger.info("compare is same:{}, differents:{}", rlt.size()<1, rlt);
//        writeRule2LocalFile(rlt, rltPaht);

    }

    /**
     * 比较sample中的规则在full中是不是都存在，返回不存在的列表并且进行打印
     * 比如sample中有规则X1,X2->Y, 如果在full中有X1,X2->Y 或者有 X1->Y 或者有X2->Y，那么就是存在的。否则就是不存在
     *
     * 规则格式如下：
     * adult_data(t0) ^ adult_data(t1) ^  adult_data.t0.capital_loss == 0 -> adult_data.t0.capital_loss == adult_data.t1.capital_loss
     * 或者标准REE语法:adult_data(t0) ^ adult_data(t1) ^  t0.capital_loss = '0' -> t0.capital_loss = t1.capital_loss
     *
     * @param fullPath  标准的规则
     * @param samplePath 需要进行比较的规则
     * @return
     */
    private static List<String> compareRule(String fullPath, String samplePath) {
        List<String> notContains = new ArrayList<>();

        List<String> fullRuleList;
        List<String> sampleRuleList;

        if(fullPath.endsWith(EvidenceGenerateMain.CSV_TYPE_SUFFIX) || fullPath.endsWith("txt")) {
            fullRuleList = readRuleFromFile(fullPath);
        } else {
            fullRuleList = readRuleFromORCFile(fullPath);
        }

        if(samplePath.endsWith(EvidenceGenerateMain.CSV_TYPE_SUFFIX) || samplePath.endsWith("txt")) {
            sampleRuleList = readRuleFromFile(samplePath);
        } else {
            sampleRuleList = readRuleFromORCFile(samplePath);
        }

        return compareRule(fullRuleList, sampleRuleList);
    }

    public static List<String> compareRule(List<String> mainRules, List<String> compareRules) {
        List<String> notContains = new ArrayList<>();

        for(String sampleRule : compareRules) {
            String tmpSampleRule = sampleRule;
            sampleRule = sampleRule.replace("⋀", "^").substring(sampleRule.contains("(t1)") ? sampleRule.indexOf("(t1)") + 8 : sampleRule.indexOf("(t0)") + 8);
            String xSample = sampleRule.split("->")[0];
            String ySample = "->" + sampleRule.split("->")[1].trim();
            if(ySample.contains(".eid_")) {
                ySample = transformEidName(ySample);
            }
            List<String> sampleXList = predicateStr2List(xSample);

            boolean containsFlag = false;
            for(String fullRule : mainRules) {
                fullRule = fullRule.replace("⋀", "^").substring(fullRule.contains("(t1)") ? fullRule.indexOf("(t1)") + 8 : fullRule.indexOf("(t0)") + 8);
                String yFull = "->" + fullRule.split("->")[1].trim();
                if(yFull.contains(".eid_")) {
                    yFull = transformEidName(yFull);
                }
                if(yFull.equals(ySample)) {
                    String xFull = fullRule.split("->")[0];
                    List<String> fullXList = predicateStr2List(xFull);
                    if(sampleXList.containsAll(fullXList)) {
                        containsFlag = true;
                        break;
                    }
                }
            }

            if(!containsFlag) {
                System.out.println(tmpSampleRule);
                notContains.add(tmpSampleRule);
            }

        }

        return notContains;
    }

    private static String transformEidName(String eidString) {
//        logger.info("source predicate String:{}", eidString);
        String lStr = eidString.split("=")[0];
        String eidName = lStr.trim().split("\\.eid_")[1];
        String newEidStr = eidString.replace(".eid_" + eidName, ".eid");
//        logger.info("new predicate String:{}", newEidStr);
        return newEidStr;
    }

    private static List<String> predicateStr2List(String predicateString) {
       List<String> rlt = Arrays.stream(predicateString.split("\\^")).sorted().collect(Collectors.toList());
       List<String> pList = new ArrayList<>();
       for(String pStr : rlt) {
           if(pStr.contains(".eid_")) {
               pStr = transformEidName(pStr);
           }
           pList.add(pStr.trim());
       }

       return pList;
    }

    private static void writeRule2LocalFile(List<String> rules, String ruleStorePath) {
        if (rules == null || rules.size() < 1) {
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(ruleStorePath))) {
            for (String ruleResult : rules) {
                writer.write(ruleResult + "\r\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<String> readRuleFromFile(String ruleStorePath) {
        List<String> readList = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(ruleStorePath))) {
            String line = "";
            while((line = reader.readLine()) != null) {
                readList.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return readList;
    }

    public static List<String> readRuleFromORCFile(String path) {
        List<String> rules = new ArrayList<>();
        try {
            Configuration conf = new Configuration();
            Reader reader = OrcFile.createReader(new Path(path),
                    OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();

            while(rows.nextBatch(batch)) {
                BytesColumnVector stringVector = (BytesColumnVector) batch.cols[1];
                for(int r=0; r < batch.size; r++) {
                    rules.add(stringVector.toString(r));
//                    logger.info(">>>> rule {}:{}", r, stringVector.toString(r));
                }
            }
            rows.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rules;
    }

}
