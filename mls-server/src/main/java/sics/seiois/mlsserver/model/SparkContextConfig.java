package sics.seiois.mlsserver.model;

import com.alibaba.fastjson.JSONObject;
import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.client.model.mls.ColumnInfo;
import com.sics.seiois.client.model.mls.TableInfo;
import com.sics.seiois.client.model.mls.TableInfos;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import sics.seiois.mlsserver.biz.mock.RuleFindRequestMock;
import sics.seiois.mlsserver.biz.der.metanome.RuleContext;
import sics.seiois.mlsserver.biz.mock.RuleFindRequestMock;

/**
 * Created by friendsyh on 2020/8/13.
 */

@Setter
@Getter
@NoArgsConstructor
public class SparkContextConfig implements Serializable {

    //配置读取，Anaconda3 的bin目录的地址。默认为:/opt/Anaconda3/bin
    private String anaconda3BinPath;

    //配置读取，Anaconda3 的python环境名称
    private String anaconda3EnvName;

    //配置读取，规则发现sklearn步骤是否需要打开。默认为true:打开
    private boolean sklearnSwitch;

    //配置读取，sklearn步骤的threshold配置参数。默认为0.0，设定值在[0,0.2]之间的浮点数
    private float sklearnThreshold;

    //配置读取，sklearn步骤的topK配置参数。默认为90，设定值在[50,100]之间的整数
    private int sklearnTopK;

    //配置读取，sklearn步骤的scale配置参数。默认为100
    private float sklearnScale;

    //配置读取，规则发现的minimize开关
    private boolean ruleFindMinimizeSwitch;

    //配置读取，谓词重叠数据比率。默认为1，设定值为[0,1]的浮点数
    private double ruleFindRatioEvidset;

    //配置读取，RHS包含数据占总数据的比率。默认-1代表取容错率的值，设定值为[0,1]的浮点数
    private double rulefindRHSRate;

    /***********************以下为规则挖掘的参数**********************/
    private String taskId;

    //规则挖掘的表名数组
    private String[] tableNames;

    //规则挖掘的列名数组
    private String[] columnList;

    //规则挖掘的列信息
    Map<String, String> columnTypeMap;

    //规则挖掘容错率
    private double ftr;

    //规则挖掘覆盖率
    private double cr;

    private String level;

    private int groupSize;

    private RuleContext ruleContext;

    public SparkContextConfig(PredicateConfig config, RuleDiscoverExecuteRequest request, TableInfos tableInfos) {
        this.anaconda3BinPath = config.getAnaconda3BinPath();
        this.anaconda3EnvName = config.getAnaconda3EnvName();
        this.sklearnSwitch = config.getSklearnSwitch();
        this.sklearnThreshold = config.getSklearnThreshold();
        this.sklearnTopK = config.getSklearnTopK();
        this.sklearnScale = config.getSklearnScale();
        this.ruleFindMinimizeSwitch = config.getRuleFindMinimizeSwitch();
        this.ruleFindRatioEvidset = config.getRuleFindRatioEvidset();
        this.rulefindRHSRate = config.getRulefindRHSRate();
        this.groupSize = config.getGroupSize();

        this.taskId = request.getTaskId();
        this.tableNames = fillTableNames(tableInfos);
        this.columnList = fillColumnNames(tableInfos);
        this.columnTypeMap = fillColumnTypeMap(tableInfos);
        this.ftr = Double.valueOf(request.getFtr());
        this.cr = Double.valueOf(request.getCr());
    }


    public SparkContextConfig(TableInfos tableInfos) {
        this.tableNames = fillTableNames(tableInfos);
        this.columnList = fillColumnNames(tableInfos);
        this.columnTypeMap = fillColumnTypeMap(tableInfos);
    }

    public SparkContextConfig(PredicateConfig config, JSONObject jsonObject, TableInfos tableInfos, int partitionNum) {
        this.anaconda3BinPath = config.getAnaconda3BinPath();
        this.anaconda3EnvName = config.getAnaconda3EnvName();
        this.sklearnSwitch = config.getSklearnSwitch();
        this.sklearnThreshold = config.getSklearnThreshold();
        this.sklearnTopK = config.getSklearnTopK();
        this.sklearnScale = config.getSklearnScale();
        this.ruleFindMinimizeSwitch = config.getRuleFindMinimizeSwitch();
        this.ruleFindRatioEvidset = config.getRuleFindRatioEvidset();
        this.rulefindRHSRate = config.getRulefindRHSRate();
        this.groupSize = config.getGroupSize();

        this.taskId = jsonObject.getString("taskId");
        this.tableNames = fillTableNames(tableInfos);
        this.columnList = fillColumnNames(tableInfos);
        this.columnTypeMap = fillColumnTypeMap(tableInfos);
//        this.ftr = jsonObject.getDouble("ftr") / partitionNum;
//        this.cr = jsonObject.getDouble("cr") / partitionNum;
        this.ftr = jsonObject.getDouble("ftr");
        this.cr = jsonObject.getDouble("cr");
    }

    /**
     * 获取该表表名。用","隔开。比如id,name,age,city
     * @param tableInfos
     * @return
     */
    private String[] fillTableNames(TableInfos tableInfos) {
        List<String> tableNameList = new ArrayList<>();
        for(TableInfo tableInfo : tableInfos.getTableInfoList()){
            tableNameList.add(tableInfo.getTableName());
        }

        return tableNameList.toArray(new String[tableNameList.size()]);
    }

    /**
     * 获取该表的列名字符串。用","隔开。比如id,name,age,city
     * @param tableInfos
     * @return
     */
    private String[] fillColumnNames(TableInfos tableInfos) {
        List<String> tableColumnNameList = new ArrayList<>();
        for(TableInfo tableInfo : tableInfos.getTableInfoList()){
            List<String> columnNameList = new ArrayList<>();
            for(ColumnInfo column : tableInfo.getColumnList()) {
                if(column.getSkip()){
                    continue;
                }
                columnNameList.add(column.getColumnName());
            }
            String columnNameString = StringUtils.join(columnNameList, ",");
            tableColumnNameList.add(columnNameString);
        }

        return tableColumnNameList.toArray(new String[tableColumnNameList.size()]);
    }

    /**
     * 获取列和列的map信息。返回如下：
     * key:tableName->columnName
     * value:Type类型。目前只支持三种类型。String对应String，LONG对应整数型，NUMERIC对应浮点型
     * @param tableInfos
     * @return
     */
    private Map<String, String> fillColumnTypeMap(TableInfos tableInfos){
        HashMap<String, String> colTypeMap = new HashMap<>();
        for(TableInfo tableInfo : tableInfos.getTableInfoList()){
            String tableName = tableInfo.getTableName();
            for(ColumnInfo column : tableInfo.getColumnList()) {
                if(column.getSkip()){
                    continue;
                }
                colTypeMap.put(tableName + "->" + column.getColumnName(), column.getColumnType());
            }
        }

        return colTypeMap;
    }

    @Override
    public String toString() {
        return "SparkContextConfig{" +
                "anaconda3BinPath='" + anaconda3BinPath + '\'' +
                ", anaconda3EnvName='" + anaconda3EnvName + '\'' +
                ", sklearnSwitch=" + sklearnSwitch +
                ", sklearnThreshold=" + sklearnThreshold +
                ", sklearnTopK=" + sklearnTopK +
                ", sklearnScale='" + sklearnScale + '\'' +
                ", ruleFindMinimizeSwitch=" + ruleFindMinimizeSwitch +
                ", ruleFindRatioEvidset=" + ruleFindRatioEvidset +
                ", rulefindRHSRate=" + rulefindRHSRate +
                ", taskId='" + taskId + '\'' +
                ", tableNames=" + Arrays.toString(tableNames) +
                ", columnList=" + Arrays.toString(columnList) +
                ", columnTypeMap=" + columnTypeMap +
                ", ftr=" + ftr +
                ", cr=" + cr +
                ", level=" + level +
                '}';
    }


    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public RuleContext getRuleContext() {
        return ruleContext;
    }

    public void setRuleContext(RuleContext ruleContext) {
        this.ruleContext = ruleContext;
    }

    public static void main(String[] args) throws Exception {
//        String tableJson = RuleFindRequestMock.mockRuleFindReqest().toString();
        String tableJson = RuleFindRequestMock.mockRuleFindReqest("").toString();

        JSONObject jsonObject = JSONObject.parseObject(tableJson);
        String taskId = jsonObject.getString("taskId");
        JSONObject tableInfosJson = jsonObject.getJSONObject("tableInfos");
        TableInfos tableInfos = tableInfosJson.toJavaObject(TableInfos.class);

        SparkContextConfig sparkContextConfig = new SparkContextConfig();
        String[] allTableNames = sparkContextConfig.fillTableNames(tableInfos);
        System.out.println(Arrays.toString(allTableNames));

        String[] allColumnNames = sparkContextConfig.fillColumnNames(tableInfos);
        System.out.println(allColumnNames.length);
        System.out.println(Arrays.toString(allColumnNames));

        Map<String, String> colTypeMap = sparkContextConfig.fillColumnTypeMap(tableInfos);
        System.out.println(colTypeMap.toString());

    }
}
