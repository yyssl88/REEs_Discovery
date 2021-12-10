package sics.seiois.mlsserver.service.impl;

import com.sics.seiois.client.model.mls.JoinInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.seiois.client.model.mls.ColumnInfo;
import com.sics.seiois.client.model.mls.TableInfo;
import sics.seiois.mlsserver.biz.ConstantPredicateGenerateMain;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.model.PredicateExpressionModel;

import java.util.*;

/***
 *
 * @author liuyong
 *
 */

public class PredicateGenerateImpl {
    private static final Logger logger = LoggerFactory.getLogger(PredicateGenerateImpl.class);
//    PredicateConfig predicateConfig;

    public static void main(String[] args) throws Exception {

        /* prepare tableInfo */
        JoinInfo tableInfo= new JoinInfo();
        tableInfo.setTableName("table1");

        List<ColumnInfo> columnList = new ArrayList<>();

        ColumnInfo cInfoName = new ColumnInfo();
        cInfoName.setColumnName("name");
        cInfoName.setColumnType("varchar(32)");
        //cInfo.setSkip(false);
        columnList.add(cInfoName);

        ColumnInfo cInfoAge = new ColumnInfo();
        cInfoAge.setColumnName("age");
        cInfoAge.setColumnType("int4");
        //cInfo.setSkip(false);
        columnList.add(cInfoAge);

        ColumnInfo cInfoPperf = new ColumnInfo();
        cInfoPperf.setColumnName("pperf");
        cInfoPperf.setColumnType("varchar(32)");
        //cInfo.setSkip(false);
        cInfoPperf.setModelId("123456");
        columnList.add(cInfoPperf);
        tableInfo.setColumnList(columnList);

        //PredicateConfig predicateConfig = new PredicateConfig(args[0]);
        PredicateConfig predicateConfig = new PredicateConfig();
        PredicateGenerateImpl predicateGenerate = new PredicateGenerateImpl();
        Map<String, PredicateExpressionModel> predicateSet = new LinkedHashMap<String, PredicateExpressionModel>();
        predicateSet = predicateGenerate.doPredicateGenerate(tableInfo, null, predicateConfig);
    }


    public static Boolean isStringType(String columnType) {
        //PG string type: varchar(n) char(n) text
        return (
                "STRING".equals(columnType.toUpperCase())
                || "VARCHAR".equals(columnType.toUpperCase())
                || "CHAR".equals(columnType.toUpperCase())
                || "TEXT".equals(columnType.toUpperCase())
                || (columnType.matches("^varchar\\([1-9][0-9]*\\)"))    //varchar(n)
                || (columnType.matches("^char\\([1-9][0-9]*\\)"))    //char(n)
        );
    }

    public static Boolean isNumberType(String columnType) {
        //PG number type: int2 int4 int8 decimal float4 float8 serial2 serial4 serial8
        return ((columnType.matches("^integer"))             //integer
                || (columnType.matches("^int[248]"))           //int2 int4 int8
                || ("decimal".equals(columnType))                    //decimal
                || (columnType.matches("^float[48]"))         //float4 float8
                || (columnType.matches("^serial[248]")));     //serial2 serial4 serial8
    }
    public Map<String, PredicateExpressionModel> doPredicateGenerate(JoinInfo tableInfo
            , List<Row> constantList, PredicateConfig config) {
        Integer columnSeq = 0;
        Map<String, PredicateExpressionModel> predicateSet = new LinkedHashMap<String, PredicateExpressionModel>();
        String tableName = tableInfo.getTableName();

//        if (config.isCrossColumnFlag()) {
//            logger.info("doPredicateDiscovery(): cross_column_predicate not supported in current version.");
//            return predicateSet;
//        }

        for (ColumnInfo columnInfo : tableInfo.getColumnList()) {
            if(ConstantPredicateGenerateMain.isSkip(columnInfo)) {
                continue;
            }

            String colType = columnInfo.getColumnType();
            String colName = columnInfo.getColumnName();

            //List<HashMap<String,String>> listFkMap = tableInfo.getFkList();
            //for(HashMap<String,String> map : listFkMap){
            Map<String,Set<String>> mapFk = tableInfo.getFkRelation();
            /*for(mapFk : mapFk){
                Map<String, String> optionMap = null;
                if (StringUtils.isNotEmpty(columnInfo.getModelId())) {
                    String modelId = columnInfo.getModelId();

                    PredicateExpressionModel predicateExpressionModelMEQ = new PredicateExpressionModel();
                    *//* e.g.:  ML(table1. t0.pperf, table1.t1.pperf) *//*
                    predicateExpressionModelMEQ.setExplicitPredicateExpression("ML( " + tableName + ".t0." + colName + "," + tableName + ".t1." + map.get(colName)+ " )");
                    *//* e.g.:  ML(123456,t0.pperf,t1.pperf) *//*
                    predicateExpressionModelMEQ.setCalcPredicateExpression("ML(" + modelId + ",t0." + colName + ",t1." + map.get(colName) + ")");
                    columnSeq++;
                    predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelMEQ);
                    break;

                } else if (isNumberType(colType)) {
                    optionMap = config.getNumberOperators();
//                geneNumberCal(columnSeq, predicateSet, tableName, colName);
                } else {
                    optionMap = config.getStringOperators();
//                geneStrCal(columnSeq, predicateSet, tableName, colName);
                }
                if (null != optionMap) {
                    for (Map.Entry<String, String> entry : optionMap.entrySet()) {
                        PredicateExpressionModel predicateExpressionModelSEQ = new PredicateExpressionModel();
                        *//* e.g.:  table1.t0.name == table1.t1.name *//*
                        predicateExpressionModelSEQ.setExplicitPredicateExpression(tableName + ".t0." + colName + " " + entry.getKey() + " " + tableName + ".t1." + map.get(colName));
                        *//* e.g.:  if(t0.name==t1.name,1,0) *//*
                        predicateExpressionModelSEQ.setCalcPredicateExpression("if(t0." + colName + "=" + "t1." + map.get(colName) + ",1,0)");
                        columnSeq++;
                        predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelSEQ);
                    }
                }
            }*/

            //if (!columnInfo.skip)
            Map<String, String> optionMap = null;
            if (StringUtils.isNotEmpty(columnInfo.getModelId())) {
                String modelId = columnInfo.getModelId();

                PredicateExpressionModel predicateExpressionModelMEQ = new PredicateExpressionModel();
                /* e.g.:  ML(table1. t0.pperf, table1.t1.pperf) */
                predicateExpressionModelMEQ.setExplicitPredicateExpression("ML( " + tableName + ".t0." + colName + "," + tableName + ".t1." + colName+ " )");
                /* e.g.:  ML(123456,t0.pperf,t1.pperf) */
                predicateExpressionModelMEQ.setCalcPredicateExpression("ML(" + modelId + ",t0." + colName + ",t1." + colName + ")");
                if(mapFk != null){
                    if(mapFk.get(colName) != null){
                        for (ColumnInfo col : tableInfo.getColumnList()){
                            if (mapFk.get(colName).equals(col.getColumnName())){
                                /* e.g.:  ML(table1. t0.pperf, table1.t1.pperf) */
                                predicateExpressionModelMEQ.setExplicitPredicateExpression("ML( " + tableName + ".t0." + colName + "," + tableName + ".t1." + mapFk.get(colName)+ " )");
                                /* e.g.:  ML(123456,t0.pperf,t1.pperf) */
                                predicateExpressionModelMEQ.setCalcPredicateExpression("ML(" + modelId + ",t0." + colName + ",t1." + mapFk.get(colName) + ")");
                            }
                        }
                    }
                }
                columnSeq++;
                predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelMEQ);
                break;

            } else if (isNumberType(colType)) {
                optionMap = config.getNumberOperators();
//                geneNumberCal(columnSeq, predicateSet, tableName, colName);
            } else {
                optionMap = config.getStringOperators();
//                geneStrCal(columnSeq, predicateSet, tableName, colName);
            }
            if (null != optionMap) {
                for (Map.Entry<String, String> entry : optionMap.entrySet()) {
                    PredicateExpressionModel predicateExpressionModelSEQ = new PredicateExpressionModel();
                    /* e.g.:  table1.t0.name == table1.t1.name */
                    predicateExpressionModelSEQ.setExplicitPredicateExpression(tableName + ".t0." + colName + " " + entry.getKey() + " " + tableName + ".t1." + colName);
                    /* e.g.:  if(t0.name <> '' and t1.name is not null and t1.name <> '' and t1.name is not null and t0.name = t1.name, 1, 0) */
                    String t0AddColumnName = "t0." + colName;
                    String t1AddColumnName = "t1." + colName;

                    //只要有一个为空，就返回0
                    String calcExpression = "if(" +
                            t0AddColumnName + " is not null and " +
                            t0AddColumnName + " <> '' and " +
                            t1AddColumnName + " is not null and " +
                            t1AddColumnName + " <> '' and " +
                            t0AddColumnName + entry.getKey() + t1AddColumnName +
                            ",1,0)";
                    predicateExpressionModelSEQ.setCalcPredicateExpression(calcExpression);
                    if(mapFk != null){
                        if(mapFk.get(colName) != null){
                            for (ColumnInfo col : tableInfo.getColumnList()){
                                if (mapFk.get(colName).equals(col.getColumnName())){
                                    /* e.g.:  table1.t0.name == table1.t1.name */
                                    predicateExpressionModelSEQ.setExplicitPredicateExpression(tableName + ".t0." + colName + " " + entry.getKey() + " " + tableName + ".t1." + mapFk.get(colName));
                                    /* e.g.:  if(t0.name <> '' and t1.name is not null and t1.name <> '' and t1.name is not null and t0.name = t1.name, 1, 0) */

                                    String t1FkAddColumnName = "t1." + mapFk.get(colName);
                                    //只要有一个为空，就返回0
                                    String calcFkExpression = "if(" +
                                            t0AddColumnName + " is not null and " +
                                            t0AddColumnName + " <> '' and " +
                                            t1FkAddColumnName + " is not null and " +
                                            t1FkAddColumnName + " <> '' and " +
                                            t0AddColumnName + entry.getKey() + t1FkAddColumnName +
                                            ",1,0)";
                                    predicateExpressionModelSEQ.setCalcPredicateExpression(calcFkExpression);
                                }
                            }
                        }
                    }
                    columnSeq++;
                    predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelSEQ);
                }
            }
        }

        //把生成的常数谓词加入到普通谓词map里面
        if(null != constantList) {
            columnSeq++;
            Map<String, PredicateExpressionModel> constantMap = ConstantPredicateGenerateMain.genePredicateExpMap(constantList,
                    tableInfo, config, columnSeq);

            for (Map.Entry<String, PredicateExpressionModel> entry : constantMap.entrySet()) {
                predicateSet.put(entry.getKey(), entry.getValue());
            }
        }

        return predicateSet;
    }


//    private void geneStrCal(Integer columnSeq, Map<String, PredicateExpressionModel> predicateSet
//            , String tableName, String colName) {
//        if (predicateConfig.isStringPredicateOperatorEQ()) {
//            PredicateExpressionModel predicateExpressionModelSEQ = new PredicateExpressionModel();
//            /* e.g.:  table1.t0.name == table1.t1.name */
//            predicateExpressionModelSEQ.setExplicitPredicateExpression(tableName + ".t0." + colName + " == " + tableName + ".t1." + colName);
//            /* e.g.:  if(t0.name==t1.name,1,0) */
//            predicateExpressionModelSEQ.setCalcPredicateExpression("if(t0." + colName + "=t1." + colName + ",1,0)");
//            columnSeq++;
//            predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelSEQ);
//        }
//
//        if (predicateConfig.isStringPredicateOperatorNEQ()) {
//            PredicateExpressionModel predicateExpressionModelSNEQ = new PredicateExpressionModel();
//            /* e.g.:  table1.t0.name <> table1.t1.name */
//            predicateExpressionModelSNEQ.setExplicitPredicateExpression(tableName + ".t0." + colName + " <> " + tableName + ".t1." + colName);
//            /* e.g.:  if(t0.name<>t1.name,1,0) */
//            predicateExpressionModelSNEQ.setCalcPredicateExpression("if(t0." + colName + "<>t1." + colName + ",1,0)");
//            columnSeq++;
//            predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelSNEQ);
//        }
//    }
//
//    private void geneNumberCal(Integer columnSeq, Map<String, PredicateExpressionModel> predicateSet
//            , String tableName, String colName) {
//        if (predicateConfig.isNumberPredicateOperatorEQ()) {
//            PredicateExpressionModel predicateExpressionModelNEQ1 = new PredicateExpressionModel();
//            /* e.g.:  table1.t0.age == table1.t1.age */
//            predicateExpressionModelNEQ1.setExplicitPredicateExpression(tableName + ".t0." + colName + " == " + tableName + ".t1." + colName);
//            /* e.g.:  if(t0.age == t1.age,1,0) */
//            predicateExpressionModelNEQ1.setCalcPredicateExpression("if(t0." + colName + "=t1." + colName + ",1,0)");
//            columnSeq++;
//            predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelNEQ1);
//        }
//        if (predicateConfig.isNumberPredicateOperatorNEQ()) {
//            PredicateExpressionModel predicateExpressionModelNNEQ = new PredicateExpressionModel();
//            /* e.g.:  table1.t0.age <> table1.t1.age */
//            predicateExpressionModelNNEQ.setExplicitPredicateExpression(tableName + ".t0." + colName + " <> " + tableName + ".t1." + colName);
//            /* e.g.:  if(t0.age <> t1.age,1,0) */
//            predicateExpressionModelNNEQ.setCalcPredicateExpression("if(t0." + colName + "<>t1." + colName + ",1,0)");
//            columnSeq++;
//            predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelNNEQ);
//        }
//
//        if (predicateConfig.isNumberPredicateOperatorGTE()) {
//            PredicateExpressionModel predicateExpressionModelNGTE = new PredicateExpressionModel();
//            /* e.g.:  table1.t0.age >= table1.t1.age */
//            predicateExpressionModelNGTE.setExplicitPredicateExpression(tableName + ".t0." + colName + " >= " + tableName + ".t1." + colName);
//            /* e.g.:  if(t0.age == t1.age,1,0) */
//            predicateExpressionModelNGTE.setCalcPredicateExpression("if(t0." + colName + ">=t1." + colName + ",1,0)");
//            columnSeq++;
//            predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelNGTE);
//        }
//
//        if (predicateConfig.isNumberPredicateOperatorLTE()) {
//            PredicateExpressionModel predicateExpressionModelNLTE = new PredicateExpressionModel();
//            /* e.g.:  table1.t0.age <= table1.t1.age */
//            predicateExpressionModelNLTE.setExplicitPredicateExpression(tableName + ".t0." + colName + " <= " + tableName + ".t1." + colName);
//            /* e.g.:  if(t0.age == t1.age,1,0) */
//            predicateExpressionModelNLTE.setCalcPredicateExpression("if(t0." + colName + "<=t1." + colName + ",1,0)");
//            columnSeq++;
//            predicateSet.put("c" + columnSeq.toString(), predicateExpressionModelNLTE);
//        }
//    }
}
