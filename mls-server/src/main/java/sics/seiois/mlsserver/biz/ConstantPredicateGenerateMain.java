package sics.seiois.mlsserver.biz;

import com.alibaba.fastjson.JSONObject;
import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.client.model.mls.*;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.model.PredicateExpressionModel;
import sics.seiois.mlsserver.service.impl.PredicateGenerateImpl;
import sics.seiois.mlsserver.utils.helper.LabelHelper;

import static sics.seiois.mlsserver.biz.SparkKmeansAlgorithm.*;

public class ConstantPredicateGenerateMain {
    private static final Logger logger = LoggerFactory.getLogger(ConstantPredicateGenerateMain.class);
    public final static String ROWID = "row_id";
    public final static String SEPARATOR = "_#@";
    public List<TableInfo> tableInfoList;

    public static void main(String[] args) {
        String tablesJson = getTestReqJson().toString();

        //参数解析
        JSONObject jsonObject = JSONObject.parseObject(tablesJson);

        JSONObject tableInfosJson = jsonObject.getJSONObject("tableInfos");
        TableInfos tableInfos = tableInfosJson.toJavaObject(TableInfos.class);

        for(int i=0; i<tableInfos.getTableInfoList().size(); i++){
            String tableName = tableInfos.getTableInfoList().get(i).getTableName();
            PredicateConfig predicateConfig = new PredicateConfig(File.separatorChar +"tmp"
                    + File.separatorChar +"predict.conf", false);
            String tableLocation = tableName + "." + predicateConfig+ ".csv";
            System.setProperty("hadoop.home.dir", "C:\\hadoop");
//        // 加载库文件
            System.load("C:\\hadoop\\bin\\hadoop.dll");

            //生成spark实例,并且创建表
            SparkSession spark = SparkSession.builder().appName("evidenceset").getOrCreate();
            Dataset<Row> ds = null;
//        if(predicateConfig.isORCTypeFile()) {
            ds = spark.read().orc(File.separatorChar +"data"
                    + File.separatorChar +"orc"
                    + File.separatorChar +"orc"+(i+2)
                    + File.separatorChar +"t"+(i+2)+".orc");//"\\tmp\\testFile\\t2.orc"
//            ds = spark.sql("SELECT * FROM t2");
//            ds.printSchema();
//        } else {
//            ds = spark.read().format("com.databricks.spark.csv").option("header", "true").load(tableLocation);
//
//        }
            ds.createOrReplaceTempView(tableName);
            ds = spark.sql("select * from t"+(i+2));
            ds.show();
//        generateConstantES(spark, tableInfo, predicateConfig);
        }

    }


    //获取多表列表
    public List<TableInfo> getTableInfoList(){
        return this.tableInfoList;
    }

    /**
     * 获取常数谓词的列名，和value
     * @param spark
     * @param tableInfo
     * @param config
     * @return [1]
     */
    public static List<Row> geneConstantList(SparkSession spark, TableInfo tableInfo, PredicateConfig config, boolean erRuleFinderFlag,
                                             String eidName) {
        //生成字符常数谓词
        String constantExtralSQL = geneConstantExtralSQL(tableInfo, config, erRuleFinderFlag, eidName);
        logger.info("####tConstantSQLStr:::"+constantExtralSQL);
        List<Row> constantRow = new ArrayList<>();
        if (constantExtralSQL.length() > 5) {
            constantRow = spark.sql(constantExtralSQL).collectAsList();
        }
        logger.info("####totalNum::tableConstantTable:" + constantRow.size());
        List<Row> array = new ArrayList<>();

        //选出数值字段
        StringBuffer fields = new StringBuffer();
        StringBuffer conditions = new StringBuffer();
        ArrayList<StructField> fieldType = new ArrayList<>();
        LinkedList<ColumnInfo> colList = new LinkedList<>();
        for(ColumnInfo col : tableInfo.getColumnList()){
            logger.info("####" + col.getColumnName() + " skip value:" + col.getSkip().booleanValue());
            if(isSkip(col)) {
                continue;
            }
            if(PredicateGenerateImpl.isNumberType(col.getColumnType())) {
                logger.info("####number col:" + col.getColumnName());
                colList.add(col);
                fields.append(col.getColumnName() + ",");
                conditions.append( " and " + col.getColumnName() + " is not null and "
                        + col.getColumnName() + " <> '' ");
                StructField field = DataTypes.createStructField(col.getColumnName()
                        , DataTypes.DoubleType, true);
                fieldType.add(field);
            }
        }

        if(fields.toString().length() > 1 && config.getKmeansCluster() >= 2) {
            //选出所有的数值字段的数据
            Dataset<Row> rowSet = spark.sql("select " + fields.substring(0, fields.length() - 1)
                    + " from " + tableInfo.getTableName() + " where 1=1 "
                    + conditions.toString());

            //先计算出最大最小值
            Map<String, double[]> maxMinMap = SparkKmeansAlgorithm.getMaxMinValForDataSet(rowSet
                    , colList.size());

            //构造0，1区间的标准化数据
            Dataset<Row> list = SparkKmeansAlgorithm.dataStandardization(spark, rowSet, fieldType, maxMinMap);

            //构造向量数据
            Dataset<Row> dataset = SparkKmeansAlgorithm.addVectorToFeature(list, fields);

            //使用数据进行聚类算法，得到预测函数
            KMeansModel kMeansModel = SparkKmeansAlgorithm.getKmeanCluster(dataset
                    , config.getKmeansCluster(), config.getKmeansMaxIterNum());

            //使用预测模型给数据进行归集
            Dataset<Row> transform = kMeansModel.transform(dataset);

            //使用归集后的数据按区间选出数值常数谓词
            SparkKmeansAlgorithm
                    .getNumberConstantVal(transform, colList, maxMinMap, array, config);
        }
        array.addAll(constantRow);
        return array;
    }

    /**
     * 字符串常数计算频率。
     * @param tableInfo
     * @return
     */
    private static String geneConstantExtralSQL(TableInfo tableInfo, PredicateConfig config, boolean erRuleFinderFlag, String eidName){
        StringBuffer newCol = new StringBuffer();
        String tableName = erRuleFinderFlag ? tableInfo.getTableName() + LabelHelper.LABELED_TABLE_NAME_SUFFIX : tableInfo.getTableName();

        for (ColumnInfo col : tableInfo.getColumnList()) {
            if(isSkip(col) || (erRuleFinderFlag && eidName.equals(col.getColumnName()))) {
                continue;
            }

            String alisName = col.getColumnName();
            if(PredicateGenerateImpl.isNumberType(col.getColumnType())){
                alisName += SEPARATOR + "Num";
                continue;
            }

            newCol.append(" UNION ALL ");

            newCol.append(" SELECT '").append(alisName).append("' AS name, t.val FROM (SELECT count(")
                    .append(col.getColumnName()).append(") * 100 / (select count(1) from ").append(tableName)
                    .append(") as avgCount,").append(col.getColumnName()).append(" AS val from ")
                    .append(tableName).append(" where ").append(col.getColumnName()).append(" is not NULL  and ")
                    .append(col.getColumnName()).append("<>''  group by ").append(col.getColumnName())
                    .append(")t where t.avgCount > ").append(config.getConstantFilterRatio())
                    .append(" AND t.avgCount < ").append(config.getConstantHighFilterRatio());
        }
        if(newCol.length() > " UNION ALL ".length()) {
            return newCol.substring(" UNION ALL ".length(), newCol.length());
        }
        return "";
    }

    public static Map<String, PredicateExpressionModel> genePredicateExpMap(List<Row> rows1
            , JoinInfo tableInfo, PredicateConfig predicateConfig, int begin){
        Map<String, PredicateExpressionModel> predicateInfos = new HashMap<>();

        //抽取常量。group by count > 阈值
        Map<String, ColumnInfo> colsMap = new HashMap<>();
        for (ColumnInfo col : tableInfo.getColumnList()) {
            if(isSkip(col)) {
                continue;
            }
            colsMap.put(col.getColumnName(), col);
        }
        String tableName = tableInfo.getTableName();

        Map<String, String> notEqNumExp = new HashMap<>();
        Map<String, String> eqNumExp = new HashMap<>();
        for(Map.Entry<String, String> entry : predicateConfig.getConstantNumOperators().entrySet()) {
            if("=".equals(entry.getKey())) {
                eqNumExp.put(entry.getKey(), entry.getValue());
            } else {
                notEqNumExp.put(entry.getKey(), entry.getValue());
            }
        }

        int count = begin;
        for (Row result : rows1) {
            System.out.println( result.toString());

            String colName = result.getString(0).split(SEPARATOR)[0];
            if(colsMap.containsKey(colName)){
                ColumnInfo col = colsMap.get(colName);
                Map<String, String> optionMap = null;
                colName = col.getColumnName();
                String constant = result.getString(1);

                if(PredicateGenerateImpl.isNumberType(col.getColumnType())){
                    if(result.getString(0).contains(SEPARATOR)){
                        optionMap = eqNumExp;
                    } else {
                        optionMap = notEqNumExp;
                    }
                } else {
                    optionMap = predicateConfig.getConstantStrOperators();
                }

                for(Map.Entry<String, String> entry : optionMap.entrySet()) {
                    String alias = "c" + count;
                    count++;

                    PredicateExpressionModel predicate = new PredicateExpressionModel();
                    predicate.setColumnName(alias);
                    predicate.setExplicitPredicateExpression(tableName + ".t0." +colName
                            + " " + entry.getKey() + " " + constant);
                    String calculateStr = "if(t0." + colName + entry.getValue() + "'" + constant
                            +"' and t1." + colName + entry.getValue() + "'" + constant
                            +"',1,0) ";
                    if(begin == 0) {
                        calculateStr ="if(ifnull(" + colName +",1)"
                                + entry.getValue() + "'" + constant +"',1,0)";
                    }
                    predicate.setCalcPredicateExpression(calculateStr);

                    logger.debug("predicate:::["+ predicate.getColumnName() + ","
                            + predicate.getExplicitPredicateExpression() + ","
                            + predicate.getCalcPredicateExpression());

                    predicateInfos.put(alias, predicate);
                }
            }
        }

        return predicateInfos;
    }

    public static List<String> genePredicateList(List<Row> rows1
            , TableInfo tableInfo, PredicateConfig predicateConfig){
        List<String> predicateInfos = new ArrayList<>();

        //抽取常量。group by count > 阈值
        Map<String, ColumnInfo> colsMap = new HashMap<>();
        for (ColumnInfo col : tableInfo.getColumnList()) {
            if(isSkip(col)) {
                continue;
            }
            colsMap.put(col.getColumnName(), col);
        }
        String tableName = tableInfo.getTableName();

        Map<String, String> notEqNumExp = new HashMap<>();
        Map<String, String> eqNumExp = new HashMap<>();
        for(Map.Entry<String, String> entry : predicateConfig.getConstantNumOperators().entrySet()) {
            if("=".equals(entry.getKey())) {
                eqNumExp.put(entry.getKey(), entry.getValue());
            } else {
                notEqNumExp.put(entry.getKey(), entry.getValue());
            }
        }

        for (Row result : rows1) {
            System.out.println( result.toString());

            String colName = result.getString(0).split(SEPARATOR)[0];
            if(colsMap.containsKey(colName)){
                ColumnInfo col = colsMap.get(colName);
                Map<String, String> optionMap = null;
                colName = col.getColumnName();
                String constant = result.getString(1);

                if(PredicateGenerateImpl.isNumberType(col.getColumnType())){
                    if(result.getString(0).contains(SEPARATOR)){
                        optionMap = eqNumExp;
                    } else {
                        optionMap = notEqNumExp;
                    }
                } else {
                    optionMap = predicateConfig.getConstantStrOperators();
                }

                for(Map.Entry<String, String> entry : optionMap.entrySet()) {
                    //常数谓词中的单引号需要进行转义，否则在笛卡尔积版本里面执行SQL会报错
                    predicateInfos.add(tableName + "->" + colName + ";" + constant.replace("'", "''") + ";" + entry.getKey());
                }
            }
        }

        return predicateInfos;
    }

    //测试数据
    public static RuleDiscoverExecuteRequest getTestReqJson(){
        RuleDiscoverExecuteRequest ruleRequest =
                new RuleDiscoverExecuteRequest();

        TableInfos tables = new TableInfos();

        TableInfo table = new TableInfo();
        table.setTableName("t2");
        table.setTableDataPath("/");

        ColumnInfo colID = new ColumnInfo();
        colID.setColumnName("id");
        colID.setColumnType("int8");
        colID.setModelStorePath("hdfs:///data/model/");
        List<FKMsg> fkMsgList = new ArrayList<>();
        FKMsg fkMsg = new FKMsg();
        fkMsg.setFkColumnName("id");
        fkMsg.setFkTableName("t3");
        fkMsgList.add(fkMsg);
        colID.setFkMsgList(fkMsgList);

        ColumnInfo colSn = new ColumnInfo();
        colSn.setColumnName("sn");
        colSn.setModelStorePath("hdfs:///data/model/");
        colSn.setColumnType("int8");

        ColumnInfo colName = new ColumnInfo();
        colName.setColumnName("name");
        colName.setModelStorePath("hdfs:///data/model/");

        ColumnInfo colCity = new ColumnInfo();
        colCity.setColumnName("city");
        colCity.setModelStorePath("hdfs:///data/model/");

        ColumnInfo zcode = new ColumnInfo();
        zcode.setColumnName("zcode");
        zcode.setColumnType("number");
        zcode.setModelStorePath("hdfs:///data/model/");
        zcode.setColumnType("int8");

        ColumnInfo addr = new ColumnInfo();
        addr.setColumnName("addr");
        addr.setModelStorePath("hdfs:///data/model/");

        ColumnInfo tel = new ColumnInfo();
        tel.setColumnName("tel");
        tel.setColumnType("int8");
        tel.setModelStorePath("hdfs:///data/model/");
        tel.setColumnType("int8");

        ColumnInfo birth = new ColumnInfo();
        birth.setColumnName("birth");
        birth.setModelStorePath("hdfs:///data/model/");

        ColumnInfo gender = new ColumnInfo();
        gender.setColumnName("gender");
        gender.setModelStorePath("hdfs:///data/model/");

        ColumnInfo product = new ColumnInfo();
        product.setColumnName("product");
        product.setModelStorePath("hdfs:///data/model/");



        List<ColumnInfo> list = new ArrayList<>();
        list.add(colID);
        list.add(colSn);
        list.add(colName);
        list.add(colCity);
        list.add(zcode);
        list.add(addr);
        list.add(tel);
        list.add(birth);
        list.add(gender);
        list.add(product);

        table.setColumnList(list);

        TableInfo tableFr = new TableInfo();
        tableFr.setTableName("t3");
        tableFr.setTableDataPath("/");

        ColumnInfo colID2 = new ColumnInfo();
        colID2.setColumnName("id");
        colID2.setColumnType("int8");
        colID2.setModelStorePath("hdfs:///data/model/");

        ColumnInfo colNom = new ColumnInfo();
        colNom.setColumnName("nom");
        colNom.setModelStorePath("hdfs:///data/model/");

        ColumnInfo colVille = new ColumnInfo();
        colVille.setColumnName("ville");
        colVille.setModelStorePath("hdfs:///data/model/");

        ColumnInfo adresse = new ColumnInfo();
        adresse.setColumnName("adresse");
        adresse.setModelStorePath("hdfs:///data/model/");

        ColumnInfo portable = new ColumnInfo();
        portable.setColumnName("portable");
        portable.setColumnType("int8");
        portable.setModelStorePath("hdfs:///data/model/");
        portable.setColumnType("int8");

        ColumnInfo date = new ColumnInfo();
        date.setColumnName("date de naissance");
        date.setModelStorePath("hdfs:///data/model/");

        ColumnInfo sexe = new ColumnInfo();
        sexe.setColumnName("sexe");
        sexe.setModelStorePath("hdfs:///data/model/");

        ColumnInfo produit = new ColumnInfo();
        produit.setColumnName("produit");
        produit.setModelStorePath("hdfs:///data/model/");


        List<ColumnInfo> listFr = new ArrayList<>();
        listFr.add(colID2);
        listFr.add(colNom);
        listFr.add(colVille);
        listFr.add(adresse);
        listFr.add(portable);
        listFr.add(date);
        listFr.add(sexe);
        listFr.add(produit);

        tableFr.setColumnList(listFr);


        List<TableInfo> listable = new ArrayList<>();
        listable.add(table);
        listable.add(tableFr);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("12");
        ruleRequest.setDimensionID("15");
        ruleRequest.setResultStorePath("/tmp/rule");
        ruleRequest.setCr("0.5");
        ruleRequest.setFtr("0.6");
        return ruleRequest;
    }

    public static boolean isSkip(ColumnInfo columnInfo) {
        return columnInfo.getSkip() || orderSkip(columnInfo);
    }

    public static boolean orderSkip(ColumnInfo columnInfo) {
//        if ("director".equals(columnInfo.getColumnName()) || "star_zh".equals(columnInfo.getColumnName())) {
//            return false;
//        }
        if(columnInfo.getColumnName().contains("id")){
            return true;
        }

        return false;
    }

}
