package sics.seiois.mlsserver.biz;

import com.alibaba.fastjson.JSONObject;
import com.sics.seiois.client.model.mls.ColumnInfo;
import com.sics.seiois.client.model.mls.TableInfo;

import com.sics.seiois.client.model.mls.TableInfos;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import sics.seiois.mlsserver.model.PredicateConfig;


public class SparkKmeansAlgorithm {
    private static final Logger logger = LoggerFactory.getLogger(SparkKmeansAlgorithm.class);
    public static final String MAX_VAL = "maxVal";
    public static final String MIN_VAL = "minVal";
    public static final String FEATURE_COLUMN = "features";

    public static void main(String[] args) {
        String tablesJson = ConstantPredicateGenerateMain.getTestReqJson().toString();

        //参数解析
        JSONObject jsonObject = JSONObject.parseObject(tablesJson);
        JSONObject tableInfosJson = jsonObject.getJSONObject("tableInfos");
        TableInfos tableInfos = tableInfosJson.toJavaObject(TableInfos.class);

        for(int i=0; i<tableInfos.getTableInfoList().size(); i++){
            String tableName=tableInfos.getTableInfoList().get(i).getTableName();
            String tableLocation=File.separatorChar+"tmp"+File.separatorChar+tableName+".csv";

            TableInfo tableInfo=tableInfos.getTableInfoList().get(i);

            SparkSession spark=SparkSession.builder()
                    .master("local")
                    .appName("evidenceset")
                    .getOrCreate();

            Dataset<Row> ds=spark.read().format("com.databricks.spark.csv")
                    .option("header","true").load(tableLocation);

            //ds.createOrReplaceTempView(tableName);

            kMeansNotNull(ds,spark,tableInfo,tableName);
        }

        /*StringBuffer fields = new StringBuffer();
        StringBuffer conditions = new StringBuffer();
        ArrayList<StructField> fieldType = new ArrayList<>();
        LinkedList<ColumnInfo> colList = new LinkedList<>();
        for(ColumnInfo col : tableInfo.getColumnList()){
            if(PredicateGenerateImpl.isNumberType(col.getColumnType())) {
                colList.add(col);
                fields.append(col.getColumnName() + ",");
                conditions.append( " and " + col.getColumnName() + " is not null and "
                        + col.getColumnName() + " <> '' ");
                StructField field = DataTypes.createStructField(col.getColumnName()
                        , DataTypes.DoubleType, true);
                fieldType.add(field);
            }
        }*/

        /*//选出所有的数值字段的数据
        Dataset<Row> rowSet = spark.sql("select "+ fields.substring(0, fields.length() - 1)
                +" from " + tableInfo.getTableName()
                + " where 1=1 " + conditions.toString());
        rowSet.show();

        Map<String, double[]> maxMinMap = getMaxMinValForDataSet(rowSet, colList.size());
        Dataset<Row> list = dataStandardization(spark, rowSet, fieldType, maxMinMap);
        Dataset<Row> dataset = addVectorToFeature(list, fields);
        KMeansModel kMeansModel = getKmeanCluster(dataset, 2, 1000);


        //输出误差平方和
        double WSSSE = kMeansModel.computeCost(dataset);
        logger.info("误差平方和 "+WSSSE);
        //输出聚类中心
        Vector[] centers = kMeansModel.clusterCenters();
        logger.info("聚类中心: ");
        for (Vector center : centers) {
            logger.info(center.toString());
        }
        //输出预测结果
        Dataset<Row> transform = kMeansModel.transform(dataset);
        transform.show();

        List<Row> constantRow = getNumberConstantVal(transform, colList, maxMinMap);

        for(Row row : constantRow) {
            logger.info(row.getString(0) + ":" + row.get(1));
        }*/

    }

    public static void kMeansNotNull(Dataset<Row> ds, SparkSession spark, TableInfo tableInfo, String tableName) {
        /*//获得数据表ds
        StringBuffer caseStr = new StringBuffer();
        for(ColumnInfo col : tableInfo.getColumnList()){
            caseStr.append(col.getColumnName() + ",");
        }
        String presql = "select " + caseStr.substring(0, caseStr.length() - 1) + " from " + tableName;
        Dataset<Row> ds = spark.sql(presql);
        ds.createOrReplaceGlobalTempView("kmeanNull");*/

        //获得数据表ds
        int count = 0;
        StringBuffer caseStr = new StringBuffer();
        StringBuffer fields = new StringBuffer();
        for (ColumnInfo col : tableInfo.getColumnList()) {
            count++;
            caseStr.append("(case when " + col + " isnull then 0 else 1) AS C" + count + "," + col.getColumnName() + ",");
            fields.append("C" + count + ",");
        }
        String presql = "select " + caseStr.substring(0, caseStr.length() - 1) + " from " + tableName;
        Dataset<Row> dataset = spark.sql(presql);
        dataset.createOrReplaceGlobalTempView("kmeanNull");

        //对数据表ds进行kmean计算
        dataset = addVectorToFeature(dataset, fields);
        KMeansModel kMeansModel = getKmeanCluster(dataset, 2, 1000);
        Dataset<Row> transform = kMeansModel.transform(dataset);//获得计算kmean后的数据表transform

        //获得计算占空比后的综合数据表predict
        StringBuffer casePre = new StringBuffer();
        for (ColumnInfo col : tableInfo.getColumnList()) {
            casePre.append("count (case when " + col + " = 0 then 1 else 0) / count (1)" + ",");
        }
        String sqlpre = "select " + casePre.substring(0, casePre.length() - 1) + " from " + transform + "group by predict";
        Dataset<Row> predict = spark.sql(sqlpre);
        logger.info(String.valueOf(predict));

        //获得去空值之后的原始数据表kmeanData
        int clusterNum = 1;
        List<Dataset<Row>> kmeanData = new ArrayList<>();
        for (Row row : predict.collectAsList()) {
            StringBuffer sql = new StringBuffer("select ");
            for (int index = 0; index < row.size(); index++) {
                if (row.getDouble(index) < 0.5) {
                    ColumnInfo col = tableInfo.getColumnList().get(index);
                    sql.append(col.getColumnName() + ",");
                }
            }
            String exSql = sql.substring(0, casePre.length() - 1) + " from transform where predict = " + clusterNum;
            Dataset<Row> kMeanData = spark.sql(exSql);
            kmeanData.add(kMeanData);
            clusterNum++;
            logger.info("kMeanData"+String.valueOf(kMeanData));
        }

        StringBuffer fieldsaf = new StringBuffer();
        ArrayList<StructField> fieldType = new ArrayList<>();
        LinkedList<ColumnInfo> colList = new LinkedList<>();
        for(ColumnInfo col : tableInfo.getColumnList()){
            colList.add(col);
            fields.append(col.getColumnName() + ",");
            StructField field = DataTypes.createStructField(col.getColumnName()
                    , DataTypes.DoubleType, true);
            fieldType.add(field);
        }

        PredicateConfig predicateConfig = new PredicateConfig();
        for(int i = 0; i < predicateConfig.getKmeansCluster();i++){
            Map<String, double[]> maxMinMap = getMaxMinValForDataSet(kmeanData.get(i), kmeanData.get(i).columns().length);
            Dataset<Row> list = dataStandardization(spark, kmeanData.get(i), fieldType, maxMinMap);
            Dataset<Row> result = addVectorToFeature(list, fieldsaf);
            KMeansModel kMeans = getKmeanCluster(result, 2, 1000);

            //输出预测结果
            Dataset<Row> transformResult = kMeans.transform(result);
//            transform.show();

            List<Row> constantRow = getNumberConstantVal(transformResult, colList, maxMinMap, predicateConfig);

            for (Row row : constantRow) {
                logger.info(row.getString(0) + ":" + row.get(1));
            }
        }
        /*Map<String, double[]> maxMinMap = getMaxMinValForDataSet(kmeanData.get(1), kmeanData.get(1).columns().length);
        Dataset<Row> list = dataStandardization(spark, kmeanData.get(1), fieldType, maxMinMap);
        Dataset<Row> result = addVectorToFeature(list, fieldsaf);
        KMeansModel kMeans = getKmeanCluster(result, 2, 1000);

        //输出预测结果
        Dataset<Row> transformResult = kMeans.transform(result);
        transform.show();

        List<Row> constantRow = getNumberConstantVal(transformResult, colList, maxMinMap);

        for (Row row : constantRow) {
            logger.info(row.getString(0) + ":" + row.get(1));
        }*/
    }

    /**
     * 使用预测结果输出常数区间
     * @param predictSet 预测结果集
     * @param colList 数值列信息
     * @param maxMinMap 最大最小值
     * @param predicateConfig 配置项参数
     * @return 返回常数谓词区间值
     */
    public static List<Row> getNumberConstantVal(Dataset<Row> predictSet
            , LinkedList<ColumnInfo> colList, Map<String, double[]> maxMinMap
            , List<Row> resultSet, PredicateConfig predicateConfig){
        Map<String, String> minAgg = new HashMap<>();
        Map<String, String> maxAgg = new HashMap<>();

        for(ColumnInfo col : colList){
            maxAgg.put(col.getColumnName(),"max");
            minAgg.put(col.getColumnName(),"min");
        }

        Dataset<Row> maxSet = predictSet.groupBy("prediction").agg(maxAgg);
//        maxSet.show();

        Dataset<Row> minSet = predictSet.groupBy("prediction").agg(minAgg);
//        minSet.show();

        int rowIndex = 0;
        double[] minArr = maxMinMap.get(MIN_VAL);
        double[] maxArr = maxMinMap.get(MAX_VAL);
        List<Row> rows = new ArrayList<>();
        for (ColumnInfo col : colList) {
            for (Row row : maxSet.collectAsList()) {
                Double val = row.getDouble(rowIndex + 1);

                val = val * (maxArr[rowIndex] - minArr[rowIndex]) + minArr[rowIndex];
                rows.add(RowFactory.create(col.getColumnName(), String.valueOf(val.longValue())));
            }
            for(Row row : minSet.collectAsList()){
                Double val = row.getDouble(rowIndex+1);
                val = val * (maxArr[rowIndex] - minArr[rowIndex]) + minArr[rowIndex];
                rows.add(RowFactory.create(col.getColumnName(), String.valueOf(val.longValue())));
            }
            rowIndex++;
        }

        System.out.println("ConstantInterval():" +predicateConfig.getConstantInterval());
        System.out.println("KmeansCluster():" + predicateConfig.getKmeansCluster());
        System.out.println("before NumberConstants:");
        for(Row row:rows){
            System.out.println(row.mkString());
        }
		// 用（maxArr-minArr）*0.1得到带宽，计算要去掉resultSet中的哪些元素
        Boolean useConstantInterval = predicateConfig.getUseConstantInterval();
        if (!useConstantInterval) {
            resultSet.addAll(rows);
        } else {
            // 常数区间。step为步长，去掉步长以内的常数
            rowIndex = 0;
            List<Row> intervalRows = new ArrayList<>();
            for (ColumnInfo col : colList) {
                Double interval = (maxArr[rowIndex] - minArr[rowIndex]) * ((predicateConfig.getConstantInterval())/100.0);
                rowIndex++;
                String columnName = col.getColumnName();
                ArrayList<Long> values = new ArrayList<Long>();
                for (Row row : rows) {
                    if (row.getString(0).equals(columnName)) {
                        Long value = Long.parseLong(row.getString(1));
                        values.add(value);
                    }
                }
                Collections.sort(values);
                if (values.size() > 0) {
                    Long prevalue = values.get(0);
                    intervalRows.add(RowFactory.create(columnName, String.valueOf(prevalue)));
                    for (int i = 1; i < values.size(); i++) {
                        if (prevalue + interval < values.get(i)) {
                            intervalRows.add(RowFactory.create(columnName, String.valueOf(values.get(i))));
                            prevalue = values.get(i);
                        }
                    }
                }
            }
            resultSet.addAll(intervalRows);
        }
        System.out.println("\nConstantInterval():" + predicateConfig.getConstantInterval());
        System.out.println("\nKmeansCluster():" + predicateConfig.getKmeansCluster());
        System.out.println("\nNumberConstants:");
        for(Row row:resultSet){
            System.out.println(row.mkString());
        }


        return resultSet;
    }


    public static List<Row> getNumberConstantVal(Dataset<Row> predictSet
            , LinkedList<ColumnInfo> colList, Map<String, double[]> maxMinMap, PredicateConfig predicateConfig){
        List<Row> constantRow = new ArrayList<>();
        return getNumberConstantVal(predictSet, colList, maxMinMap, constantRow, predicateConfig);
    }

    /**
     * 从数据集中计算出各列的最值
     * @param rowSet 数据集
     * @param colSize 列信息
     * @return 返回最值列表
     */
    public static Map<String, double[]> getMaxMinValForDataSet(Dataset<Row> rowSet
            , int colSize){
        Map<String, double[]> maxMinMap = new HashMap<>();
        double[] minArr = new double[colSize];
        double[] maxArr = new double[colSize];

        int rowIndex = 0;
        //在数据中选出最大最小值
        for(Row row : rowSet.collectAsList()) {
            for(int colIndex = 0; colIndex < row.size(); colIndex++){
                try {
                    double val = Double.valueOf(row.getString(colIndex));

                    if (rowIndex == 0) {
                        minArr[colIndex] = val;
                        maxArr[colIndex] = val;
                    } else {
                        if (minArr[colIndex] > val) {
                            minArr[colIndex] = val;
                        }
                        if (maxArr[colIndex] < val) {
                            maxArr[colIndex] = val;
                        }
                    }
                }catch (Exception e) {
                    logger.info("<<< error number:" + row.get(colIndex));
                    continue;
                }

            }
            rowIndex++;
        }

        maxMinMap.put(MAX_VAL, maxArr);
        maxMinMap.put(MIN_VAL, minArr);

        return maxMinMap;
    }

    /**
     * 处理数据并使用最大最小值将所有值标准化为0到1的区间上
     * @param sparkSession sparksession
     * @param rowSet 输入的训练集
     * @param fieldType 列信息
     * @param maxMinMap 最大最小值
     * @return 返回标准化后的数据集
     */
    public static Dataset<Row> dataStandardization(SparkSession sparkSession
            , Dataset<Row> rowSet, ArrayList<StructField> fieldType
            , Map<String, double[]> maxMinMap){

        double[] minArr = maxMinMap.get(MIN_VAL);
        double[] maxArr = maxMinMap.get(MAX_VAL);


        //构造出向量
        JavaRDD<Row> rowRDD = rowSet.toJavaRDD();
        rowRDD = rowRDD.map(record -> {
            Object [] tmp = new Object[record.size()];
            for(int index = 0; index < record.size(); index++) {
                try {
                    double val = Double.valueOf(record.getString(index));
                    tmp[index] = (val - minArr[index])
                            / (maxArr[index] - minArr[index]);
//                    logger.debug("test rdd:" + val + "|" + tmp[index] + "|" + minArr[index] + "|" + maxArr[index]);

                }catch (Exception e) {
                    logger.info("<<< error number:" + index);
                    tmp[index] = 0.0d;
                }
            }

            return new GenericRow(tmp);
        });

        StructType schema = DataTypes.createStructType(fieldType);

        Dataset<Row> list = sparkSession.createDataFrame(rowRDD, schema);
//        list.show();

        return list;
    }

    /**
     * 处理数据并增加向量作为Kmean的特征值
     * 这里的数据集使用了标准化为0，1区间的数据
     * @param list 数据集
     * @param fields 列信息
     * @return 返回包含向量值的数据级
     */
    public static Dataset<Row> addVectorToFeature(Dataset<Row> list, StringBuffer fields){
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(fields.substring(0, fields.length() - 1).split(","))
                .setOutputCol(FEATURE_COLUMN);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{assembler});
        PipelineModel model = pipeline.fit(list);
        Dataset<Row> dataset = model.transform(list);
//        dataset.show();

        return dataset;
    }

    /**
     * 训练Kmean并得到预测模型
     * @param dataset 训练数据集
     * @return 预测模型
     */
    public static KMeansModel getKmeanCluster(Dataset<Row> dataset, Integer kClusterNum, Integer maxIterNum){

        KMeans kmeans = new KMeans().setK(kClusterNum).setMaxIter(maxIterNum);
        KMeansModel kMeansModel = kmeans.fit(dataset).setFeaturesCol(FEATURE_COLUMN)
                .setPredictionCol("prediction");

        return kMeansModel;
    }

}
