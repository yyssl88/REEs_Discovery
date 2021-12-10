package sics.seiois.mlsserver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args )
    {
        String tableJson = "{\"cr\":\"0.5\",\"dimensionID\":\"15\",\"ftr\":\"0.6\",\"otherParam\":\"\",\"resultStorePath\":\"/tmp/rule\",\"tableInfo\":{\"columnList\":[{\"columnName\":\"id\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"},{\"columnName\":\"sn\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"},{\"columnName\":\"name\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"},{\"columnName\":\"city\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"},{\"columnName\":\"zcode\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"},{\"columnName\":\"addr\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"},{\"columnName\":\"tel\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"},{\"columnName\":\"birth\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"},{\"columnName\":\"gender\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"},{\"columnName\":\"product\",\"columnType\":\"\",\"modelId\":\"\",\"modelStorePath\":\"hdfs:///data/model/\"}],\"tableDataPath\":\"/data/\",\"tableName\":\"t2\"},\"taskId\":\"12\"}";

        SparkSession spark= SparkSession.builder().appName("spark-test").master("local[3]").getOrCreate();
        Dataset<Row> ds = spark.read().format("com.databricks.spark.csv").option("header", "true").load("t2.csv");
//        Dataset<Row> result=spark.read().json("employees.json");
        ds.createOrReplaceTempView("t2");
        ds.show();
        ds.printSchema();
        spark.stop();
    }
}

