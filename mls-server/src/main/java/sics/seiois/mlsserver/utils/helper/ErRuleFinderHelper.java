package sics.seiois.mlsserver.utils.helper;

import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.client.model.mls.ColumnInfo;
import com.sics.seiois.client.model.mls.TableInfo;
import com.sics.seiois.client.model.mls.TableInfos;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.collection.Seq;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.model.er.LabelInfo;

/**
 * Created by friendsyh on 2020/11/2.
 * Er规则发现流程
 */
public class ErRuleFinderHelper {

    private static final Logger logger = LoggerFactory.getLogger(ErRuleFinderHelper.class);

    public static final String EID_NAME_PREFIX = "eid_";

    /**
     *  根据标注数据获取标注好的tableInfos
     * @param spark
     * @param hdfs
     * @param request
     * @return
     */
    public static TableInfos getMarkedTableInfos(SparkSession spark, FileSystem hdfs, RuleDiscoverExecuteRequest request) throws Exception {
        String taskId = request.getTaskId();
        String eidName = request.getErRuleConfig().getEidName();
        TableInfos tableInfos = request.getTableInfos();
        //处理标注数据,存成对象并且按照表名进行分组
        List<LabelInfo> labelInfos = LabelHelper.loadLabelCsv(hdfs, request.getErRuleConfig().getLabellingFilePath());
        Map<String, List<String>> tableMarkedMap = LabelHelper.lableGroupByTalbe(labelInfos);
        logger.info("#### read label count:{},table count:{}", labelInfos.size(), tableMarkedMap.size());

        //构建新的tableInfos信息
        TableInfos markedTableInfos = new TableInfos();
        List<TableInfo> markedTableList = new ArrayList<>();
        markedTableInfos.setTableInfoList(markedTableList);

        //每个表的数据信息。每个表用二维数组进行存储
        Map<String, String[][]> tableMatrixMap = new HashMap<>();
        for(TableInfo tableInfo : tableInfos.getTableInfoList()) {
            //如果该表没有标注数据，不用生成新的表
            String tableName = tableInfo.getTableName();
            List<String> rowIDList = tableMarkedMap.get(tableName);
            if(rowIDList == null || rowIDList.size() < 1) {
                logger.info("#### erRuleFinder: {} have no marked data", tableName);
                continue;
            }

            //获取各个表的标注的rowID的List,转成一个dataset。只有row_id一个列
            Dataset<String> dsLabelTmp = spark.createDataset(rowIDList, Encoders.STRING());
            Dataset<Row> dsLabel = spark.read().csv(dsLabelTmp).toDF("row_id");

//            logger.info("#### dsLabel: {}", dsLabel.collectAsList());
            //读取原表数据生成dataset
            Dataset<Row> dsOldTable = spark.read().format("com.databricks.spark.csv").option("header", "true").load(tableInfo.getTableDataPath());

            //原表和标注数据进行join得到标注数据,数据量大大减少了很多。
            Dataset<Row> dsNew;
//            if(dsOldTable.count() < dsLabel.count() * 3) {
//                Column joinExprs = new Column("row_id");
//                dsNew = dsOldTable.join(dsLabel, joinExprs, "left");
//            } else {
                dsNew = dsOldTable.join(dsLabel, "row_id");
//            }

//            logger.info("#### dsNew: {}", dsNew.collectAsList());
            String markingTableName = tableName + LabelHelper.LABELED_TABLE_NAME_SUFFIX;
            dsNew.createOrReplaceTempView(markingTableName);
            logger.info("#### erRuleFinder: join {} and rowID success.rowIDLis={},tableSize after join={}", tableName, rowIDList, dsNew.count());

            //复制row_id列到新增加的列eidName
            Dataset<Row> dsWithEidName = dsNew.withColumn(eidName, dsNew.col("row_id"));

            //把表的信息存入二维数组里面
            String[][] tableMatrix = datasetCsv2Matrix(dsWithEidName);

            logger.info("#### erRuleFinder table:{} tableMatrix:{}", tableName, tableMatrix);
            tableMatrixMap.put(tableName, tableMatrix);

            //复制一个TableInfo对象
            markedTableList.add(copyTable(tableInfo, eidName));
        }
        logger.info("#### erRuleFinder tableDataMap.size:{},labelInfos:{}", tableMatrixMap.size(), labelInfos);

        //标注过程。对相同的实体标注为同一内容
        fillSameEidName(tableMatrixMap, labelInfos);

        //把标注好的数据写入HDFS的CVS中用于规则发现,并且更新tableInfo的存储路径为新的HDFS路径
        for(TableInfo tableInfo : markedTableList) {
            String tableName = tableInfo.getTableName();
            String[][] tableMatrix = tableMatrixMap.get(tableName);
            String markedCsvHdfsPath = PredicateConfig.MLS_TMP_HOME + taskId + "/marked/" + tableName + ".csv";
            tableInfo.setTableDataPath(markedCsvHdfsPath);

            ColumnInfo eidCol = new ColumnInfo();
            eidCol.setColumnName(eidName);
            eidCol.setColumnType("varchar(20)");
            eidCol.setSkip(false);
            tableInfo.getColumnList().add(eidCol);
            LabelHelper.writeCsvTable(hdfs, tableMatrix, markedCsvHdfsPath);
        }

        return markedTableInfos;
    }

    /**
     * 把dataset对象转为二维数组对象
     * @param dsTable
     * @return
     */
    private static String[][] datasetCsv2Matrix(Dataset<Row> dsTable) {
        logger.info("#### ds2Matrix start.ds.size={}", dsTable.count());
        List<Row> rowList = dsTable.collectAsList();
        String[] columns = dsTable.columns();
        int columnLength = columns.length;
        int lineSize = rowList.size() + 1;

        //把表头写入矩阵第一行
        String[][] tableMatrix = new String[lineSize][columnLength];
        for(int j = 0; j < columnLength; j++) {
            tableMatrix[0][j] = columns[j];
        }
        //表的数据部分写入二维数组
        for(int i = 1; i < lineSize; i++) {
            for(int j = 0; j < columnLength; j++) {
                String value = (String)rowList.get(i-1).get(j);
                //对‘,’进行转义为中文字符,因为csv都是用该符号进行分割的
                tableMatrix[i][j] = StringUtils.isEmpty(value) ? value : value.replace(",", "，");
            }
        }

        return tableMatrix;
    }

    /**
     * 对象复制
     * @param oldTable
     * @param eidName
     * @return
     */
    private static TableInfo copyTable(TableInfo oldTable, String eidName) {
        //构建新的tableInfo信息
        TableInfo markedTable = new TableInfo();

        List<ColumnInfo> newColumnList = new ArrayList<>();
        for(ColumnInfo column : oldTable.getColumnList()) {
            newColumnList.add(column);
        }
        ColumnInfo eidColumn = new ColumnInfo();
        eidColumn.setColumnName(eidName);
        eidColumn.setColumnType("varchar(50)");
        eidColumn.setSkip(true);
        newColumnList.add(eidColumn);

        markedTable.setTableName(oldTable.getTableName());
        markedTable.setColumnList(newColumnList);

        return markedTable;
    }

    /**
     * 标注过程。对于两个表中相同的列，eidName字段标注为同一个字符串 'S+Index'
     * @param tableMatrixMap 每个表都是一个二维数组
     * @param labelInfos
     */
    private static void fillSameEidName(Map<String, String[][]> tableMatrixMap, List<LabelInfo> labelInfos) {
        int index = 1;
        for(LabelInfo label : labelInfos) {
            String[][] leftTable = tableMatrixMap.get(label.getLeftTableName());
            int leftColumnLength = leftTable[0].length;
            String[] leftLine = null;
            for(int i = 1; i < leftTable.length; i++) {
                if(leftTable[i][0].equals(label.getLeftRowID())) {
                    leftLine = leftTable[i];
                    break;
                }
            }

            String[][] rightTable = tableMatrixMap.get(label.getRightTableName());
            String[] rightLine = null;
            int rightColumnLength = rightTable[0].length;
            for(int i = 1; i < rightTable.length; i++) {
                if(rightTable[i][0].equals(label.getRightRowID())) {
                    rightLine = rightTable[i];
                    break;
                }
            }

            //左边的有已经标记。右边也需要标记
            if(leftLine[leftColumnLength - 1] != null && leftLine[leftColumnLength - 1].contains("S")) {
                rightLine[rightColumnLength - 1] = leftLine[leftColumnLength - 1];
            } else if(rightLine[rightLine.length - 1] != null && rightLine[rightLine.length - 1].contains("S")) {  //右边的有已经标记，左边也需要标记
                leftLine[leftColumnLength - 1] = rightLine[rightColumnLength - 1];
            } else { //两边都标记
                leftLine[leftColumnLength - 1] = "S" + index;
                rightLine[rightColumnLength - 1] = "S" + index;
                index++;
            }
        }
    }

    /**
     * 标注过程。对于两个表中相同的列，eidName字段标注为同一个字符串 'S+Index'
     * @param tableDataMap 表数据
     * @param labelInfos
     */
    private static void fillSameEidNameOld(Map<String, Map<String, String>> tableDataMap, List<LabelInfo> labelInfos) {
        int index = 1;
        for(LabelInfo label : labelInfos) {
            Map<String, String> leftTableCsvMap = tableDataMap.get(label.getLeftTableName());
            String leftRow = leftTableCsvMap.get(label.getLeftRowID());
            int leftIndex = leftRow.lastIndexOf(",");
            String leftEidValue = leftRow.substring(leftIndex + 1);
            String leftOtherValue = leftRow.substring(0, leftIndex + 1);

            Map<String, String> rightTableCsvMap = tableDataMap.get(label.getRightTableName());
            String rightRow = rightTableCsvMap.get(label.getRightRowID());
            int rightIndex = rightRow.lastIndexOf(",");
            String rightEidValue = rightRow.substring(rightIndex + 1);
            String rightOtherValue = rightRow.substring(0, rightIndex + 1);

            //左边的有已经标记。右边也需要标记
            if(leftEidValue.contains("S")) {
                rightTableCsvMap.put(label.getRightRowID(), rightOtherValue + "S" + index);
            } else if(rightEidValue.contains("S")) {  //右边的有已经标记，左边也需要标记
                leftTableCsvMap.put(label.getLeftRowID(), leftOtherValue + "S" + index);
            } else { //两边都标记
                leftTableCsvMap.put(label.getLeftRowID(), leftOtherValue + "S" + index);
                rightTableCsvMap.put(label.getRightRowID(), rightOtherValue + "S" + index);
            }

            index++;
        }

    }
}
