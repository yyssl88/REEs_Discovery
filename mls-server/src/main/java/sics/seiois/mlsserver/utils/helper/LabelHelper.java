package sics.seiois.mlsserver.utils.helper;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import sics.seiois.mlsserver.model.er.LabelInfo;

/**
 * Created by friendsyh on 2020/11/2. 标注功能的实现帮助类
 */
public class LabelHelper {

    private static final Logger logger = LoggerFactory.getLogger(LabelHelper.class);

    //标注表名的后缀
    public static final String LABELED_TABLE_NAME_SUFFIX = "_marking";

    /**
     * 读取标注csv文件
     * @param labellingFilePath csv文件存储路径
     * @return labelInfoList
     */
    public static List<LabelInfo> loadLabelCsv(String labellingFilePath) {
        List<LabelInfo> labelInfoList = new ArrayList<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(labellingFilePath))) {
            String line = null;// 循环读取每行
            while ((line = reader.readLine()) != null) {
                String[] row = line.split(",");
                LabelInfo lable = new LabelInfo();
                lable.setLeftTableName(row[0]);
                lable.setLeftRowID(row[1]);
                lable.setRightTableName(row[2]);
                lable.setRightRowID(row[3]);

                labelInfoList.add(lable);
            }
        } catch (Exception e) {
            logger.error("loadLabelCsv exception,labellingFilePath={}", labellingFilePath, e);
        }

        return labelInfoList;
    }

    /**
     * 读取标注csv文件加载到对象里面
     * @param hdfs
     * @param hdfsFilePath csv文件存储路径
     * @return labelInfoList
     */
    public static List<LabelInfo> loadLabelCsv(FileSystem hdfs, String hdfsFilePath) throws Exception{
        List<LabelInfo> labelInfoList = new ArrayList<>();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(hdfsFilePath))))) {
            String line;// 循环读取每行
            while ((line = reader.readLine()) != null) {
                String[] row = line.split(",");
                LabelInfo lable = new LabelInfo();
                lable.setLeftTableName(row[0]);
                lable.setLeftRowID(row[1]);
                lable.setRightTableName(row[2]);
                lable.setRightRowID(row[3]);

                labelInfoList.add(lable);
            }
        } catch (Exception e) {
            logger.error("loadLabelCsv exception,labellingFilePath={}", hdfsFilePath, e);
            throw e;
        }

        return labelInfoList;
    }

    /**
     * 把标注数据根据tableName进行group并且去重,用于构建小数据集
     * @param labelInfoList
     * @return 返回示例：{"table1":["rowID3","rowID4","rowID9"]}
     */
    public static Map<String, List<String>> lableGroupByTalbe(List<LabelInfo> labelInfoList) {
        Map<String, List<String>> rowIDGroupByTableMap = new HashMap<>();
        Map<String, HashSet<String>> tableMap = new HashMap<>();

        for(LabelInfo labelInfo : labelInfoList) {
            //处理左边的表
            if(!tableMap.containsKey(labelInfo.getLeftTableName())) {
                tableMap.put(labelInfo.getLeftTableName(), new HashSet<>());
            }
            tableMap.get(labelInfo.getLeftTableName()).add(labelInfo.getLeftRowID());

            //处理右边的表
            if(!tableMap.containsKey(labelInfo.getRightTableName())) {
                tableMap.put(labelInfo.getRightTableName(), new HashSet<>());
            }
            tableMap.get(labelInfo.getRightTableName()).add(labelInfo.getRightRowID());
        }

        //map里面的value set转list
        for(Map.Entry<String, HashSet<String>> entry : tableMap.entrySet()) {
            rowIDGroupByTableMap.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }

        return rowIDGroupByTableMap;
    }

    /**
     * 写文件到HDFS指定路径
     * @param hdfs
     * @param tableMatrix
     * @param hdfsCsvFilePath
     */
    public static void writeCsvTable(FileSystem hdfs, String[][] tableMatrix, String hdfsCsvFilePath) {
        logger.info("#### [标注过程] write csvFilePath={}", hdfsCsvFilePath);

        try(FSDataOutputStream predicateOut = hdfs.create(new Path(hdfsCsvFilePath))) {
            int rowLength = tableMatrix.length;
            for(int i = 0; i < rowLength; i++) {
                String rowStr = String.join(",", tableMatrix[i]);
                predicateOut.write((rowStr + "\n").getBytes());
                predicateOut.flush();
            }
            logger.info("#### [标注过程] write success! csvFilePath={}", hdfsCsvFilePath);
        } catch (IOException e) {
            logger.error("#### [标注过程] write csvFilePath={} error", hdfsCsvFilePath);
        }
    }

    public static Map<String, String> readCsvTable(String csvFilePath) {
        logger.info("#### [标注过程] load csvFilePath={}", csvFilePath);
        Map<String, String> csvMap = new HashMap<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String key = line.substring(0, line.indexOf(","));
                csvMap.put(key, line);
            }
        } catch (Exception e) {
            logger.error("readCsvTable error,filePath={}", csvFilePath, e);
        }

        return csvMap;
    }

    public static Map<String, String> readCsvTable(FileSystem hdfs, String hdfsCsvFilePath) {
        logger.info("#### [标注过程] load csvFilePath={}", hdfsCsvFilePath);
        Map<String, String> csvMap = new HashMap<>();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(hdfsCsvFilePath))))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String key = line.substring(0, line.indexOf(","));
                csvMap.put(key, line);
            }
        } catch (Exception e) {
            logger.error("readCsvTable error,filePath={}", hdfsCsvFilePath, e);
        }

        return csvMap;
    }

    /**
     * 写文件到HDFS指定路径
     * @param hdfs
     * @param tableData
     * @param hdfsCsvFilePath
     */
    public static void writeCsvTable(FileSystem hdfs, Map<String, String> tableData, String hdfsCsvFilePath) {
        logger.info("#### [标注过程] write csvFilePath={}", hdfsCsvFilePath);

        try(FSDataOutputStream predicateOut = hdfs.create(new Path(hdfsCsvFilePath))) {
            //先把header部分写入,作为csv的header
            String header = tableData.get("row_id");
            predicateOut.write((header + "\n").getBytes());
            predicateOut.flush();

            //再写入非header部分
            for (Map.Entry<String, String> entry : tableData.entrySet()) {
                if(!"row_id".equals(entry.getKey())) {
                    predicateOut.write((entry.getValue() + "\n").getBytes());
                    predicateOut.flush();
                }
            }
        } catch (IOException e) {
            logger.error("#### [标注过程] write csvFilePath={} error", hdfsCsvFilePath);
        }
    }
}
