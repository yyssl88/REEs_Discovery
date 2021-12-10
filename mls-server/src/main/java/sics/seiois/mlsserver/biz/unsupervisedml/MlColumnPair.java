package sics.seiois.mlsserver.biz.unsupervisedml;

import lombok.Getter;
import lombok.Setter;
import sics.seiois.mlsserver.utils.helper.MLUtil;


/**
 * Created by suyh on 2021/4/7.
 */
@Setter
@Getter
public class MlColumnPair {

    //模型名称，作为模型的唯一标识
    private String name;

    //模型阈值配置
    private Double threshold;

    //bocking的文件HDFS路径,一个列对一个文件。/tmp/rulefind/{taskID}/blocking/i
    private String bockingResultHdfsPath;

    //ml结果保存的文件HDFS路径,一个列对一个文件。/tmp/rulefind/{taskID}/mlResult/i_{modelName}
    private String mlResultHdfsPath;

    //模型的HDFS路径
    private String modelHdfsPath;

    //左边表名，列名，表的HDFS地址
    private String leftTableName;
    private String leftColumnName;
    private String leftTableHdfsPath;

    //右表名，列名，表的HDFS地址
    private String rightTableName;
    private String rightColumnName;
    private String rightTableHdfsPath;

    /**
     * 获取ML结果的前面部分内容,举例：
     * ML#ditto_single_org@0.9(tableA->title;tableA->title):
     * ML#ditto_single_org@0.9(tableA->title,author,women;tableA->title,author,women):
     * @return
     */
    public String getMlResultPrefix() {
        return "ML" + MLUtil.ML_PREDICATE_SEGMENT + name + MLUtil.SIMILAR_PREDICATE_SEGMENT + threshold.toString() + "(" + leftTableName + "->" + leftColumnName + ";" + rightTableName + "->" + rightColumnName + "):";
    }

    /**
     * 返回列对字符串,e.g:
     *  tableA->title,author,women;tableA->title,author,women
     * @return
     */
    public String getColumnPairString() {
        return leftTableName + "->" + leftColumnName + ";" + rightTableName + "->" + rightColumnName;
    }


    @Override
    public String toString() {
        return "MlColumnPair{" +
                "name='" + name + '\'' +
                ", threshold=" + threshold +
                ", bockingResultHdfsPath='" + bockingResultHdfsPath + '\'' +
                ", mlResultHdfsPath='" + mlResultHdfsPath + '\'' +
                ", modelHdfsPath='" + modelHdfsPath + '\'' +
                ", leftTableName='" + leftTableName + '\'' +
                ", leftColumnName='" + leftColumnName + '\'' +
                ", leftTableHdfsPath='" + leftTableHdfsPath + '\'' +
                ", rightTableName='" + rightTableName + '\'' +
                ", rightColumnName='" + rightColumnName + '\'' +
                ", rightTableHdfsPath='" + rightTableHdfsPath + '\'' +
                '}';
    }
}
