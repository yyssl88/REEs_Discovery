package sics.seiois.mlsserver.utils.helper;

import com.sics.seiois.client.dto.request.mls.ColumnPair;
import com.sics.seiois.client.model.mls.TableColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.unsupervisedml.MlColumnPair;
import sics.seiois.mlsserver.enums.ModelTypeEnum;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.model.er.MlResult;

/**
 * Created by friendsyh on 2020/11/2.
 * ML帮助类
 */
public class MLUtil {

    private static final Logger logger = LoggerFactory.getLogger(MLUtil.class);

    //ML谓词索引文件的分隔符
    public static final String ML_PREDICATE_SEGMENT = "#";

    //相似度谓词的分隔符
    public static final String SIMILAR_PREDICATE_SEGMENT = "@";

    public static final String RELATION_ATTR_DELIMITER = "->";

    /**
     * 把字符串转对象，字符串如下：
     * ML#cosine@0.9(tableA->title,author,women;tableA->title,author,women):10383|10383;10108|10108;
     * @param line
     * @param input
     * @return
     */
    public static MlResult fromString2Object(String line, Input input) {
        MlResult mlResult = new MlResult();
        String [] data = line.split(":");

        //处理前面一部分。去掉所有的空格
        String suffix = data[0].replace(" ", "");
        mlResult.setSuffix(suffix);

        //得到ML字符串
        String op = suffix.split(MLUtil.ML_PREDICATE_SEGMENT)[0];
        mlResult.setOp(op);
        //得到cosine@0.9(tableA->title,author,women;tableA->title,author,women)字符串
        String mloptionAndColumn = suffix.split(MLUtil.ML_PREDICATE_SEGMENT)[1];
        //得到cosine@0.9
        String mlOption = mloptionAndColumn.split("[(]")[0];
        mlResult.setMlOption(mlOption);
        //得到tableA->title,author,women;tableA->title,author,women。左边表和列和右边表和列进行处理
        String column = mloptionAndColumn.split("[(]")[1].replace(")", "");

        logger.info("####ml column str:{}", column);
        mlResult.setLeftTable(column.split(";")[0].split(RELATION_ATTR_DELIMITER)[0]);
        mlResult.setLeftColumn(column.split(";")[0].split(RELATION_ATTR_DELIMITER)[1]);
        mlResult.setRightTable(column.split(";")[1].split(RELATION_ATTR_DELIMITER)[0]);
        mlResult.setRightColumn(column.split(";")[1].split(RELATION_ATTR_DELIMITER)[1]);

        //处理后面的pair对。10383|10383;10108|10108;
        int leftTupleIdStart = input.getTupleIDStart(mlResult.getLeftTable());
        int rightTupleIdStart = input.getTupleIDStart(mlResult.getRightTable());
        List<List<Integer>> pairList = new ArrayList<>();
        if(data.length > 1) {
            String pairArray[] = data[1].replace(" ", "").split(";");
            for (int i = 0; i < pairArray.length; i++) {
                //一个pair对,list永远只有两个元素
                List<Integer> tidList = new ArrayList<>();
                tidList.add(Integer.valueOf(pairArray[i].split("[|]")[0]) + leftTupleIdStart);
                tidList.add(Integer.valueOf(pairArray[i].split("[|]")[1]) + rightTupleIdStart);
                pairList.add(tidList);
            }
        }
        mlResult.setPairList(pairList);


        return mlResult;
    }
}
