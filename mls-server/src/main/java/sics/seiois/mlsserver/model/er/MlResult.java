package sics.seiois.mlsserver.model.er;

import com.clearspring.analytics.util.Pair;

import java.util.List;

import lombok.Data;

/**
 *
 * 处理ml和相似度的结果。
 * ML#cosine@0.9(tableA->title,author,women;tableA->title,author,women):10383|10383;10108|10108;
 * Created by suyh on 2021/4/13.
 */
@Data
public class MlResult {

    //前缀部分:ML#cosine@0.9(tableA->title,author,women;tableA->title,author,women)
    private String suffix;


    //操作符
    private String op;
    //ML模型名称和阈值部分:cosine@0.9
    private String mlOption;
    private String leftTable;
    private String leftColumn;
    private String rightTable;
    private String rightColumn;
    private List<List<Integer>> pairList;
}
