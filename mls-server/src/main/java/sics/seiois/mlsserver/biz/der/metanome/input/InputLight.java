package sics.seiois.mlsserver.biz.der.metanome.input;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.utils.MlsConstant;

import java.io.*;
import java.util.*;

public class InputLight implements Serializable {

    private static final long serialVersionUID = 5961087336068687061L;
    private static final Logger log = LoggerFactory.getLogger(InputLight.class);
    private static String JOIN_DELIMITER = MlsConstant.RD_JOINTABLE_SPLIT_STR;//"_AND_";
    private static String TBL_COL_DELIMITER = "__";

    // 所有行
    private int lineCount;

    // 每表的行
    private int[] lineCounts;

    // 每表的列
    private int[] colCounts;

    // 每张表每列的行数
    private int[] lineCounts_col;

    public static final String SAMPLE_RELATION_TID = "___";

    private String name;

    // 表名
    private List<String> names;

    private String taskId;

    // record the types of each columns
    public static String relationAttrDelimiter = "->";


    public InputLight(Input input) {
        // shallow copy
        this.lineCount = input.getLineCount();
        this.lineCounts = input.getLineCounts();
        this.colCounts = input.getColCounts();
        this.lineCounts_col = input.getLineCounts_col();
        this.name = input.getName();
        this.names = input.getNames();

    }

    public int getLineCount() {
        return lineCount;
    }

    public int getLineCount(int rid) {
        return lineCounts[rid];
    }

    public int[] getLineCounts_col() {
        return this.lineCounts_col;
    }

    public String getName(int rid) {
        return names.get(rid);
    }

    public String getRelationNameTID(Integer tid) {
        int tuple_id_start = 0;
        int tid_ = tid.intValue();
        for (int rid = 0; rid < colCounts.length; rid ++) {
            if (rid == 0) {
                if (tid_ >= 0 && tid_ < lineCounts[rid]) {
                    return names.get(rid);
                }
            } else {
                if (tid_ >= tuple_id_start && tid_ < (tuple_id_start + lineCounts[rid])) {
                    return names.get(rid);
                }
            }
            tuple_id_start += lineCounts[rid];
        }
        return names.get(names.size() - 1);
    }

    public int getTupleIDStart(String relation_name) {
        int tuple_id_start = 0;
        for (int rid = 0; rid < names.size(); rid++) {
            if (names.get(rid).equals(relation_name)) {
                return tuple_id_start;
            }
            tuple_id_start += lineCounts[rid];
        }
        return tuple_id_start;
    }

    public int getRowCount(String relation_name) {
        for (int rid = 0; rid < names.size(); rid++) {
            if (names.get(rid).equals(relation_name)) {
                return lineCounts[rid];
            }
        }
        return 0;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }

    public int[] getLineCounts() {
        return lineCounts;
    }

    public void setLineCounts(int[] lineCounts) {
        this.lineCounts = lineCounts;
    }

    public int[] getColCounts() {
        return colCounts;
    }

    public void setColCounts(int[] colCounts) {
        this.colCounts = colCounts;
    }

    public void setLineCounts_col(int[] lineCounts_col) {
        this.lineCounts_col = lineCounts_col;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }


    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

}


