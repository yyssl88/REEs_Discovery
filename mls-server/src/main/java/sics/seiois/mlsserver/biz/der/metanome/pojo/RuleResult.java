package sics.seiois.mlsserver.biz.der.metanome.pojo;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Created by friendsyh on 2020/6/2.
 */

public class RuleResult implements Writable, Serializable {

    private static final long serialVersionUID = -1L;

    private static final Logger logger = LoggerFactory.getLogger(RuleResult.class);

    private int ID;

    private String innerRule;
    private String rule;
    private String checkSQL;
    private String correctSQL;

    private long rowSize;
    private long support;
    private long unsupport; //如果这条是x->y。那么unsupport代表x推出的不是y的数量。容错率就是support/(support+unsupport)

    private String supportExample;
    private String unsupportExample;

    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public String getCheckSQL() {
        return checkSQL;
    }

    public void setCheckSQL(String checkSQL) {
        this.checkSQL = checkSQL;
    }

    public String getCorrectSQL() {
        return correctSQL;
    }

    public void setCorrectSQL(String correctSQL) {
        this.correctSQL = correctSQL;
    }

    public long getRowSize() {
        return rowSize;
    }

    public void setRowSize(long rowSize) {
        this.rowSize = rowSize;
    }

    public long getSupport() {
        return support;
    }

    public void setSupport(long support) {
        this.support = support;
    }

    public long getUnsupport() {
        return unsupport;
    }

    public void setUnsupport(long unsupport) {
        this.unsupport = unsupport;
    }

    public String getSupportExample() {
        return supportExample;
    }

    public void setSupportExample(String supportExample) {
        this.supportExample = supportExample;
    }

    public String getUnsupportExample() {
        return unsupportExample;
    }

    public void setUnsupportExample(String unsupportExample) {
        this.unsupportExample = unsupportExample;
    }

    public String getPrintString() {
        StringBuffer printStr = new StringBuffer(ID + ":{");
        printStr.append("rule=").append(rule).append(", ");
        if(StringUtils.isNotEmpty(checkSQL)) {
            printStr.append("checkSQL=").append(checkSQL).append(", ");
        }
        if(StringUtils.isNotEmpty(correctSQL)) {
            printStr.append("correctSQL=").append(correctSQL).append(", ");
        }

        printStr.append("rowSize=").append(rowSize)
                .append(", support=").append(support)
                .append(", unsupport=").append(unsupport)
//                .append(", supportExample=<").append(supportExample)
//                .append(">, unsupportExample=<").append(unsupportExample).append(">")
                .append(", cr=" ).append(getCr())
                .append(", ftr=").append(getFtr()).append("}");

        return printStr.toString();
    }

    public String getPrintREEs() {
        return  ID + "," +
                "rule=" + rule +
                ", rowSize=" + rowSize +
                ", support=" + support +
                ", unsupport=" + unsupport;
                /*
                ", cr=" + getCr() +
                ", ftr=" + getFtr() +
                '}';

                 */
    }

    public double getCr() {
        return BigDecimal.valueOf((float) support / (rowSize + 1)).setScale(2, BigDecimal.ROUND_UP).doubleValue();
    }

    public double getFtr() {
        return BigDecimal.valueOf((float) support / (support + unsupport)).setScale(2, BigDecimal.ROUND_UP).doubleValue();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
    @Override
    public String toString() {
        return rule;
    }

    @Override
    public boolean equals(Object obj) {
        return this.rule.equals(obj.toString());
    }

    @Override
    public int hashCode() {
        return this.rule.hashCode();
    }


    public String getInnerRule() {
        return innerRule;
    }

    public void setInnerRule(String innerRule) {
        this.innerRule = innerRule;
    }
}
