package sics.seiois.mlsserver.model;

import lombok.Getter;
import lombok.Setter;

/***
 *
 * @author liuyong
 *
 */
@Getter
@Setter
public class PredicateExpressionModel implements java.io.Serializable {
    private String columnName; //列表别名

    private String explicitPredicateExpression;  //谓词显式表达式

    private String calcPredicateExpression;      //谓词计算表达式
}