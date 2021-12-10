package sics.seiois.mlsserver.model.er;

import com.sics.seiois.common.base.BaseEntity;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by friendsyh on 2020/11/2.
 * 标注对象实体
 */
@Setter
@Getter
public class LabelInfo extends BaseEntity {

    //左边表名
    private String leftTableName;

    //左边行ID
    private String leftRowID;

    //右边表名
    private String rightTableName;

    //右边行ID
    private String rightRowID;
}
