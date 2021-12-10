package sics.seiois.mlsserver.model.er;

import com.sics.seiois.common.base.BaseEntity;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by suyh on 2020/11/10.
 */
@Setter
@Getter
public class PredicateRequest extends BaseEntity {

    //参数实例
    private List<ColumnValue> instances;

    //算法名称
    private String algorithm;
}
