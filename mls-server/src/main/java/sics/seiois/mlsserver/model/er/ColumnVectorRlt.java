package sics.seiois.mlsserver.model.er;

import com.sics.seiois.common.base.BaseEntity;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by suyh on 2020/11/10.
 */
public class ColumnVectorRlt extends BaseEntity {

    //名称
    @Setter
    @Getter
    private String name;

    //推理向量结果
    @Setter
    @Getter
    private List<List<Double>> vector;
}
