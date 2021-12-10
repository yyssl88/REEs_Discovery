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
public class ColumnListVectorRlt extends BaseEntity {

    //返回类型
    private String type;

    //返回结果
    private List<ColumnVectorRlt> resultset;
}
