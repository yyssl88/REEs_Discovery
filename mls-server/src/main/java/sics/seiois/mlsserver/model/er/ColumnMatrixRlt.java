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
public class ColumnMatrixRlt extends BaseEntity {

    //x
    private Long x;

    //y
    private Long y;

    //y
    private Double val;
}
