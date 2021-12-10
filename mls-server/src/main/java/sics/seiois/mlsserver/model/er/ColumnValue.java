package sics.seiois.mlsserver.model.er;

import com.alibaba.fastjson.annotation.JSONField;
import com.sics.seiois.common.base.BaseEntity;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by suyh on 2020/11/12.
 */
@Setter
@Getter
public class ColumnValue extends BaseEntity {

    //名称
    private String name;

    //列的值
    private List<String> data;

    //模型ID,不是推理平台的参数，不需要进行序列化
    @JSONField(serialize=false)
    private String modelID;
}
