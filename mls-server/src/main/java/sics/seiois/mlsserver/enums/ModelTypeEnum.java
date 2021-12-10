package sics.seiois.mlsserver.enums;

import com.sics.seiois.common.base.IEnum;

import lombok.Getter;
import lombok.Setter;

/**
 * 模型类型枚举类
 * Created by suyh on 2020/11/20.
 */

public enum ModelTypeEnum implements IEnum {
    MD_SIMILAR("MD"),
    MODEL("ML"),
    ;

    @Setter
    @Getter
    private String value;

    ModelTypeEnum(String value) {
        this.value = value;
    }

    @Override
    public Integer getKey() {
        return null;
    }

    @Override
    public String getValue() {
        return this.value;
    }
}
