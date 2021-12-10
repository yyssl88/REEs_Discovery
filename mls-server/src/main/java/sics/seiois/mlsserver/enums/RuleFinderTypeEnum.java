package sics.seiois.mlsserver.enums;

import lombok.Getter;
import lombok.Setter;

/**
 * 规则发现类型枚举类
 * Created by suyh on 2020/11/20.
 */

public enum RuleFinderTypeEnum {
    CR_ONLY(10),
    ER_ONLY(20),
    CR_AND_ER(30),
    ;

    @Setter
    @Getter
    private Integer value;

    RuleFinderTypeEnum(Integer value) {
        this.value = value;
    }
}
