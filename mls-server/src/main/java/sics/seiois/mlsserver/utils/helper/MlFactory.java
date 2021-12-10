package sics.seiois.mlsserver.utils.helper;

import com.sics.seiois.client.model.mls.ModelPredicate;

import sics.seiois.mlsserver.enums.ModelTypeEnum;

public class MlFactory {

    private static ModelPredicate standardMl(String name) {
        ModelPredicate model = new ModelPredicate();
        model.setName(name);
        model.setThreshold(0.0);
        model.setType(ModelTypeEnum.MODEL.getValue());
        return model;
    }


    public static ModelPredicate buildDittoSingle() {
        return standardMl("ditto_single_org");
    }

    public static ModelPredicate buildDittoMulti() {
        return standardMl("ditto_multi_attr");
    }

    public static ModelPredicate buildSentenceBertSingle() {
        return standardMl("sbert_single_org");
    }

    public static ModelPredicate buildSentenceBertMulti() {
        return standardMl("sbert_multi_attr");
    }

    public static ModelPredicate buildSelfSingle() {
        return standardMl("self_single_org");
    }

    public static ModelPredicate buildSelfMulti() {
        return standardMl("self_multi_attr");
    }

    public static ModelPredicate buildDeepMatcherSingle() {
        return standardMl("dm_single_org");
    }

    public static ModelPredicate buildDeepMatcherMulti() {
        return standardMl("dm_multi_attr");
    }
}
