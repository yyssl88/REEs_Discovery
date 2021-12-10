package sics.seiois.mlsserver.utils.helper;

import com.sics.seiois.client.model.mls.ModelPredicate;

public class SimilarAlgorithmsFactory {

    private static ModelPredicate standardAlgorithms(double min, String name){
        ModelPredicate algorithms = new ModelPredicate();
        algorithms.setName(name);
        algorithms.setThreshold(min);
        algorithms.setType("MD");
        return algorithms;
    }


    public static ModelPredicate buildLevenshtein(double min){
        return standardAlgorithms(min, "levenshtein");
    }

    public static ModelPredicate buildCosine(double min){
        return standardAlgorithms(min, "cosine");
    }

    public static ModelPredicate buildJaro(double min){
        return standardAlgorithms(min, "jaro-winkler");
    }

    public static ModelPredicate buildJaccard(double min){
        return standardAlgorithms(min, "jaccard");
    }
}
