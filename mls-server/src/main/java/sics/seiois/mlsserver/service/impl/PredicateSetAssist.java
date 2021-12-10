package sics.seiois.mlsserver.service.impl;

import java.io.Serializable;

import sics.seiois.mlsserver.biz.der.bitset.BitSetFactory;
import sics.seiois.mlsserver.biz.der.metanome.helpers.IndexProvider;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateProvider;

public class PredicateSetAssist implements Serializable {

    private static final long serialVersionUID = -1632843129256279029L;

    private IndexProvider<Predicate> indexProvider;
    private BitSetFactory bf;
    private String taskId;

    public IndexProvider<Predicate> getIndexProvider() {
        return indexProvider;
    }

    public void setIndexProvider(IndexProvider<Predicate> indexProvider) {
        this.indexProvider = indexProvider;
    }

    public BitSetFactory getBf() {
        return bf;
    }

    public void setBf(BitSetFactory bf) {
        this.bf = bf;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

}
