package sics.seiois.mlsserver.biz.der.metanome.predicates;

import java.util.HashSet;

import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;

public class PredicateSetTableMsg {
    private HashSet<Integer> tableAlias;
    private HashSet<String> colNames;
    private PredicateSet joinSet;
    private PredicateSet predicateSet;

    public void addTableAlias(Integer tableAlias){
        if(null == this.tableAlias) {
            this.tableAlias = new HashSet<>();
        }
        this.tableAlias.add(tableAlias);
    }

    public void addColName(String colName){
        if(null == this.colNames) {
            this.colNames = new HashSet<>();
        }
        this.colNames.add(colName);
    }

    public void addJoinSet(Predicate predicate){
        if(null == this.joinSet) {
            this.joinSet = new PredicateSet();
        }
        this.joinSet.add(predicate);
    }

    public HashSet<Integer> getTableAlias(){
        return this.tableAlias;
    }

    public HashSet<String> getColNames(){
        return this.colNames;
    }

    public PredicateSet getJoinSet(){
        return this.joinSet;
    }

    public void addPredicate(Predicate predicate) {
        if(null == this.predicateSet) {
            this.predicateSet = new PredicateSet();
        }
        this.predicateSet.add(predicate);
    }

    public PredicateSet getPredicateSet(){
        return this.predicateSet;
    }

    @Override
    public String toString() {
        return "###:tableAlias:" + tableAlias + " colNames:" + colNames + " joinSet:" + joinSet + " predicateSet:" + predicateSet;
    }
}
