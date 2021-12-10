package sics.seiois.mlsserver.biz.der.mining.utils;

import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLI;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.CategoricalTpIDsIndexer;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.ITPIDsIndexer;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.NumericalTpIDsIndexer;

import java.io.Serializable;
import java.util.*;

// 轻量级 PLI
public class PLILight implements Serializable {
    private static final long serialVersionUID = -7172843779266290717L;

    // 全局唯一有序索引（一次索引） --> 局部行号
    ITPIDsIndexer tpIDsIndexer;

    // 当前列的总行数
    long relSize;

    // 是否数值型
    boolean numerical;

    int scriptNull = -1;

    public PLILight(PLI pli) {
        this.tpIDsIndexer = pli.getTpIDsIndexer();
        this.relSize = pli.getRelSize();
        this.numerical = pli.getNumerical();
    }

    public int getScriptNull() {
        return this.scriptNull;
    }

    public Collection<Integer> getValues() {
        return tpIDsIndexer.getValues();
    }

    public boolean isNumerical() {
        return numerical;
    }

    public void setNumerical(boolean numerical) {
        this.numerical = numerical;
    }


    public long getRelSize() {
        return relSize;
    }

    public void setRelSize(long relSize) {
        this.relSize = relSize;
    }

    public List<Integer> getTpIDsForValue(Integer value) {
        return tpIDsIndexer.getTpIDsForValue(value);
    }

    public int getIndexForValueThatIsLessThan(int value) {
        return tpIDsIndexer.getIndexForValueThatIsLessThan(value);
    }


    public ITPIDsIndexer getTpIDsIndexer() {
        return tpIDsIndexer;
    }

    public void setTpIDsIndexer(ITPIDsIndexer tpIDsIndexer) {
        this.tpIDsIndexer = tpIDsIndexer;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
//        for (Integer key : tpIDMap.keySet()) {
//            List<Integer> list = (List<Integer>) tpIDMap.get(key);
//            sb.append(list + "\n");
//
//        }
        // sb.append("\n");
        // if (numerical)
        // sb.append((NumericalTpIDsIndexer) tpIDsIndexer);
        // else
        // sb.append((CategoricalTpIDsIndexer) tpIDsIndexer);

        return sb.toString();
    }


}
