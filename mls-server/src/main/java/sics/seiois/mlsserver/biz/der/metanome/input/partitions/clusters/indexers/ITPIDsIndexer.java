package sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers;

import java.util.Collection;
import java.util.List;

public interface ITPIDsIndexer {

    public Collection<Integer> getValues();


    public List<Integer> getTpIDsForValue(Integer value);

    public int getIndexForValueThatIsLessThan(int value);

    public int getScriptNull();

}
