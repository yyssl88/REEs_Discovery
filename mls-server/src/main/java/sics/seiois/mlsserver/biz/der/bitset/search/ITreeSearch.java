package sics.seiois.mlsserver.biz.der.bitset.search;

import java.util.Collection;
import java.util.function.Consumer;

import sics.seiois.mlsserver.biz.der.bitset.IBitSet;

public interface ITreeSearch {

    boolean add(IBitSet bs);

    void forEachSuperSet(IBitSet bitset, Consumer<IBitSet> consumer);

    void forEach(Consumer<IBitSet> consumer);

    void remove(IBitSet remove);

    boolean containsSubset(IBitSet bitset);

    Collection<IBitSet> getAndRemoveGeneralizations(IBitSet invalidDC);
    boolean add(IBitSet bs, IBitSet _rhss);
    IBitSet containSubsetRHSs(IBitSet add);

}