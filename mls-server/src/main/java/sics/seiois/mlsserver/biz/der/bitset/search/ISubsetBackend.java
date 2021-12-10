package sics.seiois.mlsserver.biz.der.bitset.search;

import java.util.Set;
import java.util.function.Consumer;

import sics.seiois.mlsserver.biz.der.bitset.IBitSet;

public interface ISubsetBackend {

    boolean add(IBitSet bs);

    Set<IBitSet> getAndRemoveGeneralizations(IBitSet invalidFD);

    boolean containsSubset(IBitSet add);

    void forEach(Consumer<IBitSet> consumer);

    boolean add(IBitSet bs, IBitSet _rhss);
    IBitSet containSubsetRHSs(IBitSet add);
}
