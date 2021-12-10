package sics.seiois.mlsserver.model;

import gnu.trove.map.hash.TObjectLongHashMap;

public class MLSObjectLongHashMap<T> extends TObjectLongHashMap<T> {
    public MLSObjectLongHashMap() {
        super();
    }

    public int getIndex(T key) {
        int index = this.index(key);
        if(index >= 0) {
            return index;
        } else {
            return -1;
        }
    }

    public Object getKeyByIndex(Integer index) {
        Object[] k = this._set;
        return k[index];
    }
}
