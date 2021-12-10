package sics.seiois.mlsserver.biz.der.metanome.helpers;

import gnu.trove.map.hash.TObjectIntHashMap;
import info.debatty.java.lsh.LSHSuperBit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.bitset.LongBitSet;


public class IndexProvider<T> implements Serializable {

    private static final long serialVersionUID = 3961087336068687061L;
    private static final Logger logger = LoggerFactory.getLogger(IndexProvider.class);

    private Map<T, Integer> indexes = new HashMap<>();
    private List<T> objects = new ArrayList<>();

    private int nextIndex = 0;
    public static LSHSuperBit lsh = new LSHSuperBit(2, 4, 500);

    public Integer getIndex(T object) {
        Integer index = indexes.putIfAbsent(object, Integer.valueOf(nextIndex));
        if (index == null) {
            index = Integer.valueOf(nextIndex);
            ++nextIndex;
            objects.add(object);
        }
        return index;
    }

    public boolean contains(T object) {
        return indexes.containsKey(object);
    }

    public List<T> getObjects() {
        return objects;
    }

    /**
     * 根据key获取对象（主要用于获取predicate对象）
     *
     * @param key
     * @return
     * @author:suyanghua
     * @sice:2020-10-20
     */
    public T getObject(String key) {
        Map<String, T> mapObjects = new HashMap<>();
        for (T t : objects) {
            mapObjects.put(t.toString().trim(), t);
        }

        T t = mapObjects.get(key);
        if (null == t) {
            logger.error("#### getObject get null!key={}, objectList={}", key, objects.toString());
        }
        return t;
    }

    public Integer getIndex_(T object) {
        Integer index;
        if (object.getClass() != String.class) {
            index = indexes.putIfAbsent(object, Integer.valueOf(nextIndex));
        } else {
            String hash = LSH((String) object);
            index = indexes.putIfAbsent((T) hash, Integer.valueOf(nextIndex));
        }
        if (index == null) {
            index = Integer.valueOf(nextIndex);
            ++nextIndex;
            objects.add(object);
        }
        return index;
    }

    /*** LSH for vector ***/
    public String LSH(String svec) {
        String[] splitV = svec.split("\\s+");
        int[] vec = new int[splitV.length];
        int sc = 0;
        for (String ss : splitV) {
            vec[sc++] = Integer.parseInt(ss);
        }
        // only check 1st and 2nd ones
        int[] hash = lsh.hash(vec);
        return Integer.toString(hash[0]) + Integer.toString(hash[1]);
    }

    public T getObject(int index) {
        return objects.get(index);
    }

    public IBitSet getBitSet(Iterable<T> objects) {
        IBitSet result = LongBitSet.FACTORY.create();
        for (T i : objects) {
            result.set(getIndex(i).intValue());
        }
        return result;
    }

    public Collection<T> getObjects(IBitSet bitset) {
        ArrayList<T> objects = new ArrayList<>();
        for (int i = bitset.nextSetBit(0); i >= 0; i = bitset.nextSetBit(i + 1)) {
            objects.add(getObject(i));
        }
        return objects;
    }

    public static <A extends Comparable<A>> IndexProvider<A> getSorted(IndexProvider<A> r) {
        IndexProvider<A> sorted = new IndexProvider<>();
        List<A> listC = new ArrayList<A>(r.objects);
        Collections.sort(listC);
        for (A c : listC) {
            sorted.getIndex(c);
        }
        // 优化内存
        ((ArrayList<?>) sorted.objects).trimToSize();
        return sorted;
    }

    public int size() {
        return nextIndex;
    }

    public Integer transform(T constant) {
        return this.indexes.get(constant);
    }

    @Override
    public String toString() {
        return objects.toString();
    }
}
