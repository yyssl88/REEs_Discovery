package sics.seiois.mlsserver.biz.der.metanome.predicates.sets;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.metanome.algorithm_integration.Operator;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.bitset.LongBitSet;
import sics.seiois.mlsserver.biz.der.metanome.helpers.IndexProvider;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import sics.seiois.mlsserver.biz.der.bitset.BitSetFactory;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.bitset.LongBitSet.LongBitSetFactory;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateSetTableMsg;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.utils.MlsConstant;

public class PredicateSet implements Iterable<Predicate>, Externalizable, KryoSerializable {

    private static final long serialVersionUID = -1682843129256279029L;
    private static Logger logger = LoggerFactory.getLogger(PredicateSet.class);

    private IBitSet bitset;

    // rhs of REE
    private int rhs;
    // relation tables
    private String table_name_1;
    private String table_name_2;
    private long support;
    private long violations;
    private Map<String, PredicateSetTableMsg> tableColMsg = null;
    private List<ImmutablePair<Integer, Integer>> tupleIds = new ArrayList<>();

    private ArrayList<Predicate> predicateDataTransfer = null;

    // prepare data transform
    public void prepareDataTransfer() {
        if (predicateDataTransfer == null) {
            predicateDataTransfer = new ArrayList<>();
            for (Predicate p : this) {
                p.initialPredicateDataTransfer();
                predicateDataTransfer.add(p);
            }
        }
    }

    // private List<ImmutablePair<Integer, Integer> > tuple_ids;

    public PredicateSet() {
        this.bitset = bf.create();
    }

    public PredicateSet(int capacity) {
        this.bitset = bf.create(capacity);
    }

    public PredicateSet(IBitSet bitset) {
        this.bitset = bitset.clone();
    }

    public PredicateSet convert() {
        PredicateSet converted = PredicateSetFactory.create();
        for (int l = bitset.nextSetBit(0); l >= 0; l = bitset.nextSetBit(l + 1)) {
            converted.add(indexProvider.getObject(l));
        }
        return converted;
    }

    public void union(IBitSet bs) {
        bitset.or(bs);
    }

    public void and(PredicateSet ps) {
        bitset.and(ps.getBitset());
    }

    public void or(PredicateSet ps) {
        bitset.or(ps.getBitset());
    }

    public void xor(PredicateSet ps) {
        bitset.xor(ps.getBitset());
    }

    public void clear() {
        this.bitset.clear();
    }

    public PredicateSet refine() {
        PredicateSet refined = PredicateSetFactory.create();
        boolean sat = false;
        String relation1 = "";
        String relation2 = "";
        for (int l = bitset.nextSetBit(0); l >= 0; l = bitset.nextSetBit(l+1)) {
            Predicate p = indexProvider.getObject(l);
            if (p.getOperator() == Operator.EQUAL) {
                // check whether the ps has ==
                sat = true;
                relation1 = p.getOperand1().getColumn().getTableName();
                relation2 = p.getOperand2().getColumn().getTableName();
                break;
            }
        }

        if (sat == false) {
            return null;
        }
        // if ps is valid, then prune useless predicates
        for (int l = bitset.nextSetBit(0); l >= 0; l = bitset.nextSetBit(l + 1)) {
            Predicate p = indexProvider.getObject(l);
            if (p.getOperand1().getColumn().getTableName().equals(relation1) &&
                    p.getOperand2().getColumn().getTableName().equals(relation2)) {
                refined.add(p);
            }
        }
        return refined;
    }

    public void setSupport(long s) {
        this.support = s;
    }

    public long getSupport() {
        return this.support;
    }

    public PredicateSet(PredicateSet pS) {
        this.bitset = pS.getBitset().clone();
    }

    public PredicateSet(Predicate p) {
        this.bitset = getBitSet(p);
    }

    public void remove(Predicate predicate) {
        this.bitset.clear(indexProvider.getIndex(predicate).intValue());
    }

    public void removeRHS() {
        this.bitset.clear(rhs);
    }

    public boolean containsPredicate(Predicate predicate) {
        return this.bitset.get(indexProvider.getIndex(predicate).intValue());
    }

    public boolean containsPS(PredicateSet ps) {
        boolean contain = false;
        for (Predicate pred : ps) {
            contain = this.bitset.get(indexProvider.getIndex(pred).intValue());
            if (contain == false) {
                break;
            }
        }
        return contain;
    }

    public boolean isSubsetOf(PredicateSet superset) {
        return this.bitset.isSubSetOf(superset.getBitset());
    }

    public IBitSet getBitset() {
        return bitset;
    }

    public int getRHS() {
        return this.rhs;
    }

    public IBitSet getRHSbs() {
        IBitSet bs = bf.create();
        bs.set(this.rhs);
        return bs;
    }

    public void addAll(PredicateSet PredicateBitSet) {
        this.bitset.or(PredicateBitSet.getBitset());
    }

    public int size() {
        return this.bitset.cardinality();
    }

    public long getViolations() {
        return violations;
    }

    public void setViolations(long violations) {
        this.violations = violations;
    }

    public boolean add(Predicate predicate) {
        int index = getIndex(predicate);
        boolean newAdded = !bitset.get(index);
        this.bitset.set(index);
        return newAdded;
    }

    public boolean addRHS(Predicate predicate) {
        int index = indexProvider.getIndex(predicate);
        boolean newAdded = !bitset.get(index);
        // set rhs
        this.rhs = index;
        this.bitset.set(index);
        return newAdded;
    }

    @Override
    public Iterator<Predicate> iterator() {
        return new Iterator<Predicate>() {
            private int currentIndex = bitset.nextSetBit(0);

            @Override
            public Predicate next() {
                if(!hasNext()){
                    throw new NoSuchElementException();
                }
                int lastIndex = currentIndex;
                currentIndex = bitset.nextSetBit(currentIndex + 1);
                return indexProvider.getObject(lastIndex);
            }

            @Override
            public boolean hasNext() {
                return currentIndex >= 0 && currentIndex < indexProvider.size();
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PredicateSet other = (PredicateSet) obj;
        if (bitset == null) {
            if (other.bitset != null) {
                return false;
            }
        } else if (!bitset.equals(other.bitset)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bitset == null) ? 0 : bitset.hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        this.forEach(p -> sb.append(p + " "));
        // this.forEach(p -> sb.append(p.getOperand1().getColumn().getName() + "" +
        // p.getOperator().getShortString() + " "));

//        return sb.toString();
        return null != sb && sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
    }

    public String toSortString() {
        List<String> pList = new ArrayList<>();
        this.forEach(p -> pList.add(p.toString()));
        pList.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o2.compareTo(o1);
            }
        });
        return pList.toString();
    }

    public String toREEString() {
        StringBuilder sb = new StringBuilder();
        this.forEach(p -> sb.append(p.toREEString() + " ^"));

        return null != sb && sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
    }

    public String toInnerString() {
        StringBuilder sb = new StringBuilder();
        this.forEach(p -> sb.append(p.toInnerString() + " ^"));
        // this.forEach(p -> sb.append(p.getOperand1().getColumn().getName() + "" +
        // p.getOperator().getShortString() + " "));

//        return sb.toString();
        return null != sb && sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
    }

    /**
     * 谓词转变成check语法。主要是== 需要变成 =，因为sql里面的相等是 =
     * 苏阳华 修改于2020-06-02
     * @return
     */
    public String toCheckSQLStr() {
        StringBuilder sb = new StringBuilder();
        this.forEach(p -> {
            sb.append(p.toCheckSQLStr() + " and");
        });
        return sb.substring(0, sb.length() - 4); //把后面的"空格and空格"去掉
    }

    /**
     * 去重获取所有列名字符串,用","隔开
     * 苏阳华 修改于2020-06-02
     * @return
     */
//    public String getColumnNameStr() {
//        StringBuilder sb = new StringBuilder();
//        this.forEach(p -> sb.append(p.getConditionColumn()));
//        String columnNameStr = sb.substring(0, sb.length() - 1);
//
//        return StringUtils.join(new HashSet<>(Arrays.asList(columnNameStr.split(","))), ",");
//    }
    public Map<String, PredicateSetTableMsg> getColumnNameStr() {
//        StringBuilder sb = new StringBuilder();
        if(null == tableColMsg) {
            tableColMsg = genePredicateMsg();
        }
        return tableColMsg;
    }

    public Map<String, PredicateSetTableMsg> genePredicateMsg() {
        Map<String, PredicateSetTableMsg> colMap = new HashMap<>();
        List<String> tableAlias = new ArrayList<>();
        this.forEach(p -> {
            addPredicateIntoMsg(p, colMap, tableAlias, false);
        });

        addPredicateIntoMsg(this.getPredicate(rhs), colMap, tableAlias, true);
        return colMap;
    }

    private void addPredicateIntoMsg(Predicate p, Map<String, PredicateSetTableMsg> colMap,
                                     List<String> tableAlias, boolean isRhs) {
        String [] array = p.getOperand1().toString_(p.getIndex1()).split("[.]");
        String [] tableNames = array[0].split(MlsConstant.RD_JOINTABLE_SPLIT_STR);

        String colName = array[2];
        String table1 = array[0];
        if(tableNames.length > 1) {
            colName = array[2].substring(array[2].lastIndexOf("__")+2);
            table1 = array[2].substring(0, array[2].lastIndexOf("__"));
        }

        if(!tableAlias.contains(table1)) {
            tableAlias.add(table1);
        }

        p.setIndex_new_1(tableAlias.indexOf(table1)*2 + p.getIndex1());
        if (!colMap.containsKey(table1)) {
            PredicateSetTableMsg colSet = new PredicateSetTableMsg();
            colMap.put(table1, colSet);
        }

        colMap.get(table1).addTableAlias(tableAlias.indexOf(table1)*2 + p.getIndex1());
        colMap.get(table1).addColName(colName);

        String tableStr = table1;
        if(!p.isConstant()) {
            array = p.getOperand2().toString_(p.getIndex2()).split("[.]");
            tableNames = array[0].split(MlsConstant.RD_JOINTABLE_SPLIT_STR);

            colName = array[2];
            tableStr = array[0];
            if(tableNames.length > 1) {
                colName = array[2].substring(array[2].lastIndexOf("__")+2);
                tableStr = array[2].substring(0, array[2].lastIndexOf("__"));
            }

            if (!colMap.containsKey(tableStr)) {
                PredicateSetTableMsg colSet = new PredicateSetTableMsg();
                colMap.put(tableStr, colSet);
            }

            if(tableStr.equals(table1)) {
                p.resetNewIndex();
            } else {
                if(!tableAlias.contains(tableStr)) {
                    tableAlias.add(tableStr);
                }
                p.setIndex_new_2(tableAlias.indexOf(tableStr)*2);
            }
            colMap.get(tableStr).addTableAlias(p.getIndex_new_2());
            colMap.get(tableStr).addColName(colName);
        }

        if(!isRhs) {
            if (tableStr.equals(table1)) {
                colMap.get(table1).addPredicate(p);
            }
        }
    }



    // table names
    public void setTableName(String table_name_1, String table_name_2) {
        this.table_name_1 = table_name_1;
        this.table_name_2 = table_name_2;
    }

    public String getTableName1() {
        return this.table_name_1;
    }

    public String getTableName2() {
        return this.table_name_2;
    }

    public static IndexProvider<Predicate> indexProvider = new IndexProvider<>();
    public static BitSetFactory bf = new LongBitSetFactory();

    static public Predicate getPredicate(int index) {
        return indexProvider.getObject(index);
    }

    static public IBitSet getBitSet(Predicate p) {
        int index = indexProvider.getIndex(p).intValue();
        IBitSet bitset = bf.create();
        bitset.set(index);
        return bitset;
    }

    public static int getIndex(Predicate add) {
        return indexProvider.getIndex(add).intValue();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, bitset);
        output.writeLong(support);
        kryo.writeObject(output, tupleIds);
//        kryo.writeObject(output, predicateDataTransfer);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        bitset = kryo.readObject(input, LongBitSet.class);
        support = input.readLong();
        tupleIds = kryo.readObject(input, ArrayList.class);
//        predicateDataTransfer = kryo.readObject(input, ArrayList.class);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(bitset);
        out.writeLong(support);
        out.writeObject(tupleIds);
//        out.writeObject(predicateDataTransfer);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bitset = (LongBitSet) in.readObject();
        support = in.readLong();
        tupleIds = (ArrayList) in.readObject();
//        predicateDataTransfer = (ArrayList) in.readObject();
    }

    public void addTidPair(Integer left, Integer right) {
        if(tupleIds.size() < PredicateConfig.getExampleSize()) {
            tupleIds.add(new ImmutablePair<>(left, right));
        }
    }

    public void setTids(List<ImmutablePair<Integer, Integer>> tids) {
        tupleIds = tids;
    }

    public List<ImmutablePair<Integer, Integer>> getTupleIds() {
        return tupleIds;
    }

}
