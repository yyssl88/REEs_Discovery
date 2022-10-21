package sics.seiois.mlsserver.biz.der.metanome.predicates;

import java.io.Serializable;

import com.jcraft.jsch.HASH;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.Operator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.RuleContext;
import sics.seiois.mlsserver.biz.der.metanome.helpers.IndexProvider;
import sics.seiois.mlsserver.biz.der.metanome.predicates.operands.ColumnOperand;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.utils.MlsConstant;

import java.util.*;

import sics.seiois.mlsserver.utils.helper.MLUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class Predicate implements PartitionRefiner, de.metanome.algorithm_integration.Predicate, Serializable {

    private static final long serialVersionUID = -1632843229256239029L;

    public static final int CONSTANT_TEMPLATE_INT = -2;
    public static final String CONSTANT_TEMPLATE_VALUE = "__XXXXXX__";

    private final Operator op;
    private final ColumnOperand operand1;
    private final ColumnOperand operand2;
    private int index1;
    private int index2;
    private String constant;  // if value == CONSTANT_TEMPLATE_VALUE, it is a constant template
    private Integer constantInt;  // if int == CONSTANT_TEMPLATE_INT, it is a constant template
    private String mlOption;
    private boolean isJoin;

    // only for multiple relational tables
    private int index_new_1;
    private int index_new_2;


    /*
        prepare data shipment
     */
    public void initialPredicateDataTransfer() {
        log.info(">>>>set Predicate Data:{} | {}", operand1.getColumn().getPliSections().size(), operand1.getColumn().getValueInt(0));
        operand1.initialParsedColumnLight();
        operand2.initialParsedColumnLight();
    }

    /*
        check whether <t, s> corresponds to correct relations
     */
    public boolean checkIfCorrectRelationName(String r_1, String r_2) {
        if (this.isConstant()) {
            return this.operand1.getColumn().getTableName().equals(r_1) || this.operand1.getColumn().getTableName().equals(r_2);
        }
        return this.operand1.getColumn().getTableName().equals(r_1) && this.operand2.getColumn().getTableName().equals(r_2);
    }

    public boolean checkIfCorrectOneRelationName(String r_1) {
        return this.operand1.getColumn().getTableName().equals(r_1);
    }

    public Predicate(Operator op, ColumnOperand<?> operand1, ColumnOperand<?> operand2) {
        if (op == null) {
            throw new IllegalArgumentException("Operator must not be null.");
        }

        if (operand1 == null) {
            throw new IllegalArgumentException("First operand must not be null.");
        }

        if (operand2 == null) {
            throw new IllegalArgumentException("Second operand must not be null.");
        }

        this.op = op;
        this.operand1 = operand1;
        this.operand2 = operand2;
        this.index1 = this.operand1.getIndex();
        this.index2 = this.operand2.getIndex();
        this.constant = null;
        this.mlOption = null;

        // set supports
        // this.setSupportPredicate();
    }

    public Predicate(Predicate old, int index_1, int index_2) {
        this.op = old.getOperator();
        this.operand1 = old.getOperand1();
        this.operand2 = old.getOperand2();
        this.index1 = index_1;
        this.index2 = index_2;
        this.constant = old.getConstant();
        this.constantInt = old.getConstantInt();
        this.mlOption = old.getMlOption();

        if (old.isConstant()) {
            this.index2 = this.index1;
        }

        // set ML list
        this.mlOnePredicate = old.getMlOnePredicate();

        // set supports
        this.support = old.getSupport();
    }


    public Predicate(Predicate old, int index_1, int index_2, String constant, int constantInt, String mlOption) {
        this.op = old.getOperator();
        this.operand1 = old.getOperand1();
        this.operand2 = old.getOperand2();
        this.index1 = index_1;
        this.index2 = index_2;
        this.constant = constant;
        this.constantInt = constantInt;
        this.mlOption = mlOption;

        if (old.isConstant()) {
            this.index2 = this.index1;
        }

        // set ML list
        this.mlOnePredicate = old.getMlOnePredicate();

        // set supports
        this.support = old.getSupport();
    }

    // set value int for CONSTANT predicates
    public void setValuesInt(IndexProvider<String> providerS, IndexProvider<Double> providerD, IndexProvider<Long> providerL) {
        if (this.operand1.getColumn().getType() == String.class) {
            this.constantInt = providerS.getIndex((String) this.constant);
        } else if (this.operand1.getColumn().getType() == Double.class) {
            this.constantInt = providerD.getIndex(Double.parseDouble(this.constant));
        } else if (this.operand1.getColumn().getType() == Long.class) {
            this.constantInt = providerL.getIndex(Long.parseLong(this.constant));
        }
    }

//    private HashSet<Integer> satisfied_tuples;

    private transient Map<String, List<List<Integer>>> mlList = null; //new HashMap<>();

    public Map<String, List<List<Integer>>> getMlList() {
        return mlList;
    }

//    public HashSet<Integer> getTuples() {
//        return this.satisfied_tuples;
//    }

    private transient ArrayList<ImmutablePair<Integer, Integer>> mlOnePredicate = null;
    private long mlOnePredicateSize = 0L;

    public ArrayList<ImmutablePair<Integer, Integer>> getMlOnePredicate() {
        return this.mlOnePredicate;
    }

    public void setMLList(ArrayList<ImmutablePair<Integer, Integer>> mllist) {
        this.mlOnePredicate = mllist;
        if (mllist != null) {
            this.mlOnePredicateSize = mllist.size();
        } else {
            this.mlOnePredicateSize = 0;
        }
    }

    private long support;

    public long getSupport() {
        return this.support;
    }

    public void setSupport(long supp) {
        this.support = supp;
    }

    /**
     * statistic：
     * // 这个 map 的 key 是每一列 即 tabName.colName
     * // value 是一个 map
     * // value key 是这一列的每个一次索引
     * // value value 是这一列的每个一次索引出现次数，即每个值的重复数
     * <p>
     * 逻辑：
     * 一句话：设置 this.support 为这个谓词的独立情况下支持数目（全 es 表时的支持数）
     * 常数谓词情况
     * <p>
     * // 关于这个常数的支持情况
     * // 假如这一列的为 aa bb cc cc
     * // 那么索引后为   1   2  3  3
     * // 这个常熟为谓词为 col="cc"，则 constantInt 就是 3
     * // ⭐⭐ 那么 support 就是这个常数 cc 在这一列出现次数
     * <p>
     * 非常数谓词情况
     * // 不跨列 c=c 或者跨列 c1=c2
     * // support = 只考虑这两列，而求出的 support 的数量（笛卡尔积）
     * // 打比方
     * // c1 = aa bb cc cc
     * // c2 = bb bb cc cc
     * // 那么 c1=c2 包含 bb=bb 的情况 2 种。 cc=cc 的情况 4 中
     * // support  = 6
     *
     * @param statistic 索引表示的值重复数
     */
    public void setSupportPredicate(HashMap<String, HashMap<Integer, Long>> statistic) {
        this.support = 0L;
        if (this.isML()) {
            this.support = this.mlOnePredicateSize;
        } else if (this.isConstant()) {
            // 关于这个常数的支持情况
            // 假如这一列的为 aa bb cc cc
            // 那么索引后为   1   2  3  3
            // 这个常熟为谓词为 col="cc"，则 constantInt 就是 3
            // ⭐⭐ 那么 support 就是这个常数 cc 在这一列出现次数
            String k = this.getOperand1().getColumn().toStringData();
            long suppConstant = 0;
            if (statistic.get(k).containsKey(this.constantInt)) {
                suppConstant = statistic.get(k).get(this.constantInt);
            }
            this.support += suppConstant;
        } else {
            // 非常数谓词
            // 不跨列 c=c 或者跨列 c1=c2
            // support = 只考虑这两列，而求出的 support 的数量（笛卡尔积）
            // 打比方
            // c1 = aa bb cc cc
            // c2 = bb bb cc cc
            // 那么 c1=c2 包含 bb=bb 的情况 2 种。 cc=cc 的情况 4 中
            // support  = 6
            String k1 = this.getOperand1().getColumn().toStringData();
            HashMap<Integer, Long> stat1 = statistic.get(k1);
            String k2 = this.getOperand2().getColumn().toStringData();
            HashMap<Integer, Long> stat2 = statistic.get(k2);
            for (Map.Entry<Integer, Long> entry : stat1.entrySet()) {
                if (stat2.containsKey(entry.getKey())) {
                    this.support += entry.getValue() * stat2.get(entry.getKey());
                }
            }
        }

//        this.support = 0L;
//        if (this.isML()) {
//            this.support = this.mlOnePredicateSize;
//        } else if (this.isConstant()) {
//            PLI pli = this.getOperand1().getColumn().getPli();
//            if (pli == null) {
//                return;
//            }
//            List<Integer> temp = pli.getTpIDsForValue(this.constantInt); //.size();
//            if (temp != null) {
//                this.support = temp.size();
//            }
//        } else {
//            PLI pliPivot = this.getOperand1().getColumn().getPli();
//            PLI pliProbe = this.getOperand2().getColumn().getPli(); // probing PLI
//            if(pliPivot != null && pliProbe != null) {
//                boolean isSame = false;
//                if (pliPivot.equals(pliProbe)) {
//                    isSame = true;
//                }
//                Collection<Integer> valuesPivot = pliPivot.getValues();
//                for (Integer vPivot : valuesPivot) {
//                    if (vPivot == -1) continue;
//                    List<Integer> tidsProbe = pliProbe.getTpIDsForValue(vPivot);
//                    if (tidsProbe != null) {
//                        List<Integer> tidsPivot = pliPivot.getTpIDsForValue(vPivot);
//                        this.support += tidsPivot.size() * tidsProbe.size();
//                        if (tidsPivot.size() == 0 || tidsProbe.size() == 0) {
//                            continue;
//                        }
//                        if (isSame) {
//                            this.support -= tidsPivot.size();
//                        }
//                    }
//                }
//            } else {
//                if (this.getOperand1().getColumnLight() == null || this.getOperand2().getColumnLight() == null) {
//                    return;
//                }
//                PLILight pliPivot_ = this.getOperand1().getColumnLight().getPliLight();
//                PLILight pliProbe_= this.getOperand2().getColumnLight().getPliLight();
//                if (pliPivot_ == null || pliProbe_ == null) {
//                    return;
//                }
//
//                boolean isSame = false;
//                if (pliPivot_.equals(pliProbe_)) {
//                    isSame = true;
//                }
//                Collection<Integer> valuesPivot_ = pliPivot_.getValues();
//                for (Integer vPivot : valuesPivot_) {
//                    if (vPivot == -1) continue;
//                    List<Integer> tidsProbe = pliProbe_.getTpIDsForValue(vPivot);
//                    if (tidsProbe != null) {
//                        List<Integer> tidsPivot = pliPivot_.getTpIDsForValue(vPivot);
//                        this.support += tidsPivot.size() * tidsProbe.size();
//                        if (tidsPivot.size() == 0 || tidsProbe.size() == 0) {
//                            continue;
//                        }
//                        if (isSame) {
//                            this.support -= tidsPivot.size();
//                        }
//                    }
//                }
//            }
//        }
    }

//    public ArrayList<ImmutablePair<Integer, Integer>> extractBiList(){
//        if (this.isML()) {
//            return this.mlOnePredicate;
//        } else if (this.isConstant()) {
//            // throw new Exception("should not be constant predicate");
//            return null;
//        } else {
//            ArrayList<ImmutablePair<Integer, Integer>> list = new ArrayList<>();
//
//            PLILight pliPivot = this.getOperand1().getColumnLight().getPliLight();
//            PLILight pliProbe = this.getOperand2().getColumnLight().getPliLight(); // probing PLI
//            if(pliPivot == null || pliProbe == null) {
//                return list;
//            }
//            Collection<Integer> valuesPivot = pliPivot.getValues();
//            for (Integer vPivot : valuesPivot) {
//                if (vPivot == -1) continue;
//                List<Integer> tidsProbe = pliProbe.getTpIDsForValue(vPivot);
//                if (tidsProbe != null) {
//                    List<Integer> tidsPivot = pliPivot.getTpIDsForValue(vPivot);
//
//                    for (Integer tidPivot : tidsPivot) {
//                        for (Integer tidProbe : tidsProbe) {
//                            if (tidPivot.equals(tidProbe))
//                                continue;
//
//                            list.add(new ImmutablePair<>(tidPivot, tidProbe));
//                        }
//                    }
//
//                }
//            }
//            return list;
//        }
//
//    }

    public ArrayList<Integer> extractUniList() {
        if (!this.isConstant()) {
            return null;
        }

        return (ArrayList<Integer>) this.operand1.getColumn().getPli().getTpIDsForValue(this.constantInt);
    }

    public HashSet<Integer> getTuples(RuleContext rc) {
        HashSet<Integer> hs = new HashSet<>();
        hs.addAll(rc.getConstantPredicatesPLI(this));
        return hs;
    }

    // only for constant predicates
//    public void constructTuples() {
//        if (this.satisfied_tuples != null) return;
//        this.satisfied_tuples = new HashSet<>();
//        List<Integer> temp = this.operand1.getColumn().getPli().getTpIDsForValue(this.getConstantInt());
//        if (temp == null) return;
//        for (Integer e : temp) {
//            this.satisfied_tuples.add(e);
//        }
//    }

//    public void constuctTuples(RuleContext rc) {
//        if (this.satisfied_tuples != null) return;
//        this.satisfied_tuples = new HashSet<>();
//        List<Integer> temp = rc.getConstantPredicatesPLI(this);
//        if (temp == null) return;
//        for (Integer e : temp) {
//        this.satisfied_tuples.add(e);
//    }
//    }


    // for ml list
    private HashMap<Integer, HashSet<Integer>> ml_list;

    public boolean ifContainTP(int tid_1, int tid_2) {
        if (ml_list == null) return false;
        if (ml_list.containsKey(tid_1) && ml_list.get(tid_1).contains(tid_2)) return true;
        return false;
    }


    // get the encoded Integer value of one tid
    // ONLY for constant predicate or constant template
    public int retrieveConstantValue(int pid, int tid, int tuple_id_start) {
        int offset = (int) this.operand1.getColumnLight().getTidsInterval(pid).left;
        int value = this.operand1.getColumnLight().getValueInt(pid, tid - tuple_id_start - offset);
        return value;
    }

    public boolean calculateTpConstant(int pid, int tid, int tuple_id_start) {
        int offset = (int) this.operand1.getColumnLight().getTidsInterval(pid).left;
        int value = this.operand1.getColumnLight().getValueInt(pid, tid - tuple_id_start - offset);
        if (value == this.getConstantInt()) {
            return true;
        }
        return false;
    }

    public boolean calculateTp(int pid1, int pid2, int tid1, int tid2, int tuple_id_start1, int tuple_id_start2) {
        if (isML()) return ifContainTP(tid1, tid2);
        int offset_1 = (int) this.operand1.getColumnLight().getTidsInterval(pid1).left;
        int value_1 = this.operand1.getColumnLight().getValueInt(pid1, tid1 - tuple_id_start1 - offset_1);
        int value_2 = 0;
        if (isConstant()) {
            value_2 = this.getConstantInt();
        } else {
            int offset_2 = (int) this.operand2.getColumnLight().getTidsInterval(pid2).left;
            value_2 = this.operand2.getColumnLight().getValueInt(pid2, tid2 - tuple_id_start2 - offset_2);
        }
        boolean res = false;
        switch (op) {
            case EQUAL: {
                res = (value_1 == value_2);
            }
            break;
            case UNEQUAL: {
                res = (value_1 != value_2);
            }
            break;
            case LESS: {
                res = (value_1 < value_2);
            }
            break;
            case LESS_EQUAL: {
                res = (value_1 <= value_2);
            }
            break;
            case GREATER: {
                res = (value_1 > value_2);
            }
            break;
            case GREATER_EQUAL: {
                res = (value_1 >= value_2);
            }
            break;
            default:
                break;
        }
        return res;
    }

    public boolean calculateTp(int tid1, int tid2, RuleContext rc) {

        String key1 = this.getOperand1().getColumn().getTableName() + PredicateConfig.TBL_COL_DELIMITER
                + this.getOperand1().getColumn().getName();
        String key2 = this.getOperand2().getColumn().getTableName() + PredicateConfig.TBL_COL_DELIMITER
                + this.getOperand2().getColumn().getName();
        Map<Integer, Integer> o1T2V = rc.getTupleValues().get(key1);
        Map<Integer, Integer> o2T2V = rc.getTupleValues().get(key2);

        if (isML()) return ifContainTP(tid1, tid2);

        Integer value_1 = o1T2V.get(tid1);
        Integer value_2 = o2T2V.get(tid2);
        if (value_1 == null || value_2 == null) return false;

        boolean res = false;
        switch (op) {
            case EQUAL: {
                res = (value_1 == value_2);
            }
            break;
            case UNEQUAL: {
                res = (value_1 != value_2);
            }
            break;
            case LESS: {
                res = (value_1 < value_2);
            }
            break;
            case LESS_EQUAL: {
                res = (value_1 <= value_2);
            }
            break;
            case GREATER: {
                res = (value_1 > value_2);
            }
            break;
            case GREATER_EQUAL: {
                res = (value_1 >= value_2);
            }
            break;
            default:
                break;
        }
        return res;
    }


    public void changeToConstantTemplate() {
        if (this.isConstant()) {
            this.constantInt = CONSTANT_TEMPLATE_INT;
            this.constant = CONSTANT_TEMPLATE_VALUE;
        }
    }

    /*
     * transform constant (String) to an Integer
     */
    public void setConstantInt(Integer b) {
        this.constantInt = b;
    }

    public Integer getConstantInt() {
        return this.constantInt;
    }

    public void setJoin(boolean isJoin) {
        this.isJoin = isJoin;
    }

    public boolean isJoin() {
        return isJoin;
    }

    public void setConstant(String c) {
        this.constant = c;
    }

    public void setML() {
        this.mlOption = "ML";
    }

    public void setML(String mlID) {
        this.mlOption = mlID;
    }

    public String getMlOption() {
        return mlOption;
    }

    public void accumIndex() {
        this.index1++;
        this.index2++;
    }

    public boolean isConstantTemplate() {
        if (this.constant != null) {
            if (this.constantInt == CONSTANT_TEMPLATE_INT) {
                return true;
            }
        }
        return false;
    }

    public boolean isConstant() {
        if (this.constant != null) {
            return true;
        }
        return false;
    }

    public boolean isML() {
        if (this.mlOption != null) {
            return true;
        }
        return false;
    }

    public String getConstant() {
        return this.constant;
    }

    public void setIndexOperand1(int index) {
        this.index1 = index;
    }

    public int getIndex1() {
        return this.index1;
    }

    public int getIndex2() {
        return this.index2;
    }

    public void setIndexOperand2(int index) {
        this.index2 = index;
    }

    private Predicate symmetric;

//    public Predicate getSymmetric() {
//        if (symmetric != null)
//            return symmetric;
//
//        symmetric = predicateProvider.getPredicate(op.getSymmetric(), operand2, operand1);
//        symmetric.setIndexOperand1(this.index1);
//        symmetric.setIndexOperand2(this.index2);
//        return symmetric;
//    }

    public Predicate getSymmetric() {
        if (symmetric != null) {
            return symmetric;
        }

        // if (this.isML()) return null;

        symmetric = predicateProvider.getPredicate(op.getSymmetric(), operand2, operand1, this.constant, this.mlOption);
        symmetric.setIndexOperand1(this.index1);
        symmetric.setIndexOperand2(this.index2);
        if (this.isML()) symmetric.setML();
        return symmetric;
    }


    public void setIndex_new_1(int index) {
        this.index_new_1 = index;
    }

    public void setIndex_new_2(int index) {
        this.index_new_2 = index;
    }

    public void resetNewIndex() {
        this.index_new_2 = this.index_new_1 + 1;
    }

    public int getIndex_new_2() {
        return this.index_new_2;
    }


    private List<Predicate> implications = null;

//    public Collection<Predicate> getImplications() {
//        if (this.implications != null)
//            return implications;
//        Operator[] opImplications = op.getImplications();
//
//        List<Predicate> implications = new ArrayList<>(opImplications.length);
//        for (int i = 0; i < opImplications.length; ++i) {
//            implications.add(predicateProvider.getPredicate(opImplications[i], operand1, operand2));
//        }
//        this.implications = Collections.unmodifiableList(implications);
//        return implications;
//    }

    public Collection<Predicate> getImplications() {
        if (this.implications != null) {
            return implications;
        }
        if (this.isML()) return null;
        Operator[] opImplications = op.getImplications();

        List<Predicate> implications = new ArrayList<>(opImplications.length);
        for (int i = 0; i < opImplications.length; ++i) {
            implications.add(predicateProvider.getPredicate(opImplications[i], operand1, operand2, constant, mlOption));
        }
        this.implications = Collections.unmodifiableList(implications);
        return implications;
    }

    public boolean ifComparable(Predicate p) {
        if (!p.getOperand1().equals(p.getOperand1())) return true;
        // if (this.op == Operator.EQUAL && p.getOperator() == Operator.UNEQUAL) return false;
        // if (this.op == Operator.UNEQUAL && p.getOperator() == Operator.EQUAL) return false;
        if ((this.op == Operator.LESS || this.op == Operator.LESS_EQUAL) &&
                (p.getOperator() == Operator.GREATER || p.getOperator() == Operator.GREATER_EQUAL)) {
            if (this.getConstantInt() > p.getConstantInt()) {
                return true;
            }
        }

        if ((this.op == Operator.GREATER || this.op == Operator.GREATER_EQUAL) &&
                (p.getOperator() == Operator.LESS || p.getOperator() == Operator.LESS)) {
            if (this.getConstantInt() < p.getConstantInt()) {
                return true;
            }
        }
        return true;
    }

    public boolean implies(Predicate add) {
        if (add.operand1.equals(this.operand1) && add.operand2.equals(this.operand2)) {
            for (Operator i : op.getImplications()) {
                if (add.op == i) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean equalRHS(Predicate add) {
        if (add.getOperand1().equals(this.getOperand1()) && add.getOperand2().equals(this.getOperand2())) {
            return true;
        }
        return false;
    }

    public Operator getOperator() {
        return op;
    }

    public ColumnOperand<?> getOperand1() {
        return operand1;
    }

    public ColumnOperand<?> getOperand2() {
        return operand2;
    }

//    public Predicate getInvT1T2() {
//        ColumnOperand<?> invO1 = operand1.getInvT1T2();
//        ColumnOperand<?> invO2 = operand2.getInvT1T2();
//
//        return invO1 != null && invO2 != null ? predicateProvider.getPredicate(op, invO1, invO2) : null;
//    }

    public Predicate getInvT1T2() {
        ColumnOperand<?> invO1 = operand1.getInvT1T2();
        ColumnOperand<?> invO2 = operand2.getInvT1T2();

        return invO1 != null && invO2 != null ? predicateProvider.getPredicate(op, invO1, invO2, constant, mlOption) : null;
    }

    private Predicate inverse;

//    public Predicate getInverse() {
//        if (inverse != null)
//            return inverse;
//
//        inverse = predicateProvider.getPredicate(op.getInverse(), operand1, operand2);
//        return inverse;
//    }

    public Predicate getInverse() {
        if (inverse != null) {
            return inverse;
        }

        if (this.isML()) return null;

        inverse = predicateProvider.getPredicate(op.getInverse(), operand1, operand2, constant, mlOption);
        return inverse;
    }

    public boolean satisfies(int line1, int line2) {
        return op.eval(operand1.getValue(line1, line2), operand2.getValue(line1, line2));
    }

    @Override
    public String toString() {
        if (this.constant != null) {
            return " " + operand1.toString_(index1) +
                    " " + op.getShortString() + " " + this.constant + "";
            //常数谓词会重复两个表达式，注释掉
//            return " " + operand1.toString_(index1) +
//                    " " + op.getShortString() + " " + this.constant + "" +
//                    " " + operand1.toString_(index1 + 1) +
//                    " " + op.getShortString() + " " + this.constant + "";
        } else if (this.mlOption != null) {
            if (op == Operator.EQUAL) {
                return " ML_" + this.mlOption + "( " + operand1.toString_(index1) +
                        " , " + operand2.toString_(index2) + " )";
            } else {
                return " norML( " + operand1.toString_(index1) +
                        " , " + operand2.toString_(index2) + " )";
            }
        } else {
            return " " + operand1.toString_(index1) +
                    " " + op.getShortString() +
                    " " + operand2.toString_(index2) + "";
        }
    }

    //	public String toString() {
//		return "[Predicate: " + operand1.toString() + " " + op.getShortString() + " " + operand2.toString() + "]";
//	}

    public String toInnerString() {
        return this.toString();
    }

    public String toREEOp(String opStr) {
        if ("==".equals(opStr)) {
            return "=";
        }
        return opStr;
    }

    public String toREEString() {
        if (this.constant != null) {
            return " " + removeTableName(operand1.toString_(index_new_1)) +
                    " " + toREEOp(op.getShortString()) + " '" + this.constant.replace("，", ",") + "'";
//            +
//                    " ⋀ " + removeTableName(operand1.toString_(index1 + 1)) +
//                    " " + toREEOp(op.getShortString()) + " '" + this.constant + "'";
        } else if (this.mlOption != null) {
            if (op == Operator.EQUAL) {
                //mlOption格式举例:cosine@0.9
                String modelName = this.mlOption.split(MLUtil.SIMILAR_PREDICATE_SEGMENT)[0];
                String threshold = this.mlOption.split(MLUtil.SIMILAR_PREDICATE_SEGMENT)[1];
                if (StringUtils.isNotEmpty(threshold) && Double.valueOf(threshold) > 0) {
                    //similar('cosine', t0.name, t1.name, 0.95)
                    return " similar( '" + modelName + "' " +
                            " , " + removeTableName(operand1.toString_(index_new_1)) +
                            " , " + removeTableName(operand2.toString_(index_new_2)) + ", " + threshold + ")";
                }
                //ML('ditto', t0.name, t1.name)
                return " ML( '" + modelName +
                        "' , " + removeTableName(operand1.toString_(index_new_1)) +
                        " , " + removeTableName(operand2.toString_(index_new_2)) + " )";
            } else {
                return " norML( " + removeTableName(operand1.toString_(index_new_1)) +
                        " , " + removeTableName(operand2.toString_(index_new_2)) + " )";
            }
        } else {
            return " " + removeTableName(operand1.toString_(index_new_1)) +
                    " " + toREEOp(op.getShortString()) +
                    " " + removeTableName(operand2.toString_(index_new_2)) + "";
        }
    }

    /**
     * 获取tableName
     * 苏阳华 修改于2020-06-02
     *
     * @return
     */
    public String getTableName() {
        String predicateStr = operand1.toString_(index1);
        return predicateStr.substring(0, predicateStr.indexOf("."));
    }

    public String getUseTableName() {
        String predicateStr = operand1.toString_(index1);
        if (predicateStr.contains(MlsConstant.RD_JOINTABLE_SPLIT_STR)) {
            String[] array = operand1.toString_(index1).split("[.]");
            StringBuffer tableStr = new StringBuffer(array[2].substring(0, array[2].lastIndexOf("__")));
            array = operand2.toString_(index2).split("[.]");
            tableStr.append(MlsConstant.RD_JOINTABLE_SPLIT_STR)
                    .append(array[2].substring(0, array[2].lastIndexOf("__")));
            return tableStr.toString();
        }
        return predicateStr.substring(0, predicateStr.indexOf("."));
    }

    /**
     * 获取谓词的列名。多个用","隔开
     * 苏阳华 修改于2020-06-02
     *
     * @return
     */
    public String getConditionColumn() {
        String columnName;
        if (this.constant != null) {
            columnName = getColumnName(operand1.toString_(index1)) + ",";
        } else {
            columnName = getColumnName(operand1.toString_(index1)) + "," + getColumnName(operand2.toString_(index2)) + ",";
        }

        return columnName;
    }

    /**
     * 谓词转变成check语法。主要是== 需要变成 =，因为sql里面的相等是 =
     * 苏阳华 修改于2020-06-02
     *
     * @return
     */
    public String toCheckSQLStr() {
        if (this.constant != null) {
            String rlt = " " + removeTableName(operand1.toString_(index_new_1)) + (op == Operator.EQUAL ? " = " : op.getShortString());
            if (Double.class.equals(this.operand1.getColumn().getType()) || Long.class.equals(this.operand1.getColumn().getType())) {
                rlt += this.constant + " ";
            } else {
                rlt += "'" + this.constant.replace("，", ",") + "' ";
            }
            return rlt;
        } else if (this.mlOption != null) {
            if (op == Operator.EQUAL) {
                //包含@为相似度谓词，不包含为ML谓词
                if (mlOption.contains("@")) {
                    String similarName = this.mlOption.split("@")[0];
                    String min = this.mlOption.split("@")[1];
                    //similar('cosine', t0.name, t1.name, 0.95)
                    return " similar( '" + similarName + "' " +
                            " , " + removeTableName(operand1.toString_(index1)) +
                            " , " + removeTableName(operand2.toString_(index2)) + ", " + min + ")";
                } else {
                    return " ML( '" + mlOption + "' ," + removeTableName(operand1.toString_(index1)) + " , " + removeTableName(operand2.toString_(index2)) + " ) ";
                }
            } else {
                return " norML( " + removeTableName(operand1.toString_(index1)) + " , " + removeTableName(operand2.toString_(index2)) + " ) ";
            }
        } else {
            return " " + removeTableName(operand1.toString_(index1)) + " " + (op == Operator.EQUAL ? "=" : op.getShortString()) + " " + removeTableName(operand2.toString_(index2)) + "";
        }
    }


    /**
     * 移除谓词表达式里面的tableName.
     * user_info.t0.name == user_info.t1.name -> t0.name == t1.name
     * 苏阳华 修改于2020-06-02
     *
     * @return
     */
    private String removeTableName(String predicateStr) {
        if (StringUtils.isEmpty(predicateStr)) {
            return predicateStr;
        }

        String[] array = predicateStr.split("[.]");
        String[] tableNames = array[0].split(MlsConstant.RD_JOINTABLE_SPLIT_STR);

        String colName = array[2];
        if (tableNames.length > 1) {
            colName = array[2].substring(array[2].lastIndexOf("__") + 2);
        }

        return array[1] + "." + colName;
    }

    /**
     * 获取谓词表达式里面的列名。
     * user_info.t0.name == user_info.t1.name -> name
     * 苏阳华 修改于2020-06-02
     *
     * @return
     */
    private String getColumnName(String predicateStr) {
        if (StringUtils.isEmpty(predicateStr)) {
            return predicateStr;
        }

        return predicateStr.substring(predicateStr.lastIndexOf(".") + 1);
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
//        final int prime = 31;
//        int result = 1;
//        result = prime * result + ((op == null) ? 0 : op.hashCode());
//        result = prime * result + ((operand1 == null) ? 0 : operand1.hashCode());
//        result = prime * result + ((operand2 == null) ? 0 : operand2.hashCode());
//
//        // added by yyssl88
//        result = prime * result + this.index1;
//        result = prime * result + this.index2;
//
//        if (this.constant != null) {
//            result = prime * result + this.constant.hashCode();
//        }
//        // added by yyssl88
//        if (this.mlOption != null) {
//            result = prime * result + this.mlOption.hashCode();
//        }
//
//
//        return result;
    }

    public String getOperands() {
        return this.operand1.toString() + "." + this.operand2.toString();
    }

    @Override
    public boolean equals(Object obj) {
        return this.toString().equals(obj.toString());
//        if (this == obj)
//            return true;
//        if (obj == null)
//            return false;
//        if (getClass() != obj.getClass())
//            return false;
//        Predicate other = (Predicate) obj;
//
//        // added by yyssl88
//        if (this.index1 != other.getIndex1() || this.index2 != other.getIndex2())
//            return false;
//        if (operand1.equals(other.operand1) && this.constant != null && op == other.op
//                && other.constant != null && this.constant.equals(other.constant)) {
//            return true;
//        }
//        if(operand1.equals(other.operand1) && operand2.equals(other.operand2) &&
//            op == other.op && mlOption != null && other.mlOption != null) {
//            return true;
//        }
//
//        if (op != other.op)
//            return false;
//        if (operand1 == null) {
//            if (other.operand1 != null)
//                return false;
//        } else if (!operand1.equals(other.operand1))
//            return false;
//        if (operand2 == null) {
//            if (other.operand2 != null)
//                return false;
//        } else if (!operand2.equals(other.operand2))
//            return false;
//        return true;
    }

    private static final PredicateProvider predicateProvider = PredicateProvider.getInstance();

    @Override
    public List<ColumnIdentifier> getColumnIdentifiers() {
        List<ColumnIdentifier> list = new ArrayList<>();
        list.add(operand1.getColumn().getColumnIdentifier());
        list.add(operand2.getColumn().getColumnIdentifier());
        return list;
    }

    public boolean isCrossColumn() {

        return !operand1.getColumn().getColumnIdentifier().equals(operand2.getColumn().getColumnIdentifier());
    }

    private static Logger log = LoggerFactory.getLogger(Predicate.class);
}
