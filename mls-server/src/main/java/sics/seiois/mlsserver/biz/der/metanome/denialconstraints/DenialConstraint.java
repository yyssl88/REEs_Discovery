package sics.seiois.mlsserver.biz.der.metanome.denialconstraints;

import com.sics.seiois.client.model.mls.JoinInfo;
import com.sics.seiois.client.model.mls.JoinInfos;
import de.metanome.algorithm_integration.Operator;
import de.metanome.algorithm_integration.PredicateVariable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.bitset.search.NTreeSearch;
import sics.seiois.mlsserver.biz.der.metanome.input.ParsedColumn;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateSetTableMsg;
import sics.seiois.mlsserver.biz.der.metanome.predicates.operands.ColumnOperand;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.Closure;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSetFactory;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.utils.MlsConstant;


public class DenialConstraint {
    private static final Logger logger = LoggerFactory.getLogger(DenialConstraint.class);
    private PredicateSet predicateSet;
    private Predicate rhs;
    private Map<String, Set<Predicate>> FKMap;
    private static final int ONLY_ONE = 1;

    // interestingness score
    private double interestingnessScore;
    //

    public void setInterestingnessScore(double score) {
        this.interestingnessScore = score;
    }

    public double getInterestingnessScore() {
        return this.interestingnessScore;
    }

    public DenialConstraint(Predicate... predicates) {
        predicateSet = PredicateSetFactory.create(predicates);
    }

    public DenialConstraint(PredicateSet predicateSet) {
        this.predicateSet = predicateSet;
        this.rhs = PredicateSet.getPredicate(predicateSet.getRHS());
        this.setSupport(predicateSet.getSupport());
        this.violations = predicateSet.getViolations();
    }

    public void removeRHS() {
        this.predicateSet.removeRHS();
    }


    // implication of REEs
    public boolean isImpliedByREEs(NTreeSearch tree, PredicateSet closure, Predicate rhs) {
        IBitSet _rhss = tree.containSubsetRHSs(PredicateSetFactory.create(closure).getBitset());
        int rhs_index = PredicateSet.indexProvider.getIndex(rhs);
        if (_rhss != null && _rhss.get(rhs_index) == true) {
            return true;
        }

        DenialConstraint sym = getInvT1T2DC();
        Predicate rhs_sym = rhs.getInvT1T2();
        if (sym != null && rhs_sym != null) {
            Closure c = new Closure(sym.getPredicateSet());
            if (!c.construct()) return true;
            IBitSet _rhss2 = tree.containSubsetRHSs(PredicateSetFactory.create(c.getClosure()).getBitset());
            int rhs_index_sym = PredicateSet.indexProvider.getIndex(rhs_sym);
            if (_rhss2 != null && _rhss2.get(rhs_index_sym) == true) {
                return true;
            }
        }
        return false;
    }

    public DenialConstraint getInvT1T2DC() {
        PredicateSet invT1T2 = PredicateSetFactory.create();
        for (Predicate predicate : predicateSet) {
            Predicate sym = predicate.getInvT1T2();
            if (sym == null) {
                return null;
            }
            invT1T2.add(sym);
        }
        return new DenialConstraint(invT1T2);
    }

    public PredicateSet getPredicateSet() {
        return predicateSet;
    }

    public Predicate getRHS() {
        return rhs;
    }

    public int getPredicateCount() {
        return predicateSet.size();
    }

    /**
     * 输出REE语法
     * REE: table1(t0) ⋀ table1(t1) ⋀ table2(t3) ⋀ t0.name == t1.name ⋀ t0.code == t2.code ⋀ ML('001004', t0.name, t1.myname) -> t0.city == t2.city
     * @return
     */

    @Override
    public String toString() {
        return "[REE: " + predicateSet.getTableName1() + " ^ " + predicateSet.getTableName2() + " ^ " + predicateSet.toString() +
                " -> " + rhs.toString() + "]";
    }

    public String toString(String tableName) {
        return "[REE: " + tableName + "(t0) ^ " + tableName + "(t1) ^ " + predicateSet.toString().replaceAll(tableName +
                ".", "").replaceAll("==", "=").replaceAll("<>", "!=") +
                " -> " + rhs.toString().replaceAll(tableName + ".", "").replaceAll("==", "=").replaceAll("<>", "!=") + "]";
    }

    public String toInnerString() {
        return predicateSet.toInnerString() + " -> " + rhs.toInnerString();
    }


    public String toREEString() {
        StringBuffer tables = new StringBuffer();
        StringBuffer joinCol = new StringBuffer();

        Map<String, PredicateSetTableMsg> tablMsg = predicateSet.getColumnNameStr();
        boolean isPair = false;
        for(PredicateSetTableMsg msg : tablMsg.values()) {
            if(ONLY_ONE < msg.getTableAlias().size()) {
                isPair = true;
                break;
            }
        }

        Map<String, PredicateSetTableMsg> colMap = generateFkMsg(tablMsg);

        StringBuffer reeStr = new StringBuffer(predicateSet.toREEString())
                .append(" -> ").append(rhs.toREEString());
        for (Map.Entry<String, PredicateSetTableMsg> table : colMap.entrySet()) {
            for(Integer alias : table.getValue().getTableAlias()) {
                tables.append(table.getKey()).append("(t").append(alias).append(") ^ ");
            }
            if(null != table.getValue().getJoinSet()) {
                for (Predicate fk : table.getValue().getJoinSet()) {
                    boolean setIncludeFk = false;
                    for(Predicate p :predicateSet){
                        if(p.equals(fk)) {
                            setIncludeFk = true;
                            break;
                        }
                    }
                    if(!setIncludeFk) {
                        fk.setIndex_new_1(fk.getIndex1());
                        fk.setIndex_new_2(fk.getIndex2());
                        joinCol.append(fk.toREEString()).append(" ^ ");
                    }
                    if(isPair) {
                        fk.setIndex_new_1(fk.getIndex1() + 1);
                        fk.setIndex_new_2(fk.getIndex2() + 1);
                        joinCol.append(fk.toREEString()).append(" ^ ");
                    }
                }
            }
        }

        return tables.toString() + joinCol + reeStr;
    }

    public String toChekSQLStr() {
        Map<String, PredicateSetTableMsg> tablMsg = predicateSet.getColumnNameStr();

        for (Map.Entry<String, PredicateSetTableMsg> table : tablMsg.entrySet()) {
            Integer alias = table.getValue().getTableAlias().iterator().next();
            if(table.getValue().getTableAlias().size() <= ONLY_ONE) {
                table.getValue().addTableAlias(alias);
            }
        }
        Map<String, PredicateSetTableMsg> colMap = generateFkMsg(tablMsg);

        StringBuffer joinCol = new StringBuffer();

        int aliasInt = 0;
        StringBuffer cols = new StringBuffer();
        Map<String, String> aliasMap = new HashMap<>();

        //构造表名及基础条件
        for(Map.Entry<String, PredicateSetTableMsg> table : colMap.entrySet()) {
            cols.append(" ON " + table.getKey()).append("(")
                    .append(StringUtils.join(table.getValue().getColNames(), ","))
                    .append(") WITH t")
                    .append(StringUtils.join(table.getValue().getTableAlias(), ",t"))
                    .append(" AS r" + aliasInt);
            aliasMap.put(table.getKey(), "r" + aliasInt);
            aliasInt++;
            if(null != table.getValue().getPredicateSet()) {
                StringBuffer sb = new StringBuffer(" WHERE");
                for(Predicate p : table.getValue().getPredicateSet()) {
                    if(!p.equals(rhs)) {
                        sb.append(p.toCheckSQLStr() + " and");
                    }
                }
                cols.append(sb.substring(0, sb.length() - 4));
            }
        }
        //构造外键
        for(Map.Entry<String, PredicateSetTableMsg> table : colMap.entrySet()) {
            if(null != table.getValue().getJoinSet()) {
                Map<String, StringBuffer> joinAliasMap = new HashMap<>();
                for (Predicate fk : table.getValue().getJoinSet()) {
                    String[] joinTables = fk.getUseTableName()
                            .split(MlsConstant.RD_JOINTABLE_SPLIT_STR);
                    String joinAlias1 = aliasMap.get(joinTables[0]);
                    String joinAlias2 = aliasMap.get(joinTables[1]);
                    String joinSql = fk.toCheckSQLStr()
                            .replace("t" + fk.getIndex1() + ".", joinAlias1 + ".")
                            .replace("t" + fk.getIndex2() + ".", joinAlias2 + ".");
                    String key = joinAlias1+ "_" + joinAlias2;
                    if(!joinAliasMap.containsKey(key)) {
                        joinAliasMap.put(key, new StringBuffer());
                    }
                    joinAliasMap.get(key).append(joinSql + " AND");
                }
                for(Map.Entry<String , StringBuffer> entry : joinAliasMap.entrySet()) {
                    String[] joinTables =entry.getKey().split("_");
                    joinCol.append(" JOIN(").append(joinTables[0]).append(",")
                            .append(joinTables[1]).append(")").append(" WHERE")
                            .append(entry.getValue().substring(0, entry.getValue().length() - 4));
                }
            }
        }

        StringBuffer checkStr = new StringBuffer(cols.toString());
        if(joinCol.length() > 0) {
            checkStr.append(joinCol.toString());
        }
        checkStr.append(" check").append(rhs.toCheckSQLStr());
        return checkStr.toString();
    }


    public String toCorrectSQLStr() {
        if(rhs.getOperator().equals(Operator.EQUAL)) {
            String correctStr = toChekSQLStr();
            correctStr = correctStr.replace(" check ", " correct ");
            return correctStr;
        }
        return "";
    }

    private Map<String, PredicateSetTableMsg> generateFkMsg(Map<String, PredicateSetTableMsg> colMap) {
        if(ONLY_ONE < colMap.keySet().size() && null != FKMap) {
            String [] tableNames = colMap.keySet().toArray(new String []{});
            for(int i = 0; i < tableNames.length; i++) {
                for(int j = i+1; j < tableNames.length; j++) {
                    String useTables = tableNames[i] + MlsConstant.RD_JOINTABLE_SPLIT_STR + tableNames[j];
                    if(!FKMap.containsKey(useTables)) {
                        useTables = tableNames[j] + MlsConstant.RD_JOINTABLE_SPLIT_STR + tableNames[i];
                        if(!FKMap.containsKey(useTables)) {
                            continue;
                        }
                    }

                    addFkIntoTable(FKMap.get(useTables), colMap, useTables);
                }
            }
        }

        return colMap;
    }

    private void addFkIntoTable(Set<Predicate> fkSet, Map<String, PredicateSetTableMsg> colMap,
            String useTables) {
        for(Predicate fk : fkSet) {
            String [] joinTables = useTables.split(MlsConstant.RD_JOINTABLE_SPLIT_STR);

            Integer alias = colMap.get(joinTables[0]).getTableAlias().iterator().next();
            fk.setIndexOperand1(alias);
            alias = colMap.get(joinTables[1]).getTableAlias().iterator().next();
            fk.setIndexOperand2(alias);

//            Predicate fkpredict = new Predicate(fk.getOperator(), fk.getOperand1(), fk.getOperand2());
//            fkpredict.setIndexOperand1(alias1);
//            fkpredict.setIndexOperand2(alias2);
//            if(alias1 > alias2) {
//                fkpredict = new Predicate(fk.getOperator(), fk.getOperand2(), fk.getOperand1());
//            }
            colMap.get(joinTables[0]).addJoinSet(fk);
            String [] array = fk.getOperand1().toString_(fk.getIndex1()).split("[.]");
            colMap.get(joinTables[0]).addColName(array[2].substring(array[2].lastIndexOf("__")+2));

            array = fk.getOperand2().toString_(fk.getIndex2()).split("[.]");
            colMap.get(joinTables[1]).addColName(array[2].substring(array[2].lastIndexOf("__")+2));

//            for(Predicate p : predicateSet) {
//                if(!p.equals(fk)){
//                    predicateSet.add(fk);
//                }
//            }
        }
    }


    @Override
    public int hashCode() {
        return (predicateSet.toString() + "->" + this.rhs.toString()).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        DenialConstraint other = (DenialConstraint) obj;
        return other.predicateSet.equals(this.predicateSet) && other.rhs.equals(this.rhs);
    }

    /* Features */

    private long support;  //support数量
    private double wsupport;
    private double confidence;
    private double cosine;
    private double coverage;
    private long violations; //如果这条是x->y。那么violations代表x->!y 的数量。容错率就是support/(support+violations)
    // added by yyssl88
    private Predicate predicate_l_0;

    private String rule;
    private String checkSQL;
    private String correctSQL;
    private String supportExamples;
    private String unSupportExamples;

    private double subjective_feature;

    public double getSubjective_feature() {
        return subjective_feature;
    }

    public void setSubjective_feature(double s) {
        this.subjective_feature = s;
    }

    public void setPredicate_l_0(Predicate l_0) {
        this.predicate_l_0 = l_0;
    }

    public Predicate getPredicate_l_0() {
        return predicate_l_0;
    }

    public void setConfidenceMeasures(long support, double confidence, double cosine) {
        this.support = support;
        this.confidence = confidence;
        this.cosine = cosine;

    }

    public void setConfidenceMeasures(long support, double wsupport, double confidence, double cosine) {

        this.support = support;
        this.wsupport = wsupport;
        this.confidence = confidence;
        this.cosine = cosine;

    }

    public double getWsupport() {
        return wsupport;
    }

    public void setWsupport(double wsupport) {
        this.wsupport = wsupport;
    }

    public long getSupport() {
        return support;
    }

    public void setSupport(long support) {
        this.support = support;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    public double getCosine() {
        return cosine;
    }

    public void setCosine(double cosine) {
        this.cosine = cosine;
    }

    public double getCoverage() {
        return coverage;
    }

    public void setCoverage(double coverage) {
        this.coverage = coverage;
    }

    public long getViolations() {
        return violations;
    }

    public void setViolations(long violations) {
        this.violations = violations;
    }

    public de.metanome.algorithm_integration.results.DenialConstraint toResult() {

        PredicateVariable[] predicates = new PredicateVariable[predicateSet.size()];
        int i = 0;
        for (Predicate p : predicateSet) {
            predicates[i] = new PredicateVariable(p.getOperand1().getColumn().getColumnIdentifier(),
                    p.getOperand1().getIndex(), p.getOperator(), p.getOperand2().getColumn().getColumnIdentifier(),
                    p.getOperand2().getIndex());
            ++i;
        }


        return new de.metanome.algorithm_integration.results.DenialConstraint(predicates);
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public String getCheckSQL() {
        return checkSQL;
    }

    public void setCheckSQL(String checkSQL) {
        this.checkSQL = checkSQL;
    }

    public String getCorrectSQL() {
        return correctSQL;
    }

    public void setCorrectSQL(String correctSQL) {
        this.correctSQL = correctSQL;
    }

}
