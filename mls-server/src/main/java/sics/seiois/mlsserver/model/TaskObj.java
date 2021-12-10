package sics.seiois.mlsserver.model;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.Getter;
import lombok.Setter;
import sics.seiois.mlsserver.biz.der.metanome.RuleContext;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraintSet;
import sics.seiois.mlsserver.biz.der.metanome.evidenceset.TroveEvidenceSet;
import sics.seiois.mlsserver.biz.der.metanome.pojo.RuleResult;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;

import java.io.Serializable;
import java.util.*;

@Setter
@Getter
public class TaskObj implements KryoSerializable {
    PredicateSet currentPS;
    PredicateSet addablePS;
    long totalCount;//携带到executor中
    float interestingness; // the threshold of interestingness

    // new hybrid rule finding
    transient RuleContext ruleContext;

    transient List<RuleResult> partRR;//携带的变量
    transient DenialConstraintSet reeSet;//携带的变量
    transient TroveEvidenceSet es;

    public TaskObj() {
        currentPS = new PredicateSet();
        addablePS = new PredicateSet();
    }

    public Set<Predicate> getAddablePS() {
        Set<Predicate> addableSet = new HashSet<>();
        for (Predicate predicate : addablePS) {
            addableSet.add(predicate);
        }
        return addableSet;
    }

    public HashMap<String, Double> getConfidenceColumns() {
        HashMap<String, Double> confidences = new HashMap<>();
        for (Predicate p : currentPS) {
            String key = p.getOperand1().getColumn().getTableName() + PredicateConfig.TBL_COL_DELIMITER + p.getOperand1().getColumn().getName();
            if (!confidences.containsKey(key)) {
                confidences.put(key, 1.0);
            }
            key = p.getOperand2().getColumn().getTableName() + PredicateConfig.TBL_COL_DELIMITER + p.getOperand2().getColumn().getName();
            if (!confidences.containsKey(key)) {
                confidences.put(key, 1.0);
            }
        }

        for (Predicate p : addablePS) {
            String key = p.getOperand1().getColumn().getTableName() + PredicateConfig.TBL_COL_DELIMITER + p.getOperand1().getColumn().getName();
            if (!confidences.containsKey(key)) {
                confidences.put(key, 1.0);
            }
            key = p.getOperand2().getColumn().getTableName() + PredicateConfig.TBL_COL_DELIMITER + p.getOperand2().getColumn().getName();
            if (!confidences.containsKey(key)) {
                confidences.put(key, 1.0);
            }
        }
        return confidences;
    }


    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        if(currentPS == null || currentPS.size() ==0 ){
            sb.append(" currentPS is null ");
        } else {
            sb.append(" currentPS is :" + currentPS.toString());
        }
        if(addablePS == null || addablePS.size() ==0){
            sb.append(" addablePS is null ");
        } else {
            sb.append(" addablePS is :" + addablePS.toString());
        }
        return sb.toString();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, currentPS);
        kryo.writeObject(output, addablePS);
        output.writeLong(totalCount);
        output.writeFloat(interestingness);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.currentPS = kryo.readObject(input, PredicateSet.class);
        this.addablePS = kryo.readObject(input, PredicateSet.class);
        this.totalCount = input.readLong();
        this.interestingness = input.readFloat();
    }
}
