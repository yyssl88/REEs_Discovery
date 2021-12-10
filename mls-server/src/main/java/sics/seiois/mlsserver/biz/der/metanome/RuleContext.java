package sics.seiois.mlsserver.biz.der.metanome;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLI;
import sics.seiois.mlsserver.biz.der.metanome.predicates.ConstantPredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.model.PredicateConfig;

import java.util.*;

@Getter
@Setter
public class RuleContext {
    private static Logger log = LoggerFactory.getLogger(RuleContext.class);
    private Set<Predicate> predicatesHighSelectivity;
//    private Map<String, PLI> constantPli = new HashMap<>();//highselectivity常数对应的pli都存储到这里
    private transient Map<String, Map<Integer, Integer>> tupleValues = new HashMap<>();//key 是表名_列名，value是tid:单元格值
    private transient Map<String, Map<Integer, List<Integer>>> val2tidsInPred = new HashMap<>();
    private String taskId;

    public static void delNonConstantTid(Set<Integer> allHighTid, Map<Integer, Integer> tid2value) {
        for (Iterator<Map.Entry<Integer, Integer>> it = tid2value.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, Integer> item = it.next();
            if (!allHighTid.contains(item.getKey())) {
                it.remove();
            }
        }
    }

    public List<Integer> getConstantPredicatesPLI(Predicate cp) {
        String key = cp.getOperand1().getColumn().getTableName() + PredicateConfig.TBL_COL_DELIMITER +
                cp.getOperand1().getColumn().getName();
        if (val2tidsInPred.get(key) != null && val2tidsInPred.get(key).get(cp.getConstantInt()) != null) {
            return val2tidsInPred.get(key).get(cp.getConstantInt());
        }

        PLI pli;
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            String outPliPath =
                    PredicateConfig.MLS_TMP_HOME + taskId + "/all_pli/";

            Kryo kryo = new Kryo();
            kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
            com.esotericsoftware.kryo.io.Input input = new com.esotericsoftware.kryo.io.Input(fs.open(new Path(outPliPath + key)));
            pli = (PLI) kryo.readObject(input, PLI.class);
            log.debug("##pli: {},{}", key, pli.getPlis().size());
            input.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        if (pli != null) {
            log.debug("HHH8:{},{},{}", cp.getConstantInt(), pli.getTpIDsForValue(cp.getConstantInt()), pli);
            List<Integer> tids = pli.getTpIDsForValue(cp.getConstantInt());
            val2tidsInPred.putIfAbsent(key, new HashMap<>());
            val2tidsInPred.get(key).putIfAbsent(cp.getConstantInt(), tids);
            return tids;
        }
        log.debug("HHH9:{}", key);
        return null;
    }

}
