package sics.seiois.mlsserver.service.impl;

import com.esotericsoftware.kryo.Kryo;
import de.metanome.algorithm_integration.Operator;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.bitset.LongBitSet;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.input.ParsedColumn;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLI;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.indexers.CategoricalTpIDsIndexer;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.operands.ColumnOperand;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.utils.WorkUnit;
import sics.seiois.mlsserver.biz.der.mining.utils.WorkUnits;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MyKyroRegistrator implements KryoRegistrator {

    private static final Logger logger = LoggerFactory.getLogger(MyKyroRegistrator.class);

    @Override
    public void registerClasses(Kryo ky) {
//        ky.register(sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateBuilder.class);
        try {
            ky.register(Object[].class);
            ky.register(PredicateBuilder.class);
            ky.register(Class.forName("scala.collection.mutable.WrappedArray$ofRef"));
            ky.register(edu.stanford.nlp.util.Interval.class);
            ky.register(Input.class);
            ky.register(java.util.HashMap.class);
            ky.register(PredicateSet.class);
            ky.register(Predicate.class);
            ky.register(Operator.class);
            ky.register(LongBitSet.class);
            ky.register(ArrayList.class);
            ky.register(List.class);
            ky.register(Map.class);
            ky.register(ColumnOperand.class);
            ky.register(ParsedColumn.class);
            ky.register(PLI.class);
            ky.register(CategoricalTpIDsIndexer.class);
            ky.register(java.lang.Class.class);
            ky.register(com.google.common.collect.HashMultiset.class);
            ky.register(java.util.HashSet.class);
            ky.register(WorkUnit.class);
            ky.register(WorkUnits.class);


        } catch (ClassNotFoundException e) {
            logger.error("register class faied", e);
        }

    }
}
