package sics.seiois.mlsserver.biz.der.mining.utils;

import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ILatticeConstantAggFunction implements Function2<LatticeConstant, LatticeConstant, LatticeConstant> {
   private static Logger log = LoggerFactory.getLogger(ILatticeConstantAggFunction.class);

   @Override
    public LatticeConstant call(LatticeConstant mg_1, LatticeConstant mg_2) {

       if (mg_1 == null && mg_2 == null) {
           return new LatticeConstant();
       }

       if (mg_1 == null) {
           return mg_2;
       }

       if (mg_2 == null) {
           return mg_1;
       }

       LatticeConstant allLattice = new LatticeConstant(mg_1.getMaxTupleNumPerRule());

       for (LatticeVertex lv : mg_1.getLatticeLevel().values()) {
           allLattice.addLatticeVertex(lv);
       }
       for (LatticeVertex lv : mg_2.getLatticeLevel().values()) {
           allLattice.addLatticeVertex(lv);
       }

       return allLattice;

   }
}
