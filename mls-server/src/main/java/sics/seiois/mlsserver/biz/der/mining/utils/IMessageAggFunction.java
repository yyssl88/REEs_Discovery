package sics.seiois.mlsserver.biz.der.mining.utils;

import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.mining.Message;

import java.util.ArrayList;
import java.util.List;

public class IMessageAggFunction implements Function2<List<Message>, List<Message>, List<Message>> {
   private static Logger log = LoggerFactory.getLogger(IMessageAggFunction.class);

   @Override
    public List<Message> call(List<Message> mg_1, List<Message> mg_2) {
       if (mg_1 == null) {
           return mg_2;
       }

       if (mg_2 == null) {
           return mg_1;
       }

       List<Message> allmg = new ArrayList<>();
       allmg.addAll(mg_1);
       allmg.addAll(mg_2);
       return allmg;

   }
}
