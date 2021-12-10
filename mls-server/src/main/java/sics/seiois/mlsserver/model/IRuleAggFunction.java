package sics.seiois.mlsserver.model;

import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import sics.seiois.mlsserver.biz.der.metanome.pojo.RuleResult;

/**
 * Created by friendsyh on 2020/8/10.
 */
public class IRuleAggFunction implements Function2<List<RuleResult>, List<RuleResult>, List<RuleResult>> {

    private static Logger log = LoggerFactory.getLogger(IRuleAggFunction.class);
    @Override
    public List<RuleResult> call(List<RuleResult> rule1, List<RuleResult> rule2) throws Exception {
        log.info("####[规则发现进行合并开始]");
//        log.info("####rule1:{}", rule1 == null ? "null" : rule1.size());
//        log.info("####rule2:{}", rule2 == null ? "null" : rule2.size());
        if (rule1 == null) {
            return rule2;
        }
        if (rule2 == null) {
            return rule1;
        }
        int count = 10;
        List<RuleResult> allRule = new ArrayList<RuleResult>();
//        for (RuleResult rule : rule1) {
//            allRule.add(rule);
//        }
//        for (RuleResult rule : rule2) {
//            allRule.add(rule);
//        }
        //allAll速度更快
        allRule.addAll(rule1);
        allRule.addAll(rule2);
        //输出部分样例规则用于展示，
        if(allRule .size() > 10000) {
            int size = allRule.size() / count;
            Random rand = new Random();
            for (int index = 0; index < count; index++) {
                int show = rand.nextInt(size) + (index * size);
                log.info(">>>> {}", allRule.get(show).getPrintString());
            }
        }
        log.info("####[规则发现进行合并完成]:rule1={},rule2={},total={}",rule1.size(),rule2.size(),allRule.size());
        return allRule;
    }
}
