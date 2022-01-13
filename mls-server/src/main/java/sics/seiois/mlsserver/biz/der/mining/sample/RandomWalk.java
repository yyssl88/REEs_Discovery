package sics.seiois.mlsserver.biz.der.mining.sample;

import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLI;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLISection;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.model.PredicateConfig;

import java.util.*;

public class RandomWalk {
    private long dataNum;
    private List<HashSet<Long>> selectedTids;
    private long sampleNum;
    private int maxTupleNum;
    private Random random;

//    private int[] tidStarts;
//    private int[] tupleNums;

    public RandomWalk(double sampleRatio, long dataNum, int maxTupleNum, Input input) {
        this.selectedTids = new ArrayList<>();
        this.dataNum = dataNum;
        this.sampleNum = (long)(sampleRatio * dataNum);
        this.maxTupleNum = maxTupleNum;
        this.random = new Random();
        this.random.setSeed(1234567);

//        int table_num = input.getNewTblList().size();
//        this.tidStarts = new int[table_num];
//        this.tupleNums = new int[table_num];
//        int idx = 0;
//        for (NewTBLInfo info : input.getNewTblList()) {
//            tidStarts[idx] = info.getTidStart();
//            tupleNums[idx] = info.getTupleNum();
//            idx = idx + 1;
//        }
    }

    /**
        predicates: only consider equality and ML predicates
     */
    public HashSet<Long> randomWalkSampling(List<Predicate> predicates) {
        HashSet<Long> selectedTidsOneRound = new HashSet<>();
        int psize = predicates.size();
        while (selectedTidsOneRound.size() < sampleNum) {
            // randomly choose a predicate
            int p_id = this.random.nextInt(psize);
            Predicate predicate = predicates.get(p_id);
            List<PLISection> pliSection1 = predicate.getOperand1().getColumn().getPliSections();
            List<PLISection> pliSection2 = predicate.getOperand2().getColumn().getPliSections();
            String key1 = predicate.getTableName() + PredicateConfig.TBL_COL_DELIMITER + predicate.getOperand1().getColumn().getName();
            String key2 = predicate.getTableName() + PredicateConfig.TBL_COL_DELIMITER + predicate.getOperand2().getColumn().getName();

            // randomly choose a pli section for operand1
            int section_id1 = this.random.nextInt(pliSection1.size());
            PLI pli1 = pliSection1.get(section_id1).getPLI(key1);

            // randomly choose an attribute value for operand1
            int attr_value_id1 = this.random.nextInt(pli1.getValues().size());
            int attr_value = 0;
            int idx = 0;
            for (Integer value : pli1.getValues()) {
                if (idx == attr_value_id1) {
                    attr_value = value;
                    break;
                }
                idx = idx + 1;
            }

            // randomly choose a tuple id 'tid1' with attr value being 'attr_value'
            List<Integer> tupleids1 = pli1.getTpIDsForValue(attr_value);
            int choose_idx1 = this.random.nextInt(tupleids1.size());
            int tid1 = tupleids1.get(choose_idx1);

            // get another tuple id 'tid2' where the tuple pair <tid1, tid2> satisfies the randomly chosen predicate at first
            List<Integer> cand_secIds = new ArrayList<>();
            for (int secId = 0; secId < pliSection2.size(); secId++) {
                if (pliSection2.get(secId).getPLI(key2).getValues().contains(attr_value)) {
                    cand_secIds.add(secId);
                }
            }
            if (cand_secIds.size() == 0) { // no tid2 satisfied
                continue;
            }

            // randomly choose a pli section for operand2
            int section_id2 = this.random.nextInt(cand_secIds.size());
            PLI pli2 = pliSection2.get(section_id2).getPLI(key2);

            // randomly choose a tuple id 'tid2' with attr value being 'attr_value'
            List<Integer> tupleids2 = pli2.getTpIDsForValue(attr_value);
            int tid2 = tid1;
            while (tid1 == tid2) {
                int choose_idx2 = this.random.nextInt(tupleids2.size());
                tid2 = tupleids2.get(choose_idx2);
            }
            selectedTidsOneRound.add((long) (tid1 + pliSection1.get(section_id1).getTidStart()));
            selectedTidsOneRound.add((long) (tid2 + pliSection2.get(section_id2).getTidStart()));

            // get left tuples in path
            int curr_tid = tid2;
            for (int i = 0; i < this.maxTupleNum - 2; i++) {
                // randomly choose a predicate
                int new_p_id = this.random.nextInt(psize);
                Predicate new_predicate = predicates.get(new_p_id);
                List<PLISection> new_pliSection1 = new_predicate.getOperand1().getColumn().getPliSections();
                List<PLISection> new_pliSection2 = new_predicate.getOperand2().getColumn().getPliSections();
                String new_key1 = new_predicate.getTableName() + PredicateConfig.TBL_COL_DELIMITER + new_predicate.getOperand1().getColumn().getName();
                String new_key2 = new_predicate.getTableName() + PredicateConfig.TBL_COL_DELIMITER + new_predicate.getOperand2().getColumn().getName();

                // not finished, to be filled...

            }

        }
        return selectedTidsOneRound;
    }


    /**
        k round random walk sampling
     */
    public void kRoundRWSampling(List<Predicate> predicates, int k) {
        for (int i = 0; i < k; i++) {
            this.random.setSeed(i);
            this.selectedTids.add(randomWalkSampling(predicates));
        }
    }

}
