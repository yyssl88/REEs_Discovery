package sics.seiois.mlsserver.biz.der.mining;

import gnu.trove.list.array.TIntArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.input.InputLight;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.utils.*;

import java.util.*;

/*
    Now only consider REEs that only contain t_0.A = t_1.A predicates
 */


public class MultiTuplesRuleMiningOptConstant {
    private int max_num_tuples; // the maximum number of tuples a REE contains
    private InputLight inputLight;
    private long support;
    private float confidence;

    private long maxOneRelationNum;

    // tuple numbers of each relation
    private HashMap<String, Long> tupleNumberRelations;

    private long allCount;

    private static int BATCH_TUPLE_NUM = 1000;
    private static int ENUMERATE_RATIO = 100000;

    public MultiTuplesRuleMiningOptConstant(int max_num_tuples, InputLight inputLight, long support, float confidence,
                                            long maxOneRelationNum, long allCount, HashMap<String, Long> tupleNumberRelations) {
        this.max_num_tuples = max_num_tuples;
        this.inputLight = inputLight;
        this.support = support;
        this.confidence = confidence;
        this.maxOneRelationNum = maxOneRelationNum;
        this.allCount = allCount;
        this.tupleNumberRelations = tupleNumberRelations;
    }

    public ArrayList<Integer> createHashMap(ArrayList<Integer> list, List<Predicate> current, int pid) {
        ArrayList<Integer> res = new ArrayList<>();
        for (int line = 0; line < list.size(); line++) {
            boolean ifSatisfy = true;
            for (int ppid = 0; ppid < current.size(); ppid++) {
                // 遍历所有谓词
                Predicate currp = current.get(ppid);
                int v = currp.getOperand1().getColumnLight().getValueInt(pid, line);
                if (v != currp.getConstantInt()) {
                    ifSatisfy = false;
                    break;
                }
            }
            if (ifSatisfy) {
                res.add(line);
            }

        }
        return res;
    }


    public List<Message> validationMap1(WorkUnits unitSet, Predicate pBegin) {
        ArrayList<Message> messages = new ArrayList<>();
        List<WorkUnit> units = unitSet.getUnits();
        int[] pids = unitSet.getPids();

        PredicateSet sameSet = unitSet.getSameSet();
        ArrayList<Predicate> samePs = new ArrayList<>();
        if (sameSet.size() > 0) {
            for (Predicate p : sameSet) {
                samePs.add(p);
            }
        }

        PredicateSet currrentPs = unitSet.getCurrrent();
        // check whether t_0 and t_1 are in the same relations
        boolean ifSame = true;

        // currentSet does not contain same Set
        ArrayList<Predicate> currentSet = new ArrayList<>();
        Map<String, ArrayList<Predicate>> constantPs = new HashMap<>();
        for (Predicate p : currrentPs) {
            if (p.isConstant()) {
                // the map of "constantPs" already distinguishes t0 and t1
                constantPs.putIfAbsent(p.getOperand1().toString(), new ArrayList<>());
                constantPs.get(p.getOperand1().toString()).add(p);
                continue;
            }
        }

        // prepare statistic
        Long[] suppLHS = new Long[unitSet.getUnits().size()];
        Long[][] suppRHSs = new Long[suppLHS.length][];
        ArrayList<ArrayList<Predicate>> unitsRHSs = new ArrayList<>();
        for (int index = 0; index < suppLHS.length; index++) {
            suppLHS[index] = 0L;
            WorkUnit unit = unitSet.getUnits().get(index);
            ArrayList<Predicate> rhss = new ArrayList<>();
            for (Predicate p : unit.getRHSs()) {
                rhss.add(p);
            }
            unitsRHSs.add(rhss);
            Long[] supprhss = new Long[rhss.size()];
            for (int i = 0; i < rhss.size(); i++) {
                supprhss[i] = 0L;
            }
            suppRHSs[index] = supprhss;
        }


        ArrayList<Integer> pList = (ArrayList<Integer>) pBegin.getOperand1().getColumnLight().getPliLight(unitSet.getPids()[pBegin.getIndex1()]).getTpIDsForValue(pBegin.getConstantInt());
        pList = createHashMap(pList, samePs, unitSet.getPids()[pBegin.getIndex1()]);
        this.updateHyperCubeMap1(suppLHS, suppRHSs, unitSet, currentSet, pList, constantPs, pids);

        // statistic
        for (int index = 0; index < units.size(); index++) {
            WorkUnit unit = units.get(index);
            ArrayList<Predicate> rhsList = new ArrayList<>();
            for (Predicate p : unit.getRHSs()) {
                rhsList.add(p);
            }
            ArrayList<Predicate> currentList = new ArrayList<>();
            for (Predicate p : unit.getCurrrent()) {
                currentList.add(p);
            }

            // collect message
            Message message = new Message(currentList, suppLHS[index], suppLHS[index], 0);
            message.updateAllCurrentRHSsSupport(suppRHSs[index], rhsList);
            messages.add(message);
        }

        return messages;
    }

    /*
        constantPs: key [operand1().to_string()] -> [list]
     */
    private void updateHyperCubeMap1(Long[] suppLHS, Long[][] suppRHSs, WorkUnits unitSet, ArrayList<Predicate> currentSetB,
                                                List<Integer> lines, Map<String, ArrayList<Predicate>> constantPs, int[] pids) {
        Map<String, List<Integer>> prdMap = unitSet.getPredicateMap();
        List<ArrayList<Predicate>> unitRhs = new ArrayList<>();
        boolean[] ifConst = new boolean[unitSet.getUnits().size()];

        // initialize all work units
        for (int index = 0; index < unitSet.getUnits().size(); index++) {
            WorkUnit unit = unitSet.getUnits().get(index);
            ArrayList<Predicate> rhs = new ArrayList<>();
            for (Predicate p : unit.getRHSs()) {
                rhs.add(p);
            }
            unitRhs.add(rhs);
            ifConst[index] = true;
        }

        for (int lid = 0; lid < lines.size(); lid++) {
            int line = lines.get(lid);
            //遍历常数谓词
            for (Map.Entry<String, ArrayList<Predicate>> entry : constantPs.entrySet()) {
                Predicate currp = entry.getValue().get(0);
                int c = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
                for (Predicate curr : entry.getValue()) {
                    if (c != curr.getConstantInt().intValue()) {
                        // index is the script of work units
                        for (Integer index : prdMap.get(curr.toString())) {
//                            log.info(">>>>error index: {}", index);
                            ifConst[index.intValue()] = false;
                        }
                    }
                }
            }

            // insert into the hypercube
            for (int index = 0; index < unitSet.getUnits().size(); index++) {
                WorkUnit unit = unitSet.getUnits().get(index);
                if (ifConst[index]) {
                    suppLHS[index]++;
                    ArrayList<Predicate> pset = unitRhs.get(index);
                    for (int rhsID = 0; rhsID < pset.size(); rhsID++) {
                        Predicate rhs_ = pset.get(rhsID);
                        int v = rhs_.getOperand1().getColumnLight().getValueInt(pids[rhs_.getIndex1()], line);
                        if (v == rhs_.getConstantInt().intValue()) {
                            suppRHSs[index][rhsID]++;
                        }
                    }

                    ifConst[index] = true;

                }

            }
        }
    }

    private static Logger log = LoggerFactory.getLogger(MultiTuplesRuleMiningOptConstant.class);
}
