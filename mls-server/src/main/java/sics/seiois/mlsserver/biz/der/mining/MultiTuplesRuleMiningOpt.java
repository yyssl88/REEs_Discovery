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


public class MultiTuplesRuleMiningOpt {
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

    public MultiTuplesRuleMiningOpt(int max_num_tuples, InputLight inputLight, long support, float confidence,
                                    long maxOneRelationNum, long allCount, HashMap<String, Long> tupleNumberRelations) {
        this.max_num_tuples = max_num_tuples;
        this.inputLight = inputLight;
        this.support = support;
        this.confidence = confidence;
        this.maxOneRelationNum = maxOneRelationNum;
        this.allCount = allCount;
        this.tupleNumberRelations = tupleNumberRelations;
    }

    private void updateHyperCube(Predicate pBegin, HyperCube hyperCube, ArrayList<Predicate> currentSetB, ArrayList<Predicate> rhss_arr, int[] pids, int indexUsed, int indexNotUsed) {
        TIntArrayList _list1 = null;
        if (indexUsed == 0) {
            _list1 = pBegin.getOperand1().getColumnLight().getValueIntList(pids[pBegin.getIndex1()]);
        } else {
            _list1 = pBegin.getOperand2().getColumnLight().getValueIntList(pids[pBegin.getIndex2()]);
        }
        finish:
        for (int line = 0; line < _list1.size(); line++) {
            int tid = line;
            TIntArrayList xValue = new TIntArrayList(currentSetB.size(), -1);
            boolean ifCont = true;
            for (int ppid = 0; ppid < currentSetB.size(); ppid++) {
                // 遍历所有谓词
                Predicate currp = currentSetB.get(ppid);
                if (currp.getIndex1() == indexNotUsed && currp.isConstant()) {
                    continue;
                }
                if (currp.getOperand1().getColumnLight().getValueIntLength(pids[currp.getIndex1()]) <= line) {
                    continue finish;
                }

                // consider t_0.A = c
                if (currp.getIndex1() == indexUsed && currp.isConstant()) {
                    int c = 0;
                    if (indexUsed == 0) {
                        c = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
                    } else {
                        c = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
                    }
                    if (c == currp.getConstantInt()) {
                        continue;
                    } else {
                        ifCont = false;
                        break;
                    }
                }
                int v = 0;
                if (indexUsed == 0) {
                    v = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
                } else {
                    v = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
                }
                xValue.add(v);
            }
            if (indexUsed == 0) hyperCube.addxValue0(xValue);
            else hyperCube.addxValue1(xValue);
            if (ifCont) {
                for (int rhsID = 0; rhsID < rhss_arr.size(); rhsID++) {
                    Predicate rhs_ = rhss_arr.get(rhsID);
                    int v = 0;
                    if (indexUsed == 0) {
                        v = rhs_.getOperand1().getColumnLight().getValueInt(pids[rhs_.getIndex1()], line);
                        hyperCube.addTupleT0(xValue, v, rhsID, line);
                    } else {
                        v = rhs_.getOperand2().getColumnLight().getValueInt(pids[rhs_.getIndex2()], line);
                        hyperCube.addTupleT1(xValue, v, rhsID, line);
                    }
                }
            }
        }
    }

    public List<Message> validation(ArrayList<Predicate> currentSet, PredicateSet rhss, int[] pids) {
        ArrayList<Predicate> rhss_arr = new ArrayList<>();
        for (Predicate rhs : rhss) {
            rhss_arr.add(rhs);
        }
        return validation(currentSet, rhss_arr, pids);
    }

    public List<Message> validation(ArrayList<Predicate> currentSet, ArrayList<Predicate> rhss, int[] pids) {


//        if (currentSet.get(0).toString().trim().equals("airports.t0.iso_region == airports.t1.iso_region")) {
//            System.out.println("Action !!!");
//        }

        ArrayList<Message> messages = new ArrayList<>();
        ArrayList<Predicate> rhss_arr = new ArrayList<>();
        for (Predicate rhs : rhss) {
            rhss_arr.add(rhs);
        }
//        ArrayList<ImmutablePair<Integer, Integer>> tupleStartIDs = this.computeTupleIDsStart(currentSet);
//
//        Predicate minP = null;
//        Long minSupp = Long.MAX_VALUE;
//        for (Predicate p : currentSet) {
//            if (p.isConstant()) {
//                continue;
//            }
//            if (minSupp > p.getSupport()) {
//                minP = p;
//                minSupp = p.getSupport();
//            }
//        }
//        if (minSupp > 0.1 * (this.maxOneRelationNum * this.maxOneRelationNum - this.maxOneRelationNum)) {
//
//            Message message = new Message(currentSet, 0, 0, 0);
//            // add into a list of messages
//            messages.add(message);
//            return messages;
//        }


        Predicate pBegin = null;
        int sc = 0;
        for (int i = 0; i < currentSet.size(); i++) {
            Predicate p = currentSet.get(i);
            if (!p.isConstant() && !p.isML()) {
                pBegin = p;
                sc = i;
                break;
            }
        }

        /* if partition ID of t_0 is larger than partition ID of t_1, then discard it */
        if (pids[pBegin.getIndex1()] > pids[pBegin.getIndex2()]) {
            Message message = new Message(currentSet, 0, 0, 0);
            // add into a list of messages
            messages.add(message);
            return messages;
        }

        // copy one without pBegin
        ArrayList<Predicate> currentSetB = new ArrayList<>();
        for (int i = 0; i < currentSet.size(); i++) {
            if (i == sc) {
                continue;
            }
            currentSetB.add(currentSet.get(i));
        }
        HyperCube hyperCube = new HyperCube(rhss_arr);

        this.updateHyperCube(pBegin, hyperCube, currentSetB, rhss_arr, pids, 0, 1);
        log.info(">>>> show size :{}", hyperCube.toSize());

        if (pids[pBegin.getIndex1()] != pids[pBegin.getIndex2()]) {
            this.updateHyperCube(pBegin, hyperCube, currentSetB, rhss_arr, pids, 1, 0);
        }
        log.info(">>>> show size :{}", hyperCube.toSize());

        // statistic
        if (pids[pBegin.getIndex1()] < pids[pBegin.getIndex2()]) {
            hyperCube.getStatistic(false);
        } else if (pids[pBegin.getIndex1()] == pids[pBegin.getIndex2()]) {
            hyperCube.getStatistic(true);
        }

        // collect message
        Message message = new Message(currentSet, hyperCube.getSupportX(), 0, 0);
//        Message message = new Message(currentSet, 10, 0, 0);

        message.updateAllCurrentRHSsSupport(hyperCube.getSuppRHSs(), rhss_arr);

        // add into a list of messages
        messages.add(message);
        return messages;

    }

//    public List<Message> validationMap(ArrayList<Predicate> currentSet, WorkUnits unitSet) {
//        ArrayList<Message> messages = new ArrayList<>();
//        List<WorkUnit> units = unitSet.getUnits();
//        int[] pids = unitSet.getPids();
//
//        Predicate pBegin = null;
//        for (Predicate p : currentSet) {
//            if (!p.isConstant() && !p.isML()) {
//                pBegin = p;
//                break;
//            }
//        }
//
//        /* if partition ID of t_0 is larger than partition ID of t_1, then discard it */
//        if (pids[pBegin.getIndex1()] > pids[pBegin.getIndex2()]) {
//            for(WorkUnit unit : units) {
//                ArrayList<Predicate> currentList = new ArrayList<>();
//                for (Predicate p : unit.getCurrrent()) {
//                    currentList.add(p);
//                }
//                Message message = new Message(currentList, 0, 0, 0);
//                // add into a list of messages
//                messages.add(message);
//            }
//            return messages;
//        }
//
//
////        for (int index = 0; index < unitSet.getUnits().size(); index++) {
////            WorkUnit unit = unitSet.getUnits().get(index);
////            ArrayList<Predicate> rhs = new ArrayList<>();
////            for (Predicate p : unit.getRHSs()) {
////                rhs.add(p);
////            }
////            HyperCube hyperCube = new HyperCube(rhs);
////            hyperCubes.add(hyperCube);
////        }
//        List<HyperCube> hyperCubes = this.updateHyperCubeMap(unitSet, currentSet, pids, pBegin);
//
//        // statistic
//        for (int index = 0; index < units.size(); index++) {
//            WorkUnit unit = units.get(index);
//            ArrayList<Predicate> rhsList = new ArrayList<>();
//            for (Predicate p : unit.getRHSs()) {
//                rhsList.add(p);
//            }
//            ArrayList<Predicate> currentList = new ArrayList<>();
//            for (Predicate p : unit.getCurrrent()) {
//                currentList.add(p);
//            }
//            HyperCube hyperCube = hyperCubes.get(index);
//            if (pids[pBegin.getIndex1()] < pids[pBegin.getIndex2()]) {
//                hyperCube.getStatistic(false);
//            } else if (pids[pBegin.getIndex1()] == pids[pBegin.getIndex2()]) {
//                hyperCube.getStatistic(true);
//            }
//
//            log.info(">>>>show hyperCube support : {} | rhs support: {} ", hyperCube.getSupportX(), hyperCube.getSuppRHSs());
//            // collect message
//            Message message = new Message(currentList, hyperCube.getSupportX(), 0, 0);
////        Message message = new Message(currentSet, 10, 0, 0);
//
//            message.updateAllCurrentRHSsSupport(hyperCube.getSuppRHSs(), rhsList);
//
//            messages.add(message);
//        }
//
//        return messages;
//    }
//
//    private List<HyperCube> updateHyperCubeMap(WorkUnits unitSet, ArrayList<Predicate> currentSetB, int[] pids,
//                                               Predicate pBegin) {
//        List<HyperCube> hyperCubes = new ArrayList<>();
//        Map<String, List<Integer>> prdMap = unitSet.getPredicateMap();
//        TIntArrayList[] xValues = new TIntArrayList[unitSet.getUnits().size()];
//        List<ArrayList<Predicate>> unitRhs = new ArrayList<>();
//
//        for (int index = 0; index < unitSet.getUnits().size(); index++) {
//            WorkUnit unit = unitSet.getUnits().get(index);
//            ArrayList<Predicate> rhs = new ArrayList<>();
//            for (Predicate p : unit.getRHSs()) {
//                rhs.add(p);
//            }
//            HyperCube hyperCube = new HyperCube(rhs);
//            hyperCubes.add(hyperCube);
//            unitRhs.add(rhs);
//            TIntArrayList xValue = new TIntArrayList(unit.getCurrrent().size(), -1);
//            xValues[index] = xValue;
//        }
//
//        TIntArrayList _list1 = pBegin.getOperand1().getColumnLight().getValueIntList(pids[pBegin.getIndex1()]);
//        TIntArrayList _list2 = pBegin.getOperand2().getColumnLight().getValueIntList(pids[pBegin.getIndex2()]);
//
//        for (int line = 0; line < _list1.size(); line++) {
////            int tid = line;
//            for (int ppid = 0; ppid < currentSetB.size(); ppid++) {
//                // 遍历所有谓词
//                Predicate currp = currentSetB.get(ppid);
//
//                // consider t_0.A = c
//                if (currp.isConstant()) {
//                    int c = 0;
//                    if (currp.getIndex1() == 0) {
//                        c = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
//                        for (Integer indexs : prdMap.get(currp.toString())) {
//                            xValues[indexs].add(c);
//                        }
//                    } else {
//                        c = currp.getConstantInt();
//                        for (Integer indexs : prdMap.get(currp.toString())) {
//                            xValues[indexs].add(c);
//                        }
//                    }
//                } else {
//                    int v = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
//                    for (Integer index : prdMap.get(currp.toString())) {
//                        xValues[index].add(v);
//                    }
//                }
//            }
//            for (int index = 0; index < unitSet.getUnits().size(); index++) {
//                WorkUnit unit = unitSet.getUnits().get(index);
//                hyperCubes.get(index).addxValue0(xValues[index]);
//                ArrayList<Predicate> pset = unitRhs.get(index);
//                for (int rhsID = 0; rhsID < pset.size(); rhsID++) {
//                    Predicate rhs_ = pset.get(rhsID);
//                    int v = rhs_.getOperand1().getColumnLight().getValueInt(pids[rhs_.getIndex1()], line);
//                    hyperCubes.get(index).addTupleT0(xValues[index], v, rhsID, line);
//                }
//
//                TIntArrayList xValue = new TIntArrayList(unit.getCurrrent().size(), -1);
//                xValues[index] = xValue;
//            }
//        }
//
//        for (int line = 0; line < _list2.size(); line++) {
////            int tid = line;
//            for (int ppid = 0; ppid < currentSetB.size(); ppid++) {
//                // 遍历所有谓词
//                Predicate currp = currentSetB.get(ppid);
//
//                // consider t_0.A = c
//                if (currp.isConstant()) {
//                    int c = 0;
//                    if (currp.getIndex1() == 1) {
//                        c = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
//                        for (Integer index : prdMap.get(currp.toString())) {
//                            xValues[index.intValue()].add(c);
//                        }
//                    } else {
//                        c = currp.getConstantInt();
//                        for (Integer index : prdMap.get(currp.toString())) {
//                            xValues[index.intValue()].add(c);
//                        }
//                    }
//                } else {
//                    int v = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
//                    for (Integer index : prdMap.get(currp.toString())) {
//                        xValues[index.intValue()].add(v);
//                    }
//                }
//            }
//            for (int index = 0; index < unitSet.getUnits().size(); index++) {
//                WorkUnit unit = unitSet.getUnits().get(index);
//                hyperCubes.get(index).addxValue1(xValues[index]);
//                ArrayList<Predicate> pset = unitRhs.get(index);
//                for (int rhsID = 0; rhsID < pset.size(); rhsID++) {
//                    Predicate rhs_ = pset.get(rhsID);
//                    int v = rhs_.getOperand2().getColumnLight().getValueInt(pids[rhs_.getIndex2()], line);
//                    hyperCubes.get(index).addTupleT1(xValues[index], v, rhsID, line);
//                    rhsID++;
//                }
//
//                TIntArrayList xValue = new TIntArrayList(unit.getCurrrent().size(), -1);
//                xValues[index] = xValue;
//            }
//        }
//        return hyperCubes;
//    }


    public Map<TIntArrayList, List<Integer>> createHashMap(TIntArrayList list, List<Predicate> current, int pid, boolean isLeft) {
        Map<TIntArrayList, List<Integer>> hashMap = new HashMap<>();
        boolean ifconst = true;
        for (int line = 0; line < list.size(); line++) {
            TIntArrayList xValue = new TIntArrayList(current.size(), -1);
            for (int ppid = 0; ppid < current.size(); ppid++) {
                // 遍历所有谓词
                Predicate currp = current.get(ppid);
                int v = 0;
//                if (currp.isConstant()) {
//                    if (currp.getIndex1() == 0) {
//                        v = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
//                        if(v != currp.getConstantInt()) {
//                            ifconst = false;
//                            break;
//                        }
//                    } else {
//                        v = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
//                        if(v != currp.getConstantInt()) {
//                            ifconst = false;
//                            break;
//                        }
//                    }
//                } else {
//
//                }
                if (isLeft) {
                    v = currp.getOperand1().getColumnLight().getValueInt(pid, line);
                } else {
                    v = currp.getOperand2().getColumnLight().getValueInt(pid, line);
                }
                xValue.add(v);
            }
            if (ifconst) {
                hashMap.putIfAbsent(xValue, new ArrayList<>());
                hashMap.get(xValue).add(line);
            }
        }

        return hashMap;
    }


    public List<Message> validationMap(ArrayList<Predicate> currentSet, WorkUnits unitSet, Map<TIntArrayList, List<Integer>> leftMap,
                                       Map<TIntArrayList, List<Integer>> rightMap) {
        ArrayList<Message> messages = new ArrayList<>();
        List<WorkUnit> units = unitSet.getUnits();
        int[] pids = unitSet.getPids();


        Predicate pBegin = null;
        for (Predicate p : unitSet.getSameSet()) {
            if (!p.isConstant() && !p.isML()) {
                pBegin = p;
                break;
            }
        }

        /* if partition ID of t_0 is larger than partition ID of t_1, then discard it */
        if (pids[pBegin.getIndex1()] > pids[pBegin.getIndex2()]) {
            for(WorkUnit unit : units) {
                ArrayList<Predicate> currentList = new ArrayList<>();
                for (Predicate p : unit.getCurrrent()) {
                    currentList.add(p);
                }
                Message message = new Message(currentList, 0, 0, 0);
                // add into a list of messages
                messages.add(message);
            }
            return messages;
        }

        List<HyperCube> hyperCubes = new ArrayList<>();
        for (int index = 0; index < unitSet.getUnits().size(); index++) {
            WorkUnit unit = unitSet.getUnits().get(index);
            ArrayList<Predicate> rhs = new ArrayList<>();
            for (Predicate p : unit.getRHSs()) {
                rhs.add(p);
            }
            HyperCube hyperCube = new HyperCube(rhs);
            hyperCubes.add(hyperCube);
        }
        int lcount = 0;
        int rcount = 0;
        for (Map.Entry<TIntArrayList, List<Integer>> entry : leftMap.entrySet()) {
            lcount += entry.getValue().size();
            if (rightMap.containsKey(entry.getKey())) {
                this.updateHyperCubeMap(hyperCubes, unitSet, currentSet, pids, entry.getValue(),
                        rightMap.get(entry.getKey()), entry.getKey());
                rcount += rightMap.get(entry.getKey()).size();
            }
        }
        log.info(">>>>total count : {} | {}", lcount, rcount);
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
            HyperCube hyperCube = hyperCubes.get(index);
            if (pids[pBegin.getIndex1()] < pids[pBegin.getIndex2()]) {
                hyperCube.getStatistic(false);
            } else if (pids[pBegin.getIndex1()] == pids[pBegin.getIndex2()]) {
                hyperCube.getStatistic(true);
            }

            log.info(">>>>show hyperCube support : {} | rhs support: {} ", hyperCube.getSupportX(), hyperCube.getSuppRHSs());
            // collect message
            Message message = new Message(currentList, hyperCube.getSupportX(), 0, 0);
//        Message message = new Message(currentSet, 10, 0, 0);
            message.updateAllCurrentRHSsSupport(hyperCube.getSuppRHSs() , rhsList);

            messages.add(message);
        }

        return messages;
    }
    private List<HyperCube> updateHyperCubeMap(List<HyperCube> hyperCubes, WorkUnits unitSet, ArrayList<Predicate> currentSetB,
                                    int[] pids, List<Integer> left,  List<Integer> right, TIntArrayList key) {
        Map<String, List<Integer>> prdMap = unitSet.getPredicateMap();
        TIntArrayList[] xValues = new TIntArrayList[unitSet.getUnits().size()];
//        int [] counts = new int[unitSet.getUnits().size()];
//        int sameSize = unitSet.getSameSet().size();
        List<ArrayList<Predicate>> unitRhs = new ArrayList<>();

        for (int index = 0; index < unitSet.getUnits().size(); index++) {
            WorkUnit unit = unitSet.getUnits().get(index);
//            if(unit.getCurrrent().equals(unitSet.getSameSet())) {
//                hyperCube.setxValue0(key, left.size());
//                hyperCube.setxValue1(key, right.size());
//            }
            ArrayList<Predicate> rhs = new ArrayList<>();
            for (Predicate p : unit.getRHSs()) {
                rhs.add(p);
            }
            unitRhs.add(rhs);
            TIntArrayList xValue = new TIntArrayList(unit.getCurrrent().size(), -1);
            xValue.addAll(key);
            xValues[index] = xValue;
//            counts[index] = sameSize;
        }
//        log.info("####x value:{} | left:{} | right: {}", xValues, left.size(), right.size());
        for (Integer line : left) {
//            int tid = line;
            for (int ppid = 0; ppid < currentSetB.size(); ppid++) {
                // 遍历所有谓词
                Predicate currp = currentSetB.get(ppid);

                // consider t_0.A = c
                if (currp.isConstant()) {
                    int c = 0;
                    if (currp.getIndex1() == 0) {
                        c = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
                        for (Integer indexs : prdMap.get(currp.toString())) {
                            xValues[indexs].add(c);
//                            counts[indexs.intValue()]++;
                        }
                    } else {
                        c = currp.getConstantInt();
                        for (Integer indexs : prdMap.get(currp.toString())) {
                            xValues[indexs].add(c);
//                            counts[indexs.intValue()]++;
                        }
                    }
                } else {
                    int v = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
                    for (Integer index : prdMap.get(currp.toString())) {
                        xValues[index].add(v);
//                        counts[index.intValue()]++;
                    }
                }
            }
            for (int index = 0; index < unitSet.getUnits().size(); index++) {
                WorkUnit unit = unitSet.getUnits().get(index);
                hyperCubes.get(index).addxValue0(xValues[index]);
                ArrayList<Predicate> pset = unitRhs.get(index);
                for (int rhsID = 0; rhsID < pset.size(); rhsID++) {
                    Predicate rhs_ = pset.get(rhsID);
                    int v = rhs_.getOperand1().getColumnLight().getValueInt(pids[rhs_.getIndex1()], line);
                    hyperCubes.get(index).addTupleT0(xValues[index], v, rhsID, line);
                }


                TIntArrayList xValue = new TIntArrayList(unit.getCurrrent().size(), -1);
                xValue.addAll(key);
                xValues[index] = xValue;
//                counts[index] = sameSize;
            }
        }

        for (Integer line : right) {
//            int tid = line;
            for (int ppid = 0; ppid < currentSetB.size(); ppid++) {
                // 遍历所有谓词
                Predicate currp = currentSetB.get(ppid);

                // consider t_0.A = c
                if (currp.isConstant()) {
                    int c = 0;
                    if (currp.getIndex1() == 1) {
                        c = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
                        for (Integer index : prdMap.get(currp.toString())) {
                            xValues[index.intValue()].add(c);
//                            counts[index.intValue()]++;
                        }
                    } else {
                        c = currp.getConstantInt();
                        for (Integer index : prdMap.get(currp.toString())) {
                            xValues[index.intValue()].add(c);
//                            counts[index.intValue()]++;
                        }
                    }
                } else {
                    int v = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
                    for (Integer index : prdMap.get(currp.toString())) {
                        xValues[index.intValue()].add(v);
//                        counts[index.intValue()]++;
                    }
                }
            }
            for (int index = 0; index < unitSet.getUnits().size(); index++) {
                WorkUnit unit = unitSet.getUnits().get(index);
                hyperCubes.get(index).addxValue1(xValues[index]);
                ArrayList<Predicate> pset = unitRhs.get(index);
                for (int rhsID = 0; rhsID < pset.size(); rhsID++) {
                    Predicate rhs_ = pset.get(rhsID);
                    int v = rhs_.getOperand2().getColumnLight().getValueInt(pids[rhs_.getIndex2()], line);
                    hyperCubes.get(index).addTupleT1(xValues[index], v, rhsID, line);
//                    rhsID++;
                }

                TIntArrayList xValue = new TIntArrayList(unit.getCurrrent().size(), -1);
                xValue.addAll(key);
                xValues[index] = xValue;
//                counts[index] = sameSize;
            }
        }
        return hyperCubes;
    }

    public List<Message> validationMap1(WorkUnits unitSet, Predicate pBegin) {
        ArrayList<Message> messages = new ArrayList<>();
        List<WorkUnit> units = unitSet.getUnits();
        int[] pids = unitSet.getPids();

        /* if partition ID of t_0 is larger than partition ID of t_1, then discard it */
        if (pids[pBegin.getIndex1()] > pids[pBegin.getIndex2()]) {
            for(WorkUnit unit : units) {
                ArrayList<Predicate> currentList = new ArrayList<>();
                for (Predicate p : unit.getCurrrent()) {
                    currentList.add(p);
                }
                Message message = new Message(currentList, 0, 0, 0);
                // add into a list of messages
                messages.add(message);
            }
            return messages;
        }

        PredicateSet sameSet = unitSet.getSameSet();
        ArrayList<Predicate> samePs = new ArrayList<>();
        if (sameSet.size() > 0) {
            for (Predicate p : sameSet) {
                samePs.add(p);
            }
        }

        PredicateSet currrentPs = unitSet.getCurrrent();
        // currentSet does not contain constant predicates and same Set
        ArrayList<Predicate> currentSet = new ArrayList<>();
        Map<String, ArrayList<Predicate>> constantPs = new HashMap<>();
        for (Predicate p : currrentPs) {
            if (p.isConstant()) {
                // the map of "constantPs" already distinguishes t0 and t1
                constantPs.putIfAbsent(p.getOperand1().toString(), new ArrayList<>());
                constantPs.get(p.getOperand1().toString()).add(p);
                continue;
            }
            if (sameSet.containsPredicate(p)) {
                continue;
            }
            currentSet.add(p);
        }

//        Predicate pBegin = null;
//        for (Predicate p : unitSet.getSameSet()) {
//            if (!p.isConstant() && !p.isML()) {
//                pBegin = p;
//                break;
//            }
//        }



        List<HyperCube> hyperCubes = new ArrayList<>();
        for (int index = 0; index < unitSet.getUnits().size(); index++) {
            WorkUnit unit = unitSet.getUnits().get(index);
            ArrayList<Predicate> rhs = new ArrayList<>();
            for (Predicate p : unit.getRHSs()) {
                rhs.add(p);
            }
            HyperCube hyperCube = new HyperCube(rhs);
            hyperCubes.add(hyperCube);
        }

        TIntArrayList _list = pBegin.getOperand1().getColumnLight().getValueIntList(unitSet.getPids()[pBegin.getIndex1()]);
        // Key is the values of attributes, List<Integer> is the tuple IDs that satisfy the "key"
        Map<TIntArrayList, List<Integer>> leftMap = createHashMap(_list, samePs, unitSet.getPids()[pBegin.getIndex1()], true);
        /*
        if(pids[pBegin.getIndex1()] == pids[pBegin.getIndex2()]) {
            for (Map.Entry<TIntArrayList, List<Integer>> entry : leftMap.entrySet()) {
                this.updateHyperCubeMap1(hyperCubes, unitSet, currentSet, entry.getValue(), constantPs, pids, entry.getKey(), 0);
            }
        } else {
            _list = pBegin.getOperand2().getColumnLight().getValueIntList(unitSet.getPids()[pBegin.getIndex2()]);
            Map<TIntArrayList, List<Integer>> rightMap = createHashMap(_list, samePs, unitSet.getPids()[pBegin.getIndex2()], false);
            for (Map.Entry<TIntArrayList, List<Integer>> entry : leftMap.entrySet()) {
                TIntArrayList key = entry.getKey();
                if (rightMap.containsKey(key)) {
                    this.updateHyperCubeMap1(hyperCubes, unitSet, currentSet, entry.getValue(), constantPs, pids, key, 0);
                    this.updateHyperCubeMap1(hyperCubes, unitSet, currentSet, rightMap.get(key), constantPs, pids, key, 1);
                }
            }
        }
         */

        _list = pBegin.getOperand2().getColumnLight().getValueIntList(unitSet.getPids()[pBegin.getIndex2()]);
        Map<TIntArrayList, List<Integer>> rightMap = createHashMap(_list, samePs, unitSet.getPids()[pBegin.getIndex2()], false);
        for (Map.Entry<TIntArrayList, List<Integer>> entry : leftMap.entrySet()) {
            TIntArrayList key = entry.getKey();
            if (rightMap.containsKey(key)) {
                this.updateHyperCubeMap1(hyperCubes, unitSet, currentSet, entry.getValue(), constantPs, pids, key, 0);
                this.updateHyperCubeMap1(hyperCubes, unitSet, currentSet, rightMap.get(key), constantPs, pids, key, 1);
            }
        }

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
            HyperCube hyperCube = hyperCubes.get(index);
            hyperCube.getStatistic_new(false);
            /*
            if (pids[pBegin.getIndex1()] < pids[pBegin.getIndex2()]) {
                // hyperCube.getStatistic(false);
                hyperCube.getStatistic_new(false);
            } else if (pids[pBegin.getIndex1()] == pids[pBegin.getIndex2()]) {
                // hyperCube.getStatistic(true);
                hyperCube.getStatistic_new(true);
            }
             */

//            log.info(">>>>show hyperCube support : {} | rhs support: {} ", hyperCube.getSupportX(), hyperCube.getSuppRHSs());
            // collect message
            // Message message = new Message(currentList, hyperCube.getSupportX(), 0, 0);
            Message message = new Message(currentList, hyperCube.getSupportX(), hyperCube.getSupportXCP0(), hyperCube.getSupportXCP1());
            message.updateAllCurrentRHSsSupport(hyperCube.getSuppRHSs() , rhsList);
//            log.info(">>>>show message support : {} | rhs support: {} ", message.getCurrentSupp(), message.getAllCurrentRHSsSupport());
            messages.add(message);
        }

        return messages;
    }

    /*
        constantPs: key [operand1().to_string()] -> [list]
     */
    private List<HyperCube> updateHyperCubeMap1(List<HyperCube> hyperCubes, WorkUnits unitSet, ArrayList<Predicate> currentSetB,
                                                List<Integer> lines, Map<String, ArrayList<Predicate>> constantPs, int[] pids,
                                                TIntArrayList key, int indexUsed) {
        Map<String, List<Integer>> prdMap = unitSet.getPredicateMap();
        TIntArrayList[] xValues = new TIntArrayList[unitSet.getUnits().size()];
        List<ArrayList<Predicate>> unitRhs = new ArrayList<>();
        boolean [] ifConst = new boolean[unitSet.getUnits().size()];

        // initialize all work units
        for (int index = 0; index < unitSet.getUnits().size(); index++) {
            WorkUnit unit = unitSet.getUnits().get(index);
            ArrayList<Predicate> rhs = new ArrayList<>();
            for (Predicate p : unit.getRHSs()) {
                rhs.add(p);
            }
            unitRhs.add(rhs);
            TIntArrayList xValue = new TIntArrayList(unit.getCurrrent().size(), -1);
            xValue.addAll(key);
            xValues[index] = xValue;
            ifConst[index] = true;
        }

//        TIntArrayList _list1 = null;
//        if (indexUsed == 0) {
//            _list1 = pBegin.getOperand1().getColumnLight().getValueIntList(pids[pBegin.getIndex1()]);
//        } else {
//            _list1 = pBegin.getOperand2().getColumnLight().getValueIntList(pids[pBegin.getIndex2()]);
//        }

//        log.info(">>>> line count : {}", lines.size());
//        finish:
        int countLine = 0;
        for (int lid = 0; lid < lines.size(); lid++) {
            int line = lines.get(lid);
            //遍历常数谓词
            for (Map.Entry<String, ArrayList<Predicate>> entry : constantPs.entrySet()) {
                Predicate currp = entry.getValue().get(0);
                int c = 0;
                // consider t_0.A = c or t_1.A = c
                if (currp.getIndex1() == indexUsed) {
                // if (true) {
                    /*
                    if (indexUsed == 0) {
                        c = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
                    } else {
                        c = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
                    }
                     */
                    c = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
                    for (Predicate curr : entry.getValue()) {
                        if (c != curr.getConstantInt().intValue()) {
                            // index is the script of work units
                            for (Integer index : prdMap.get(curr.toString())) {
//                            log.info(">>>>error index: {}", index);
                                ifConst[index.intValue()] = false;
                            }
                        }
                    }
                } else {
                    continue;
                }
            }

//            for (int ppid = 0; ppid < samePs.size(); ppid++) {
//                // 遍历所有相同谓词
//                Predicate currp = samePs.get(ppid);
//                int v = 0;
//
////                if (currp.isConstant()) {
////                    int c = 0;
////                    // consider t_0.A = c
////                    if (currp.getIndex1() == indexUsed) {
////                        if (indexUsed == 0) {
////                            c = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
////                        } else {
////                            c = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
////                        }
////                        if (c == currp.getConstantInt()) {
////                            continue;
////                        } else {
////                            continue finish;
////                        }
////                    } else {
////                        continue;
////                    }
////                }
//                if (indexUsed == 0) {
//                    v = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
//                } else {
//                    v = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
//                }
//                for (int index = 0; index < xValues.length; index++) {
//                    xValues[index].add(v);
//                }
//            }

            // extract the value of a tuple 'line'
            for (int ppid = 0; ppid < currentSetB.size(); ppid++) {
                // 遍历所有谓词
                Predicate currp = currentSetB.get(ppid);
                int v = 0;
                if (indexUsed == 0) {
                    v = currp.getOperand1().getColumnLight().getValueInt(pids[currp.getIndex1()], line);
                } else {
                    v = currp.getOperand2().getColumnLight().getValueInt(pids[currp.getIndex2()], line);
                }
                for (Integer index : prdMap.get(currp.toString())) {
                    xValues[index].add(v);
                }
            }

            // insert into the hypercube
            for (int index = 0; index < unitSet.getUnits().size(); index++) {
                WorkUnit unit = unitSet.getUnits().get(index);
                if(ifConst[index]) {
                    if (indexUsed == 0) {
                        hyperCubes.get(index).addxValue0(xValues[index]);
                    } else {
                        hyperCubes.get(index).addxValue1(xValues[index]);
                    }
                    ArrayList<Predicate> pset = unitRhs.get(index);
                    for (int rhsID = 0; rhsID < pset.size(); rhsID++) {
                        Predicate rhs_ = pset.get(rhsID);
//                        if (rhs_.toString().trim().equals("ncvoter.t0.party == DEMOCRATIC")) {
//                            System.out.println();
//                        }
                        if (rhs_.isConstant()) {
                            if (rhs_.getIndex1() == indexUsed) {
                                int v = rhs_.getOperand1().getColumnLight().getValueInt(pids[rhs_.getIndex1()], line);
                                if (v == rhs_.getConstantInt().intValue()) {
                                    if (indexUsed == 0) {
                                        hyperCubes.get(index).addTupleT0(xValues[index], v, rhsID, line);
                                    } else {
                                        hyperCubes.get(index).addTupleT1(xValues[index], v, rhsID, line);
                                    }
                                }
                            }
                        } else {

                            if (indexUsed == 0) {
                                int v = rhs_.getOperand1().getColumnLight().getValueInt(pids[rhs_.getIndex1()], line);
//                            if (rhs_.toString().trim().equals("ncvoter.t0.party == DEMOCRATIC") && rhs_.isConstant() && v == rhs_.getConstantInt()) {
//                                System.out.println("Test Constants ... ");
//                            }
                                hyperCubes.get(index).addTupleT0(xValues[index], v, rhsID, line);
                            } else {
                                int v = rhs_.getOperand2().getColumnLight().getValueInt(pids[rhs_.getIndex2()], line);
                                hyperCubes.get(index).addTupleT1(xValues[index], v, rhsID, line);
                            }
                        }
                    }
                }

                TIntArrayList xValue = new TIntArrayList(unit.getCurrrent().size(), -1);
                xValue.addAll(key);
                xValues[index] = xValue;
                ifConst[index] = true;

                countLine++;
            }

        }
//        log.info(">>>>count line : {}, unit size: {}", countLine, unitSet.getUnits().size());
        return hyperCubes;
    }

    /**
     * Old version below
     *
     * @param currentOneGroup
     * @return
     */
    private ArrayList<ImmutablePair<Integer, Integer>> computeTupleIDsStart(ArrayList<Predicate> currentOneGroup) {

        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start = new ArrayList<>();
        for (int i = 0; i < currentOneGroup.size(); i++) {
            int t1 = this.inputLight.getTupleIDStart(currentOneGroup.get(i).getOperand1().getColumnLight().getTableName());
            int t2 = this.inputLight.getTupleIDStart(currentOneGroup.get(i).getOperand2().getColumnLight().getTableName());
            tuple_IDs_start.add(new ImmutablePair<>(t1, t2));
        }
        return tuple_IDs_start;
    }

    private ImmutablePair<Integer, Integer> computeTupleIDsStart(Predicate p) {

        int t1 = this.inputLight.getTupleIDStart(p.getOperand1().getColumnLight().getTableName());
        int t2 = this.inputLight.getTupleIDStart(p.getOperand2().getColumnLight().getTableName());

        return new ImmutablePair<Integer, Integer>(t1, t2);
    }

    private int computeTupleIDsStart(ParsedColumnLight<?> column) {

        int t = this.inputLight.getTupleIDStart(column.getTableName());
        return t;
    }

    private long checkOtherPredicates(int[] PIDs, int tid_left, int tid_right, ArrayList<Predicate> currentOneGroup, ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start,
                                      HashMap<Integer, ArrayList<Integer>> res, int t_left, int t_right, ArrayList<HashSet<Integer>> usedData) {
        long supp = 0;
        boolean f = true;
        for (int i = 0; i < currentOneGroup.size(); i++) {
            if (!currentOneGroup.get(i).calculateTp(PIDs[t_left], PIDs[t_right], tid_left, tid_right,
                    tuple_IDs_start.get(i).left, tuple_IDs_start.get(i).right)) {
                f = false;
                break;
            }
        }
        if (f) {
            supp++;
            if (res.containsKey(tid_left)) {
                res.get(tid_left).add(tid_right);
            } else {
                ArrayList<Integer> temp = new ArrayList<>();
                temp.add(tid_right);
                res.put(tid_left, temp);
            }
            usedData.get(t_left).add(tid_left);
            usedData.get(t_right).add(tid_right);
        }
        return supp;
    }

    public ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long> findResultsOneGroup_(ArrayList<Predicate> currentOneGroup,
                                                                                          int tid_left, int tid_right,
                                                                                          int[] PIDs,
                                                                                          ArrayList<HashSet<Integer>> usedData,
                                                                                          ArrayList<Boolean> usedTids) {
        HashMap<Integer, ArrayList<Integer>> res = new HashMap<>();
        if (currentOneGroup.size() == 0 || currentOneGroup == null) {
            return null;
        }

        Predicate minP = null;
        long minSupport = Long.MAX_VALUE;
        int minScript = 0;
        for (int i = 0; i < currentOneGroup.size(); i++) {
            Predicate p = currentOneGroup.get(i);
            if (p.isConstant()) {
                continue;
            }
            if (minSupport > p.getSupport()) {
                minP = p;
                minSupport = p.getSupport();
                minScript = i;
            }
        }
        log.info("The minimal SUPPORT ### : {} with Current X {}", minSupport, currentOneGroup);

        // check returned support
        long maxCount = maxOneRelationNum * maxOneRelationNum - maxOneRelationNum;
        double supp_ratio = 0.1;
        if (currentOneGroup == null || currentOneGroup.size() == 0 || minSupport < 0 || minSupport > supp_ratio * maxCount) {
            return null;
        }

        // if a predicate contains enumerate columns and its tuple ID is not <t_0, t_1>, then terminate
        //if (tid_left != 0 && tid_right != 1) {
        if (true) {
            long uniqueNum1 = minP.getOperand1().getColumnLight().getUniqueConstantNumber();
            long countR1 = this.tupleNumberRelations.get(minP.getOperand1().getColumnLight().getTableName());
            long uniqueNum2 = minP.getOperand2().getColumnLight().getUniqueConstantNumber();
            long countR2 = this.tupleNumberRelations.get(minP.getOperand2().getColumnLight().getTableName());
            if ((uniqueNum1 <= countR1 * 1.0 / ENUMERATE_RATIO) || (uniqueNum2 <= countR2 * 1.0 / ENUMERATE_RATIO)) {
                return null;
            }
            // if (tid_left != 0 && tid_right != 1) {
//            if (true) {
//                if (minP.getSupport() > maxOneRelationNum * 5000) {
//                    return null;
//                }
//            }
//            if (tid_left != 0 && tid_right != 1) {
//                if (minP.getSupport() > maxOneRelationNum * 10000) {
//                    return null;
//                }
//            }
        }

        currentOneGroup.remove(minScript);
        // ArrayList<ImmutablePair<Integer, Integer>> minList = minP.extractBiList();

        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start = this.computeTupleIDsStart(currentOneGroup);

        long supp = 0;
        if (minP.isML()) {
            ArrayList<ImmutablePair<Integer, Integer>> minList = minP.getMlOnePredicate();
            int beginTid1 = minP.getOperand1().getColumnLight().getTidsInterval(PIDs[tid_left]).left;
            int endTid1 = minP.getOperand1().getColumnLight().getTidsInterval(PIDs[tid_left]).right;
            int beginTid2 = minP.getOperand2().getColumnLight().getTidsInterval(PIDs[tid_right]).left;
            int endTid2 = minP.getOperand2().getColumnLight().getTidsInterval(PIDs[tid_right]).right;
            if (minList == null) {
                return null;
            }
            // filter
            for (ImmutablePair<Integer, Integer> pair : minList) {
                if (pair.left >= pair.right || pair.left < beginTid1 || pair.left >= endTid1 || pair.right < beginTid2 || pair.right >= endTid2) {
                    continue;
                }

                if (usedTids.get(tid_left) == Boolean.TRUE && (!usedData.get(tid_left).contains(pair.left))) {
                    continue;
                }

                if (usedTids.get(tid_right) == Boolean.TRUE && (!usedData.get(tid_right).contains(pair.right))) {
                    continue;
                }

                supp += this.checkOtherPredicates(PIDs, pair.left, pair.right, currentOneGroup, tuple_IDs_start, res, tid_left, tid_right,
                        usedData);

            }

        } else if (minP.isConstant()) {
            // throw new Exception("should not be constant predicate");
            return null;
        } else {

            PLILight pliPivot = minP.getOperand1().getColumnLight().getPliLight(PIDs[tid_left]);
            PLILight pliProbe = minP.getOperand2().getColumnLight().getPliLight(PIDs[tid_right]); // probing PLI
            if (pliPivot == null || pliProbe == null) {
                return null;
            }
            Collection<Integer> valuesPivot = pliPivot.getValues();
            for (Integer vPivot : valuesPivot) {
                if (vPivot == -1) continue;
                List<Integer> tidsProbe = pliProbe.getTpIDsForValue(vPivot);
                if (tidsProbe != null) {
                    List<Integer> tidsPivot = pliPivot.getTpIDsForValue(vPivot);

                    for (Integer tidPivot : tidsPivot) {
                        for (Integer tidProbe : tidsProbe) {
                            if (tidPivot >= tidProbe) {
                                continue;
                            }

                            if (usedTids.get(tid_left) == Boolean.TRUE && (!usedData.get(tid_left).contains(tidPivot))) {
                                continue;
                            }

                            if (usedTids.get(tid_right) == Boolean.TRUE && (!usedData.get(tid_right).contains(tidProbe))) {
                                continue;
                            }

                            supp += this.checkOtherPredicates(PIDs, tidPivot, tidProbe, currentOneGroup, tuple_IDs_start, res, tid_left, tid_right, usedData);

                        }
                    }

                }
            }
        }

        return new ImmutablePair<>(res, supp);
    }

    public ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long> findResultsOneGroup_(ArrayList<Predicate> currentOneGroup_,
                                                                                          int tid_left, int tid_right,
                                                                                          int[] PIDs,
                                                                                          ArrayList<HashSet<Integer>> usedData,
                                                                                          ArrayList<Boolean> usedTids,
                                                                                          HashSet<Integer> validConstantTIDs_left,
                                                                                          HashSet<Integer> validConstantTIDs_right) {
        // copy; in case that some elements are deleted
        ArrayList<Predicate> currentOneGroup = new ArrayList<>();
        for (Predicate p : currentOneGroup_) {
            currentOneGroup.add(p);
        }

        HashMap<Integer, ArrayList<Integer>> res = new HashMap<>();
        if (currentOneGroup.size() == 0 || currentOneGroup == null) {
            return null;
        }

        Predicate minP = null;
        long minSupport = Long.MAX_VALUE;
        int minScript = 0;
        for (int i = 0; i < currentOneGroup.size(); i++) {
            Predicate p = currentOneGroup.get(i);
            if (p.isConstant()) {
                continue;
            }
            if (minSupport > p.getSupport()) {
                minP = p;
                minSupport = p.getSupport();
                minScript = i;
            }
        }
        log.info("The minimal SUPPORT ### : {} with Current X {}", minSupport, currentOneGroup);

        // check returned support
        long maxCount = maxOneRelationNum * maxOneRelationNum - maxOneRelationNum;
        double supp_ratio = 0.1;
        if (currentOneGroup == null || currentOneGroup.size() == 0 || minSupport < 0 || minSupport > supp_ratio * maxCount) {
//        if (currentOneGroup == null || currentOneGroup.size() == 0 || minSupport < 0) {
            return null;
        }

        // if a predicate contains enumerate columns and its tuple ID is not <t_0, t_1>, then terminate
        //if (tid_left != 0 && tid_right != 1) {
        if (true) {
            long uniqueNum1 = minP.getOperand1().getColumnLight().getUniqueConstantNumber();
            long countR1 = this.tupleNumberRelations.get(minP.getOperand1().getColumnLight().getTableName());
            long uniqueNum2 = minP.getOperand2().getColumnLight().getUniqueConstantNumber();
            long countR2 = this.tupleNumberRelations.get(minP.getOperand2().getColumnLight().getTableName());
//            if ( (uniqueNum1 <= countR1 * 1.0 / ENUMERATE_RATIO) || (uniqueNum2 <= countR2 * 1.0 / ENUMERATE_RATIO) ) {
//                return null;
//            }
            // if (tid_left != 0 && tid_right != 1) {
//            if (true) {
//                if (minP.getSupport() > maxOneRelationNum * 5000) {
//                    return null;
//                }
//            }
//            if (tid_left != 0 && tid_right != 1) {
//                if (minP.getSupport() > maxOneRelationNum * 10000) {
//                    return null;
//                }
//            }
        }

        currentOneGroup.remove(minScript);
        // ArrayList<ImmutablePair<Integer, Integer>> minList = minP.extractBiList();

        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start = this.computeTupleIDsStart(currentOneGroup);

        long supp = 0;
        if (minP.isML()) {
            ArrayList<ImmutablePair<Integer, Integer>> minList = minP.getMlOnePredicate();
            int beginTid1 = minP.getOperand1().getColumnLight().getTidsInterval(PIDs[tid_left]).left;
            int endTid1 = minP.getOperand1().getColumnLight().getTidsInterval(PIDs[tid_left]).right;
            int beginTid2 = minP.getOperand2().getColumnLight().getTidsInterval(PIDs[tid_right]).left;
            int endTid2 = minP.getOperand2().getColumnLight().getTidsInterval(PIDs[tid_right]).right;
            if (minList == null) {
                return null;
            }
            // filter
            for (ImmutablePair<Integer, Integer> pair : minList) {
                if (pair.left >= pair.right || pair.left < beginTid1 || pair.left >= endTid1 || pair.right < beginTid2 || pair.right >= endTid2) {
                    continue;
                }

                if (usedTids.get(tid_left) == Boolean.TRUE && (!usedData.get(tid_left).contains(pair.left))) {
                    continue;
                }

                if (usedTids.get(tid_right) == Boolean.TRUE && (!usedData.get(tid_right).contains(pair.right))) {
                    continue;
                }

                // filter --- valid tids satisfying constant predicates
                if (validConstantTIDs_left != null && (!validConstantTIDs_left.contains(pair.left))) {
                    continue;
                }
                if (validConstantTIDs_right != null && (!validConstantTIDs_right.contains(pair.right))) {
                    continue;
                }

                supp += this.checkOtherPredicates(PIDs, pair.left, pair.right, currentOneGroup, tuple_IDs_start, res, tid_left, tid_right,
                        usedData);

            }

        } else if (minP.isConstant()) {
            // throw new Exception("should not be constant predicate");
            return null;
        } else {

            PLILight pliPivot = minP.getOperand1().getColumnLight().getPliLight(PIDs[tid_left]);
            PLILight pliProbe = minP.getOperand2().getColumnLight().getPliLight(PIDs[tid_right]); // probing PLI
            if (pliPivot == null || pliProbe == null) {
                return null;
            }
            Collection<Integer> valuesPivot = pliPivot.getValues();
            for (Integer vPivot : valuesPivot) {
                if (vPivot == -1) continue;
                List<Integer> tidsProbe = pliProbe.getTpIDsForValue(vPivot);
                if (tidsProbe != null) {
                    List<Integer> tidsPivot = pliPivot.getTpIDsForValue(vPivot);

                    for (Integer tidPivot : tidsPivot) {
                        // filter --- valid TIDs
                        if (validConstantTIDs_left != null && (!validConstantTIDs_left.contains(tidPivot))) {
                            continue;
                        }

                        for (Integer tidProbe : tidsProbe) {
                            if (tidPivot >= tidProbe) {
                                continue;
                            }
                            // filter --- valid TIDs
                            if (validConstantTIDs_right != null && (!validConstantTIDs_right.contains(tidProbe))) {
                                continue;
                            }

                            if (usedTids.get(tid_left) == Boolean.TRUE && (!usedData.get(tid_left).contains(tidPivot))) {
                                continue;
                            }

                            if (usedTids.get(tid_right) == Boolean.TRUE && (!usedData.get(tid_right).contains(tidProbe))) {
                                continue;
                            }

                            supp += this.checkOtherPredicates(PIDs, tidPivot, tidProbe, currentOneGroup, tuple_IDs_start, res, tid_left, tid_right, usedData);

                        }
                    }

                }
            }
        }

        return new ImmutablePair<>(res, supp);
    }


    /*
        all predicates of RHSs are <t_0, t_1> or <t_0, t_0>
     */
    private void uniVerify(int[] PIDs, Long[] counts, HashSet<Integer> resultsX, ArrayList<Predicate> uniPredicateRHSs) {

        int t_left = 0;
        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start_rhs = this.computeTupleIDsStart(uniPredicateRHSs);
        for (Integer tid : resultsX) {
            for (int i = 0; i < uniPredicateRHSs.size(); i++) {

                if (uniPredicateRHSs.get(i).calculateTp(PIDs[t_left], PIDs[t_left], tid, -1, tuple_IDs_start_rhs.get(i).left, -1)) {
                    counts[i]++;
                }
            }
        }
    }

    private void biVerify(int[] PIDs, Long[] counts, ArrayList<ImmutablePair<Integer, Integer>> resultsX, ArrayList<Predicate> biPredicateRHSs) {

        int t_left = 0, t_right = 1;
        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start_rhs = this.computeTupleIDsStart(biPredicateRHSs);
        for (ImmutablePair<Integer, Integer> pair : resultsX) {
            for (int i = 0; i < biPredicateRHSs.size(); i++) {
                if (biPredicateRHSs.get(i).calculateTp(PIDs[t_left], PIDs[t_right], pair.left, pair.right, tuple_IDs_start_rhs.get(i).left, tuple_IDs_start_rhs.get(i).right)) {
                    counts[i]++;
                }
            }
        }
    }


    /******************************************************************************************************************************
     *  new validation with [start, end]
     ******************************************************************************************************************************/

    private HashSet<Integer> findResultsOneConstantPredicatesGroup(int tuple_variable, int[] PIDs, ArrayList<Predicate> constantPredicatesPrefix) {
        HashSet<Integer> res = new HashSet<>();

        // 1. find the predicate of the smallest support in each group
        Predicate minP = null;
        int minSc = -1;
        long minSupport = Long.MAX_VALUE;
        for (int i = 0; i < constantPredicatesPrefix.size(); i++) {
            Predicate cp = constantPredicatesPrefix.get(i);
            if (minSupport > cp.getSupport()) {
                minP = cp;
                minSupport = cp.getSupport();
                minSc = i;
            }
        }

        int pid = PIDs[tuple_variable];
        ImmutablePair<Integer, Integer> tuple_IDs_start = this.computeTupleIDsStart(minP);

        constantPredicatesPrefix.remove(minSc);
        // 2. retrieve the list
        PLILight pli = minP.getOperand1().getColumnLight().getPliLight(PIDs[tuple_variable]);
        if (pli == null) {
            return null;
        }
        List<Integer> _list = pli.getTpIDsForValue(minP.getConstantInt());
        for (Integer tid : _list) {
            boolean f = true;
            // check other constant predicates of the same tuple_variable
            for (Predicate cp_ : constantPredicatesPrefix) {
                if (!cp_.calculateTpConstant(pid, tid, tuple_IDs_start.left)) {
                    f = false;
                    break;
                }
            }
            if (f) {
                res.add(tid);
            }
        }
        return res;

    }

    private HashMap<Integer, HashSet<Integer>> retrieveSatisfiedConstantTIDs(int[] PIDs, ArrayList<Predicate> constantPredicatesPrefix) {
        HashMap<Integer, HashSet<Integer>> res = new HashMap<>();
        // 1. group different constant predicate according to tuple variables
        HashMap<Integer, ArrayList<Predicate>> constantPredicatesPrefixGroup = new HashMap<>();
        for (Predicate cp : constantPredicatesPrefix) {
            int t_var = cp.getIndex1();
            if (constantPredicatesPrefixGroup.containsKey(t_var)) {
                constantPredicatesPrefixGroup.get(t_var).add(cp);
            } else {
                ArrayList<Predicate> _list = new ArrayList<>();
                _list.add(cp);
                constantPredicatesPrefixGroup.put(t_var, _list);
            }
        }

        // 2. find valid TIDs
        for (Map.Entry<Integer, ArrayList<Predicate>> entry : constantPredicatesPrefixGroup.entrySet()) {
            res.put(entry.getKey(), this.findResultsOneConstantPredicatesGroup(entry.getKey(), PIDs, entry.getValue()));
        }

        return res;
    }

    /*
        check whether X -> p_0 has duplicate constant predicates, e.g., t.A = c, t.A = d
        return (1) TRUE -> no duplicates, (2) FALSE -> has duplicates, invalid X
     */
    private boolean checkIfDupConstantPredicates(ArrayList<Predicate> currentX) {
        HashMap<String, Integer> stat = new HashMap<>();
        for (Predicate p : currentX) {
            if (!p.isConstant()) {
                continue;
            }
            String k = p.getOperand1().toString();
            if (stat.containsKey(k)) {
                return false;
            } else {
                stat.put(k, 1);
            }
        }
        return true;
    }

    public List<Message> validation_new(ArrayList<Predicate> currentSet, PredicateSet rhss, int[] pids) {

//        // only for test
//        if (! currentSet.get(0).toString().trim().equals("airports.t0.keywords == airports.t1.keywords")) {
//            return new ArrayList<>();
//        }

        List<Message> messages = new ArrayList<>();
        log.info("Validation CurrentX : {}, RHSs : {}", currentSet, rhss);

        // first check whether currentSet -> rhss is valid or not
        if (!this.checkIfDupConstantPredicates(currentSet)) {
            Message message_ = new Message(currentSet, 0, 0, 0);
            for (Predicate rhs : rhss) {
                message_.addInValidRHS(rhs);
            }
            messages.add(message_);
            return messages;
        }

        // select constant predicates
        ArrayList<Predicate> constantPredicates = new ArrayList<>();
        HashMap<Integer, ArrayList<Predicate>> predicateGroups = new HashMap<>();
        ArrayList<ImmutablePair<Integer, Integer>> keys = new ArrayList<>();
        ArrayList<Long> keysFreqs_ = new ArrayList<>();

        String relationT0 = null;
        int maxtuple = 0;
        for (Predicate p : currentSet) {
            // check the relation of t_0
            if (p.getIndex1() == 0) {
                relationT0 = p.getOperand1().getColumn().getTableName();
            }
            maxtuple = Math.max(maxtuple, Math.max(p.getIndex1(), p.getIndex2()));
            if (p.isConstant()) {
                constantPredicates.add(p);
            } else {
                Integer key = MultiTuplesOrder.encode(new ImmutablePair<>(p.getIndex1(), p.getIndex2()));
                if (!predicateGroups.containsKey(key)) {
                    ArrayList<Predicate> group = new ArrayList<>();
                    group.add(p);
                    predicateGroups.put(key, group);
                } else {
                    predicateGroups.get(key).add(p);
                }
            }
        }
//        for (Predicate p : constantPredicates) {
//            int index = p.getIndex1();
//            for (int t = 0; t <= maxtuple; t++) {
//                if (index == t) {
//                    continue;
//                }
//                int min_t = Math.min(index, t);
//                int max_t = Math.max(index, t);
//                int key = MultiTuplesOrder.encode(new ImmutablePair<>(min_t, max_t));
//                if (predicateGroups.containsKey(key)) {
//                    predicateGroups.get(key).add(p);
//                }
//            }
//        }

        // retrieve valid TIDs of predicatesPrefix
        HashMap<Integer, HashSet<Integer>> validConstantTIDs = this.retrieveSatisfiedConstantTIDs(pids, constantPredicates);

        // record the initial keys and keysFreq
        for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
            keys.add(MultiTuplesOrder.decode(entry.getKey()));
            keysFreqs_.add(0L);
        }
        // rearrange keys
        MultiTuplesOrder multiTuplesOrder_new = new MultiTuplesOrder(keys, keysFreqs_);
        ArrayList<ImmutablePair<Integer, Integer>> keys_new = multiTuplesOrder_new.rearrangeKeys();

        // get the tuple number of relationT0
        long countT0 = this.tupleNumberRelations.get(relationT0);
        boolean ifTermin = true;
        for (String r : this.tupleNumberRelations.keySet()) {
            if (r.equals(relationT0)) {
                continue;
            }
            if (countT0 < this.tupleNumberRelations.get(r)) {
                ifTermin = false;
            }
        }
        if (this.tupleNumberRelations.size() > 1 && ifTermin) {
            Message message_ = new Message(currentSet, 0, 0, 0);
            for (Predicate rhs : rhss) {
                message_.addInValidRHS(rhs);
            }
            messages.add(message_);
            return messages;
        }

        // only consider t_0
        int dataNumT0 = 0;
        int pidt0 = pids[0];
        for (Predicate p : currentSet) {
            if (p.getIndex1() == 0) {
                int b = p.getOperand1().getColumnLight().getTidsInterval(pidt0).left;
                int e = p.getOperand1().getColumnLight().getTidsInterval(pidt0).right;
                countT0 = e - b;
                break;
            }
        }

        // confirm and split RHSs
        // extract constant predicates
        ArrayList<Predicate> constantPs0 = new ArrayList<>();
        ArrayList<Predicate> constantPs1 = new ArrayList<>();
        ArrayList<Predicate> biPredicates = new ArrayList<>();
        for (Predicate p : rhss) {
            if (p.isConstant()) {
                if (p.getIndex1() == 0) {
                    constantPs0.add(p);
                } else if (p.getIndex1() == 1) {
                    constantPs1.add(p);
                }
            } else {
                biPredicates.add(p);
            }
        }

        HashSet<Predicate> invalidRHSs = new HashSet<>();

        for (int i = 0; i < biPredicates.size(); i++) {
            Predicate p = biPredicates.get(i);
            if (p.getSupport() < this.support) {
                // invalid RHS
                invalidRHSs.add(p);
                biPredicates.remove(i);
            }
        }

        for (int i = 0; i < constantPs0.size(); i++) {
            Predicate p = constantPs0.get(i);
            if (p.getSupport() * this.maxOneRelationNum < this.support) {
                invalidRHSs.add(p);
                constantPs0.remove(i);
            }
        }

        for (int i = 0; i < constantPs1.size(); i++) {
            Predicate p = constantPs1.get(i);
            if (p.getSupport() * this.maxOneRelationNum < this.support) {
                invalidRHSs.add(p);
                constantPs1.remove(i);
            }
        }

        int batch_num = (int) (countT0 / BATCH_TUPLE_NUM) + 1;

        long supportXAll = 0L;
        long supportCP0 = 0L;
        long supportCP1 = 0L;
        Long[] countsBiRHSs = new Long[biPredicates.size()];
        for (int i = 0; i < countsBiRHSs.length; i++) countsBiRHSs[i] = 0L;
        Long[] countsUniRHSs0 = new Long[constantPs0.size()];
        for (int i = 0; i < countsUniRHSs0.length; i++) countsUniRHSs0[i] = 0L;
        Long[] countsUniRHSs1 = new Long[constantPs1.size()];
        for (int i = 0; i < countsUniRHSs1.length; i++) countsUniRHSs1[i] = 0L;


        for (int batch_id = 0; batch_id < batch_num; batch_id++) {

            boolean flag = true;
            long tupleIDs_start_temp = batch_id * BATCH_TUPLE_NUM;
            long tupleIDs_end_temp = Math.min((batch_id + 1) * BATCH_TUPLE_NUM, countT0);
            long tupleIDs_start = tupleIDs_start_temp + this.inputLight.getTupleIDStart(relationT0);
            long tupleIDs_end = tupleIDs_end_temp + this.inputLight.getTupleIDStart(relationT0);

            // process data in a batch mode
            ArrayList<Boolean> usedTids = new ArrayList<>();
            usedTids.add(Boolean.TRUE);
            for (int t = 1; t <= maxtuple; t++) {
                usedTids.add(Boolean.FALSE);
            }
            ArrayList<HashSet<Integer>> usedData = new ArrayList<>();
            for (int t = 0; t <= maxtuple; t++) {
                usedData.add(new HashSet<>());
            }
            for (int tid = (int) tupleIDs_start; tid < tupleIDs_end; tid++) {
                usedData.get(0).add(tid);
            }


            // calculate the intersection and result for each group

            ArrayList<ImmutablePair<Integer, Integer>> keysTid = new ArrayList<>();
            ArrayList<Long> keysFreq = new ArrayList<>();
            HashMap<Integer, ArrayList<Integer>> beginTuplePairs = null;
            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict = new HashMap<>();
//        log.info("Predicate group : {}", predicateGroups);
            for (ImmutablePair<Integer, Integer> tid_pair : keys_new) {
                // for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
                int kk = MultiTuplesOrder.encode(tid_pair);
                ArrayList<Predicate> group = predicateGroups.get(kk);
                // ImmutablePair<Integer, Integer> tid_pair = MultiTuplesOrder.decode(entry.getKey());

                HashSet<Integer> validConstantTIDs_left = validConstantTIDs.get(tid_pair.left);
                HashSet<Integer> validConstantTIDs_right = validConstantTIDs.get(tid_pair.right);

                ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long> dataOneGroup =
                        this.findResultsOneGroup_(group, tid_pair.left, tid_pair.right, pids, usedData, usedTids,
                                validConstantTIDs_left, validConstantTIDs_right);
                // update usedTids
                usedTids.set(tid_pair.left, Boolean.TRUE);
                usedTids.set(tid_pair.right, Boolean.TRUE);

                if (dataOneGroup == null || dataOneGroup.left.size() <= 0) {
                    flag = false;
                    break;
                }
                // the first case
//            log.info("Predicate tuple ID pair : {}", entry.getKey());
                if (kk == MultiTuplesOrder.encode(new ImmutablePair<>(0, 1))) {
                    beginTuplePairs = dataOneGroup.left;
                } else {
                    datadict.put(kk, dataOneGroup.left);
                }

                keysFreq.add(dataOneGroup.right);
                keysTid.add(tid_pair);
            }
            if (flag == false) {
                // heuristic filter

//                /*****************************************
//                 * ONLY for heuristic
//                 */
//                // for very very large predicate group of LARGE support
//                supportXAll += support * support;
//                supportCP0 += 0.1 * BATCH_TUPLE_NUM * maxOneRelationNum;
//                supportCP1 += 0.1 * BATCH_TUPLE_NUM * maxOneRelationNum;
//                for (int i = 0; i < biPredicates.size(); i++) {
//                    countsBiRHSs[i] = biPredicates.get(i).getSupport();
//                }
//                for (int i = 0; i < constantPs0.size(); i++) {
//                    countsUniRHSs0[i] = constantPs0.get(i).getSupport();
//                }
//                for (int i = 0; i < constantPs1.size(); i++) {
//                    countsUniRHSs1[i] = constantPs1.get(i).getSupport();
//                }
//                /**********************
//
//                 ****************************************************/

                continue;
            }
            MultiTuplesOrder multiTuplesOrder = new MultiTuplesOrder(keysTid, keysFreq);
            ArrayList<ImmutablePair<Integer, Integer>> keys_new_new = multiTuplesOrder.rearrangeKeys();
            // retrieve tuple pairs of X
            ArrayList<ImmutablePair<Integer, Integer>> resultsX = multiTuplesOrder.joinAll(beginTuplePairs, keys_new_new, datadict);

//        log.info("First List : {}", beginTuplePairs);
//        log.info("Final List : {}", resultsX);

            supportXAll += resultsX.size();

            // extract lists for constant predicates
            HashSet<Integer> resultsXCP0 = new HashSet<>();
            HashSet<Integer> resultsXCP1 = new HashSet<>();
            for (ImmutablePair<Integer, Integer> pair : resultsX) {
                resultsXCP0.add(pair.left);
                resultsXCP1.add(pair.right);
            }

            supportCP0 += resultsXCP0.size();
            supportCP1 += resultsXCP1.size();

            biVerify(pids, countsBiRHSs, resultsX, biPredicates);
//        log.info("Mining after biVerify : {}, biPredicates : {}", message, biPredicates);
            uniVerify(pids, countsUniRHSs0, resultsXCP0, constantPs0);
            uniVerify(pids, countsUniRHSs1, resultsXCP1, constantPs1);
        }

        // put together
//        if (supportXAll < this.support) {
//            // all RHSs with currentList are invalid, add all
//            Message message_ = new Message(currentSet, supportXAll, supportCP0, supportCP1);
//            for (Predicate rhs : rhss) {
//                message_.addInValidRHS(rhs);
//            }
//            messages.add(message_);
//            return messages;
//        }

        Message message = new Message(currentSet, supportXAll, supportCP0, supportCP1);
        // add invalid RHSs
        for (Predicate p : invalidRHSs) {
            message.addInValidRHS(p);
        }

        message.updateAllCurrentRHSsSupport(countsBiRHSs, biPredicates);
        message.updateAllCurrentRHSsSupport(countsUniRHSs0, constantPs0);
        message.updateAllCurrentRHSsSupport(countsUniRHSs1, constantPs1);
        message.updateAllCurrentRHSsSupport(invalidRHSs);

        // uniStatistic(message, countsUniRHSs0, constantPs0, supportXAll);
        // uniStatistic(message, countsUniRHSs1, constantPs1, supportXAll);
        // biStatistic(message, countsBiRHSs, biPredicates, supportXAll);

        // add into a list of messages
        messages.add(message);
        return messages;
    }


    /***************************************************************************
     *
     * @param currentSet
     * @param rhss: all predicates in rhss have the same tuple id, e.g., <t_0, t_1> or <t_0>, or <t_0, t_2>
     *            Constant predicate ONLY contains <t_0>
     * @param pids
     * @return
     */
    public List<Message> validation_complex(ArrayList<Predicate> currentSet, PredicateSet rhss, int[] pids) {

        // first confirm the targetTID along with t_0 (0)
        int targetTID = 1;
        for (Predicate rhs : rhss) {
            if (!rhs.isConstant()) {
                targetTID = rhs.getIndex2();
                break;
            }
        }

        List<Message> messages = new ArrayList<>();
        log.info("Validation CurrentX : {}, RHSs : {}", currentSet, rhss);
        // select constant predicates
        ArrayList<Predicate> constantPredicates = new ArrayList<>();
        HashMap<Integer, ArrayList<Predicate>> predicateGroups = new HashMap<>();
        ArrayList<ImmutablePair<Integer, Integer>> keys = new ArrayList<>();
        ArrayList<Long> keysFreqs_ = new ArrayList<>();

        String relationT0 = null;
        int maxtuple = 0;
        for (Predicate p : currentSet) {
            // check the relation of t_0
            if (p.getIndex1() == 0) {
                relationT0 = p.getOperand1().getColumn().getTableName();
            }
            maxtuple = Math.max(maxtuple, Math.max(p.getIndex1(), p.getIndex2()));
            if (p.isConstant()) {
                constantPredicates.add(p);
            } else {
                Integer key = MultiTuplesOrder.encode(new ImmutablePair<>(p.getIndex1(), p.getIndex2()));
                if (!predicateGroups.containsKey(key)) {
                    ArrayList<Predicate> group = new ArrayList<>();
                    group.add(p);
                    predicateGroups.put(key, group);
                } else {
                    predicateGroups.get(key).add(p);
                }
            }
        }
        for (Predicate p : constantPredicates) {
            int index = p.getIndex1();
            for (int t = 0; t <= maxtuple; t++) {
                if (index == t) {
                    continue;
                }
                int min_t = Math.min(index, t);
                int max_t = Math.max(index, t);
                int key = MultiTuplesOrder.encode(new ImmutablePair<>(min_t, max_t));
                if (predicateGroups.containsKey(key)) {
                    predicateGroups.get(key).add(p);
                }
            }
        }

        // record the initial keys and keysFreq
        for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
            keys.add(MultiTuplesOrder.decode(entry.getKey()));
            keysFreqs_.add(0L);
        }
        // rearrange keys
        MultiTuplesOrder multiTuplesOrder_new = new MultiTuplesOrder(keys, keysFreqs_);
        ArrayList<ImmutablePair<Integer, Integer>> keys_new = multiTuplesOrder_new.rearrangeKeys();

        // get the tuple number of relationT0
        long countT0 = this.tupleNumberRelations.get(relationT0);
        boolean ifTermin = true;
        for (String r : this.tupleNumberRelations.keySet()) {
            if (r.equals(relationT0)) {
                continue;
            }
            if (countT0 < this.tupleNumberRelations.get(r)) {
                ifTermin = false;
            }
        }
        if (this.tupleNumberRelations.size() > 1 && ifTermin) {
            Message message_ = new Message(currentSet, 0, 0, 0);
            for (Predicate rhs : rhss) {
                message_.addInValidRHS(rhs);
            }
            messages.add(message_);
            return messages;
        }

        // only consider t_0
        int dataNumT0 = 0;
        int pidt0 = pids[0];
        for (Predicate p : currentSet) {
            if (p.getIndex1() == 0) {
                int b = p.getOperand1().getColumnLight().getTidsInterval(pidt0).left;
                int e = p.getOperand1().getColumnLight().getTidsInterval(pidt0).right;
                countT0 = e - b;
                break;
            }
        }

        // confirm and split RHSs
        // extract constant predicates
        ArrayList<Predicate> constantPs0 = new ArrayList<>();
        ArrayList<Predicate> constantPs1 = new ArrayList<>();
        ArrayList<Predicate> biPredicates = new ArrayList<>();
        for (Predicate p : rhss) {
            if (p.isConstant()) {
                if (p.getIndex1() == 0) {
                    constantPs0.add(p);
                } else if (p.getIndex1() == 1) {
                    constantPs1.add(p);
                }
            } else {
                biPredicates.add(p);
            }
        }

        HashSet<Predicate> invalidRHSs = new HashSet<>();

        for (int i = 0; i < biPredicates.size(); i++) {
            Predicate p = biPredicates.get(i);
            if (p.getSupport() < this.support) {
                // invalid RHS
                invalidRHSs.add(p);
                biPredicates.remove(i);
            }
        }

        for (int i = 0; i < constantPs0.size(); i++) {
            Predicate p = constantPs0.get(i);
            if (p.getSupport() * this.maxOneRelationNum < this.support) {
                invalidRHSs.add(p);
                constantPs0.remove(i);
            }
        }

        for (int i = 0; i < constantPs1.size(); i++) {
            Predicate p = constantPs1.get(i);
            if (p.getSupport() * this.maxOneRelationNum < this.support) {
                invalidRHSs.add(p);
                constantPs1.remove(i);
            }
        }

        int batch_num = (int) (countT0 / BATCH_TUPLE_NUM) + 1;

        long supportXAll = 0L;
        long supportCP0 = 0L;
        long supportCP1 = 0L;
        Long[] countsBiRHSs = new Long[biPredicates.size()];
        for (int i = 0; i < countsBiRHSs.length; i++) countsBiRHSs[i] = 0L;
        Long[] countsUniRHSs0 = new Long[constantPs0.size()];
        for (int i = 0; i < countsUniRHSs0.length; i++) countsUniRHSs0[i] = 0L;
        Long[] countsUniRHSs1 = new Long[constantPs1.size()];
        for (int i = 0; i < countsUniRHSs1.length; i++) countsUniRHSs1[i] = 0L;


        for (int batch_id = 0; batch_id < batch_num; batch_id++) {

            boolean flag = true;
            long tupleIDs_start_temp = batch_id * BATCH_TUPLE_NUM;
            long tupleIDs_end_temp = Math.min((batch_id + 1) * BATCH_TUPLE_NUM, countT0);
            long tupleIDs_start = tupleIDs_start_temp + this.inputLight.getTupleIDStart(relationT0);
            long tupleIDs_end = tupleIDs_end_temp + this.inputLight.getTupleIDStart(relationT0);

            // process data in a batch mode
            ArrayList<Boolean> usedTids = new ArrayList<>();
            usedTids.add(Boolean.TRUE);
            for (int t = 1; t <= maxtuple; t++) {
                usedTids.add(Boolean.FALSE);
            }
            ArrayList<HashSet<Integer>> usedData = new ArrayList<>();
            for (int t = 0; t <= maxtuple; t++) {
                usedData.add(new HashSet<>());
            }
            for (int tid = (int) tupleIDs_start; tid < tupleIDs_end; tid++) {
                usedData.get(0).add(tid);
            }


            // calculate the intersection and result for each group

            ArrayList<ImmutablePair<Integer, Integer>> keysTid = new ArrayList<>();
            ArrayList<Long> keysFreq = new ArrayList<>();
            HashMap<Integer, ArrayList<Integer>> beginTuplePairs = null;
            HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> datadict = new HashMap<>();
//        log.info("Predicate group : {}", predicateGroups);
            for (ImmutablePair<Integer, Integer> tid_pair : keys_new) {
                // for (Map.Entry<Integer, ArrayList<Predicate>> entry : predicateGroups.entrySet()) {
                int kk = MultiTuplesOrder.encode(tid_pair);
                ArrayList<Predicate> group = predicateGroups.get(kk);
                // ImmutablePair<Integer, Integer> tid_pair = MultiTuplesOrder.decode(entry.getKey());
                ImmutablePair<HashMap<Integer, ArrayList<Integer>>, Long> dataOneGroup =
                        this.findResultsOneGroup_(group, tid_pair.left, tid_pair.right, pids, usedData, usedTids);
                // update usedTids
                usedTids.set(tid_pair.left, Boolean.TRUE);
                usedTids.set(tid_pair.right, Boolean.TRUE);

                if (dataOneGroup == null || dataOneGroup.left.size() <= 0) {
                    flag = false;
                    break;
                }
                // the first case
//            log.info("Predicate tuple ID pair : {}", entry.getKey());
                if (kk == MultiTuplesOrder.encode(new ImmutablePair<>(0, 1))) {
                    beginTuplePairs = dataOneGroup.left;
                } else {
                    datadict.put(kk, dataOneGroup.left);
                }

                keysFreq.add(dataOneGroup.right);
                keysTid.add(tid_pair);
            }
            if (flag == false) {
                continue;
            }
            MultiTuplesOrder multiTuplesOrder = new MultiTuplesOrder(keysTid, keysFreq);
            ArrayList<ImmutablePair<Integer, Integer>> keys_new_new = multiTuplesOrder.rearrangeKeys();
            // retrieve tuple pairs of X
            // ArrayList<ImmutablePair<Integer, Integer>> resultsX = multiTuplesOrder.joinAll(beginTuplePairs, keys_new_new, datadict);

            HashSet<ImmutablePair<Integer, Integer>> resultsX = multiTuplesOrder.joinAll(beginTuplePairs, keys_new_new, datadict, targetTID);

//        log.info("First List : {}", beginTuplePairs);
//        log.info("Final List : {}", resultsX);

            supportXAll += resultsX.size();

            // extract lists for constant predicates
            HashSet<Integer> resultsXCP0 = new HashSet<>();
            HashSet<Integer> resultsXCP1 = new HashSet<>();
            for (ImmutablePair<Integer, Integer> pair : resultsX) {
                resultsXCP0.add(pair.left);
                resultsXCP1.add(pair.right);
            }

            supportCP0 += resultsXCP0.size();
            supportCP1 += resultsXCP1.size();

            biVerify(pids, countsBiRHSs, resultsX, biPredicates);
//        log.info("Mining after biVerify : {}, biPredicates : {}", message, biPredicates);
            uniVerify(pids, countsUniRHSs0, resultsXCP0, constantPs0);
            // do not need t_1 constant predicates
            //uniVerify(pids, countsUniRHSs1, resultsXCP1, constantPs1);
        }

        // put together
//        if (supportXAll < this.support) {
//            // all RHSs with currentList are invalid, add all
//            Message message_ = new Message(currentSet, supportXAll, supportCP0, supportCP1);
//            for (Predicate rhs : rhss) {
//                message_.addInValidRHS(rhs);
//            }
//            messages.add(message_);
//            return messages;
//        }

        Message message = new Message(currentSet, supportXAll, supportCP0, supportCP1);
        // add invalid RHSs
        for (Predicate p : invalidRHSs) {
            message.addInValidRHS(p);
        }

        message.updateAllCurrentRHSsSupport(countsBiRHSs, biPredicates);
        message.updateAllCurrentRHSsSupport(countsUniRHSs0, constantPs0);
        message.updateAllCurrentRHSsSupport(countsUniRHSs1, constantPs1);
        message.updateAllCurrentRHSsSupport(invalidRHSs);

        // uniStatistic(message, countsUniRHSs0, constantPs0, supportXAll);
        // uniStatistic(message, countsUniRHSs1, constantPs1, supportXAll);
        // biStatistic(message, countsBiRHSs, biPredicates, supportXAll);

        // add into a list of messages
        messages.add(message);
        return messages;
    }


    private void biVerify(int[] PIDs, Long[] counts, HashSet<ImmutablePair<Integer, Integer>> resultsX, ArrayList<Predicate> biPredicateRHSs) {

        int t_left = 0, t_right = 1;
        ArrayList<ImmutablePair<Integer, Integer>> tuple_IDs_start_rhs = this.computeTupleIDsStart(biPredicateRHSs);
        for (ImmutablePair<Integer, Integer> pair : resultsX) {
            for (int i = 0; i < biPredicateRHSs.size(); i++) {
                if (biPredicateRHSs.get(i).calculateTp(PIDs[t_left], PIDs[t_right], pair.left, pair.right, tuple_IDs_start_rhs.get(i).left, tuple_IDs_start_rhs.get(i).right)) {
                    counts[i]++;
                }
            }
        }
    }

    private static Logger log = LoggerFactory.getLogger(MultiTuplesRuleMiningOpt.class);
}
