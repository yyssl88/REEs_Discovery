package sics.seiois.mlsserver.biz.der.metanome.predicates;

import de.metanome.algorithm_integration.Operator;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.predicates.operands.ColumnOperand;
//import sun.swing.SwingUtilities2;

public class ConstantPredicateBuilder implements Serializable {

    private static final long serialVersionUID = 3961087336068687061L;
    private static final Logger logger = LoggerFactory.getLogger(ConstantPredicateBuilder.class);

    public static HashMap<Integer, Long> predicates_card = new HashMap<>();
    private double COMPARE_AVG_RATIO = 0.3d;

    private double minimumSharedValue = 0.30d;
    private double maximumSharedValue = 0.6d;

    private boolean noCrossColumn = true;

    private Set<Predicate> predicates;
    private Set<Predicate> predicatesHighSelectivity;
    private Set<Predicate> predicatesLowSelectivity;

    private Collection<Collection<Predicate>> predicateGroups;

    private Collection<Collection<Predicate>> predicateGroupsNumericalSingleColumn;
    private Collection<Collection<Predicate>> predicateGroupsCategoricalSingleColumn;

    private Collection<Predicate> predicatesNumericalSingleColumn;
    private Collection<Predicate> predicatesCategoricalSingleColumn;

    public static String relationAttrDelimiter = "->";

    public static double highRatio = 0.2;

    /*
     * select numerical constant predicates
     */
    private void divideConstantPredicatesTypes() {
        predicatesNumericalSingleColumn = new ArrayList<>();
        predicatesCategoricalSingleColumn = new ArrayList<>();
        for (Predicate p : getPredicates()) {
            if (p.getOperand1().getColumn().getType().equals(String.class)) {
                predicatesCategoricalSingleColumn.add(p);
            } else {
                predicatesNumericalSingleColumn.add(p);
            }
        }
    }

    public Collection<Predicate> getPredicatesNumericalSingleColumn() {
        return this.predicatesNumericalSingleColumn;
    }

    public Collection<Predicate> getPredicatesCategoricalSingleColumn() {
        return this.predicatesCategoricalSingleColumn;
    }

    public ConstantPredicateBuilder(Input input, String constantPredicates_file) {
        predicates = new HashSet<>();
        predicateGroups = new ArrayList<>();

        // add all constant predicates
        addConstantPredicates(constantPredicates_file, input);

        dividePredicateGroupsByType();

        divideConstantPredicatesTypes();

        addPredicates();
//        sortPredicates(input);
    }


    public ConstantPredicateBuilder(Input input, List<String> constantPredicateList) {
        predicates = new HashSet<>();
        predicateGroups = new ArrayList<>();

        // add all constant predicates
        addConstantPredicates(constantPredicateList, input);

        dividePredicateGroupsByType();

        divideConstantPredicatesTypes();

        addPredicates();
//        sortPredicates(input);
    }


//    private ArrayList<ColumnPair> constructColumnPairs(Input input) {
//        ArrayList<ColumnPair> pairs = new ArrayList<ColumnPair>();
//        for (int i = 0; i < input.getColumns().length; ++i) {
//            ParsedColumn<?> c1 = input.getColumns()[i];
//            for (int j = i; j < input.getColumns().length; ++j) {
//                ParsedColumn<?> c2 = input.getColumns()[j];
//                boolean joinable = isJoinable(c1, c2);
//                boolean comparable = isComparable(c1, c2);
//                if (joinable || comparable)
//                    // pairs.add(new ColumnPair(c1, c2, joinable, comparable));
//                    pairs.add(new ColumnPair(c1, c2, true, comparable));
//            }
//        }
//        return pairs;
//    }

//    private boolean isJoinable(ParsedColumn<?> c1, ParsedColumn<?> c2) {
//        if (noCrossColumn)
//            return c1.equals(c2);
//
//        // if two columns are from the same table but not equal, return false
//        if ( (!c1.equals(c2)) && c1.getTableName().equals(c2.getTableName()))
//            return false;
//
//        if (!c1.getType().equals(c2.getType()))
//            return false;
//
//        if (c1.equals(c2)) return true;
//
//        // check cardinality
//        //if (c1.checkHighCard() || c2.checkHighCard())
//            //return false;
//
//        double ratio = c1.getSharedPercentage(c2);
//        return ratio > minimumSharedValue; //&& ratio < maximumSharedValue;
//    }

//    private boolean isComparable(ParsedColumn<?> c1, ParsedColumn<?> c2) {
//        if (noCrossColumn) {
//            return c1.equals(c2) && (c1.getType().equals(Double.class) || c1.getType().equals(Long.class));
//        }
//
//        if (c1.equals(c2)) {
//            return true;
//        }
//
//        //if (c1.checkHighCard() || c2.checkHighCard())
//            //return false;
//
//        if (!c1.getType().equals(c2.getType())) {
//            return false;
//        }
//
//        // if two columns are from the same table but not equal, return false
//        if ( (!c1.equals(c2)) && c1.getTableName().equals(c2.getTableName())) {
//            return false;
//        }
//
//        if (c1.getType().equals(Double.class) || c1.getType().equals(Long.class)) {
////            if (c1.equals(c2))
////                return true;
//
//            double avg1 = c1.getAverage();
//            double avg2 = c2.getAverage();
//            return Math.min(avg1, avg2) / Math.max(avg1, avg2) > COMPARE_AVG_RATIO;
//        }
//        return false;
//    }

    public Set<Predicate> getPredicates() {
        return predicates;
    }

    // split constant predicates into low and high
    public Set<Predicate> getHighSelectivityPredicates() {
        return predicatesHighSelectivity;
    }

    public Set<Predicate> getLowSelectivityPredicates() {
        return predicatesLowSelectivity;
    }

//    private Integer getSupport(Predicate p, Input input) {
//        List<PLISection> sections = input.getSectionPli();
//        int support = 0;
//        for(PLISection section : sections) {
//            String key = p.getOperand1().getColumn().getTableName() + PredicateConfig.TBL_COL_DELIMITER +
//                    p.getOperand1().getColumn().getName();
//            PLI pli = section.getPLI(key);
//            if (pli != null) {
//                List<Integer> list = section.getPLI(key).getTpIDsForValue(p.getConstantInt());
//                support += list == null ? 0 : list.size();
//            }
//        }
//        return support;
//    }

    private void addPredicates() {
        predicatesLowSelectivity = new HashSet<>();
        predicatesHighSelectivity = new HashSet<>();
        for (Predicate p : getPredicates()) {
            predicatesLowSelectivity.add(p);
        }
    }

//    private void sortPredicates(Input input) {
//        ArrayList<ImmutablePair<Predicate, Integer>> res = new ArrayList<>();
//        for (Predicate p : getPredicates()) {
//            res.add(new ImmutablePair<>(p, getSupport(p, input)));
//        }
//
//        // sort
//        res.sort(new Comparator<ImmutablePair<Predicate, Integer>>() {
//            @Override
//            public int compare(ImmutablePair<Predicate, Integer> o1, ImmutablePair<Predicate, Integer> o2) {
//                return Double.compare((double)o1.getValue(), (double)o2.getValue());
//            }
//        });
//
//        logger.info("sorted predicates supports : ");
////        for (ImmutablePair<Predicate, Integer> e : res) {
////            logger.info("element {}, {}", e.getLeft(), e.getRight());
////        }
//        predicatesHighSelectivity = new HashSet<>();
//        predicatesLowSelectivity = new HashSet<>();
//        int highsel_num = (int)(res.size() * highRatio);
//        int lowsel_num = res.size() - highsel_num;
//        for (int i = 0; i < res.size(); i++) {
//            if (i < highsel_num) {
//                predicatesHighSelectivity.add(res.get(i).getLeft());
//            } else {
//                predicatesLowSelectivity.add(res.get(i).getLeft());
//            }
//        }
//        logger.info("###high predicates:{}, low predicates:{}", predicatesHighSelectivity, predicatesLowSelectivity);
//    }

    public Collection<Collection<Predicate>> getPredicateGroups() {
        return predicateGroups;
    }

//    public Collection<Collection<Predicate>> getNumericalSingleColPredicates() {
//
//        Collection<Collection<Predicate>> numericalPredicates = new ArrayList<>();
//
//        for (Collection<Predicate> predicateGroup : getPredicateGroups()) {
//
//            if (predicateGroup.size() == 6) {
//                numericalPredicates.add(predicateGroup);
//            }
//        }
//
//        return numericalPredicates;
//    }
//
//    public Collection<Collection<Predicate>> getCategoricalSingleColPredicates() {
//
//        Collection<Collection<Predicate>> categoricalPredicates = new ArrayList<>();
//
//        for (Collection<Predicate> predicateGroup : getPredicateGroups()) {
//
//            if (predicateGroup.size() == 2) {
//                categoricalPredicates.add(predicateGroup);
//            }
//        }
//
//        return categoricalPredicates;
//    }

    private void dividePredicateGroupsByType() {

        predicateGroupsNumericalSingleColumn = new ArrayList<>();
        predicateGroupsCategoricalSingleColumn = new ArrayList<>();

        for (Collection<Predicate> predicateGroup : getPredicateGroups()) {

            // handle non-constant predicates
            if (checkNumerical(predicateGroup)) {// numeric
                    predicateGroupsNumericalSingleColumn.add(predicateGroup);
            } else {
                    predicateGroupsCategoricalSingleColumn.add(predicateGroup);
            }

        }

    }

    private boolean checkNumerical(Collection<Predicate> ps) {
        boolean T = false;
        for (Predicate p : ps) {
           if (p.getOperator() != Operator.EQUAL && p.getOperator() != Operator.UNEQUAL) {
              return true;
           }
        }
        return T;
    }


    public Collection<Collection<Predicate>> getPredicateGroupsNumericalSingleColumn() {
        return predicateGroupsNumericalSingleColumn;
    }


    public Collection<Collection<Predicate>> getPredicateGroupsCategoricalSingleColumn() {
        return predicateGroupsCategoricalSingleColumn;
    }

    public Predicate getPredicateByType(Collection<Predicate> predicateGroup, Operator type) {
        Predicate pwithtype = null;
        for (Predicate p : predicateGroup) {
            if (p.getOperator().equals(type)) {
                pwithtype = p;
                break;
            }
        }
        return pwithtype;
    }

    public ArrayList<Predicate> getPredicateByTypeConstants(Collection<Predicate> predicateGroup, Operator type) {
        ArrayList<Predicate> pweighttypes = new ArrayList<>();
        for (Predicate p: predicateGroup) {
            if (p.getOperator().equals(type) && p.isConstant()) {
                pweighttypes.add(p);
            }
        }
        return pweighttypes;
    }

//    public Collection<ColumnPair> getColumnPairs() {
//        Set<List<ParsedColumn<?>>> joinable = new HashSet<>();
//        Set<List<ParsedColumn<?>>> comparable = new HashSet<>();
//        Set<List<ParsedColumn<?>>> all = new HashSet<>();
//        for (Predicate p : predicates) {
//            List<ParsedColumn<?>> pair = new ArrayList<>();
//            pair.add(p.getOperand1().getColumn());
//            pair.add(p.getOperand2().getColumn());
//
//            if (p.getOperator() == Operator.EQUAL)
//                joinable.add(pair);
//
//            if (p.getOperator() == Operator.LESS)
//                comparable.add(pair);
//
//            all.add(pair);
//        }
//
//        Set<ColumnPair> pairs = new HashSet<>();
//        for (List<ParsedColumn<?>> pair : all) {
//            pairs.add(new ColumnPair(pair.get(0), pair.get(1), joinable.contains(pair), comparable.contains(pair)));
//        }
//        return pairs;
//    }
//
//
//    private void addConstrantPredicates(ColumnOperand<?> o, int countThreshold) {
//        Set<Predicate> predicates = new HashSet<>();
//        Collection<String> constants = o.getColumn().getConstantValues(countThreshold);
//        // loop
//        for (String c : constants) {
//            Predicate constant_predicate = predicateProvider.getPredicate(Operator.EQUAL, o, o);
//            constant_predicate.setConstant(c);
//            predicates.add(constant_predicate);
//        }
//        this.predicates.addAll(predicates);
//    }

    private void addConstantPredicates(String pred_file, Input input) {
        if(StringUtils.isEmpty(pred_file)) {
            return;
        }
        Set<Predicate> predicates = new HashSet<>();

        FileReader fr = null;
        BufferedReader br = null;
        try{
            File file = new File(pred_file);
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            StringBuffer sb = new StringBuffer();
            String line;

            while ( (line = br.readLine()) != null) {
                String names = line.split(";")[0];
                String relation_name = names.split(relationAttrDelimiter)[0];
                String attr_name = names.split(relationAttrDelimiter)[1];
                String constant = line.split(";")[1];
                String opc = line.split(";")[2];
                ColumnOperand<?> o = new ColumnOperand<>(input.getParsedColumn(relation_name, attr_name), 0);
                ColumnOperand<?> _o_ = new ColumnOperand<>(input.getParsedColumn(relation_name, attr_name), 1);
                Predicate constantp = null;
                Predicate constantp_ = null;
                if("==".equals(opc)) {
                    constantp = predicateProvider.getPredicate(Operator.EQUAL, o, o, constant);
                    constantp_ = predicateProvider.getPredicate(Operator.EQUAL, _o_, _o_, constant);
                } else if("<=".equals(opc)) {
                    if (! o.getColumn().checkHighCard()) {
                        constantp = predicateProvider.getPredicate(Operator.LESS_EQUAL, o, o, constant);
                    }
                } else if(">=".equals(opc)) {
                    if (! o.getColumn().checkHighCard()) {
                        constantp = predicateProvider.getPredicate(Operator.GREATER_EQUAL, o, o, constant);
                    }
                } else {
                    throw new Exception("wrong constant argument");
                }
                if (constantp != null) {
                    constantp.setConstant(constant);
                    predicates.add(constantp);
                }
                if (constantp_ != null) {
                    constantp_.setConstant(constant);
                    predicates.add(constantp_);
                }
            }

            // insert string Int
            input.transformConstantPredicates(predicates);

            // insert predicates into predicateGroups
            HashMap<String, List<Predicate>> agg = new HashMap<>();
            for(Predicate p : predicates) {
                String k = p.getOperand1().toString() + "." + p.getOperand2().toString();
                if (!agg.containsKey(k)) {
                    agg.put(k, new ArrayList<Predicate>());
                }
                agg.get(k).add(p);
            }

            this.predicates.addAll(predicates);
            for(List<Predicate> ll : agg.values()) {
                this.predicateGroups.add(ll);
            }

        } catch(IOException e){
            logger.error("IOException error", e);
        } catch (Exception e) {
            logger.error("addConstantPredicates error", e);
        } finally{
            try {
                if(br != null){
                    br.close();
                }
            } catch (Exception e) {
                logger.error("BufferedReader close error", e);
            }

            try {
                if(fr != null){
                    fr.close();
                }
            } catch (Exception e) {
                logger.error("FileReader close error", e);
            }
        }
    }

    private boolean checkInvalidConstantPredicates(String constant) {
        // check whether the constant predicate is meaningless
        String splChrs = "-./@#$%^&_+=()";
        if (constant.trim().equals("") || constant.trim().matches(splChrs)) {
            return true;
        }
        return false;
    }

    private void addConstantPredicates(List<String> constantPredicateList, Input input) {
        if(null == constantPredicateList || constantPredicateList.size() < 1) {
            return;
        }
        Set<Predicate> predicates = new HashSet<>();
        try{
            for(String predicateStr : constantPredicateList){
                logger.info(">>>>show all constant predicate:{}", predicateStr);
                String names = predicateStr.split(";")[0];
                String relation_name = names.split(relationAttrDelimiter)[0];
                String attr_name = names.split(relationAttrDelimiter)[1];

                String constant = predicateStr.split(";")[1];

                // check invalid constants
                if (this.checkInvalidConstantPredicates(constant)) {
                    continue;
                }

                String opc = predicateStr.split(";")[2];
                ColumnOperand<?> o = new ColumnOperand<>(input.getParsedColumn(relation_name, attr_name), 0);
                ColumnOperand<?> _o_ = new ColumnOperand<>(input.getParsedColumn(relation_name, attr_name), 1);
                Predicate constantp = null;
                Predicate constantp_ = null;
                if("==".equals(opc)) {
                    constantp = predicateProvider.getPredicate(Operator.EQUAL, o, o, constant);
                    constantp_ = predicateProvider.getPredicate(Operator.EQUAL, _o_, _o_, constant);
                } else if("<=".equals(opc)) {
                    if (! o.getColumn().checkHighCard()) {
                        constantp = predicateProvider.getPredicate(Operator.LESS_EQUAL, o, o, constant);
                    }
                } else if(">=".equals(opc)) {
                    if (! o.getColumn().checkHighCard()) {
                        constantp = predicateProvider.getPredicate(Operator.GREATER_EQUAL, o, o, constant);
                    }
                } else {
                    logger.warn("wrong constant argument,predicateStr={}", predicateStr);
//                    throw new Exception(predicateStr + ":wrong constant argument");
                }
                if (constantp != null) {
                    constantp.setConstant(constant);
                    predicates.add(constantp);
                }
                if (constantp_ != null) {
                    constantp_.setConstant(constant);
                    predicates.add(constantp_);
                }
//                constantp.setConstant(constant);
//                predicates.add(constantp);
            }

            // insert string Int
            input.transformConstantPredicates(predicates);

            // insert predicates into predicateGroups
            HashMap<String, List<Predicate>> agg = new HashMap<>();
            for(Predicate p : predicates) {
                String k = p.getOperand1().toString() + "." + p.getOperand2().toString();
                if (!agg.containsKey(k)) {
                    agg.put(k, new ArrayList<Predicate>());
                }
                agg.get(k).add(p);
            }

            this.predicates.addAll(predicates);
            for(List<Predicate> ll : agg.values()) {
                this.predicateGroups.add(ll);
            }

        } catch (Exception e) {
            logger.error("#### addConstantPredicates exception", e);
        }
    }


    public static void setHighRatio(double highRatio) {
        ConstantPredicateBuilder.highRatio = highRatio;
    }


    /*
    private void addPredicates(ColumnOperand<?> o1, ColumnOperand<?> o2, boolean joinable, boolean comparable) {
        Set<Predicate> predicates = new HashSet<Predicate>();
        for (Operator op : Operator.values()) {

            // do not consider predicates with STRING and <, <=, >, >=
            if (op != Operator.EQUAL && op != Operator.UNEQUAL) {
                if (o1.getColumn().getType() == String.class || o2.getColumn().getType() == String.class)
                    continue;
            }

            // EQUAL and UNEQUAL must be joinable, all other comparable
            // if (op == Operator.EQUAL || op == Operator.UNEQUAL) {
            if (op == Operator.EQUAL || op == Operator.UNEQUAL) {
                // if (joinable ) {
                if (joinable && (o1.getIndex() != o2.getIndex())) {
                    // predicates.add(predicateProvider.getPredicate(op, o1, o2));
                    Predicate padd = predicateProvider.getPredicate(op, o1, o2);
                    predicates.add(padd);

                    // store cardinality of EQUAL
                    if (op == Operator.EQUAL) {
                        Integer _hash_ = new Integer(padd.hashCode());
                        if (predicates_card.get(_hash_) == null) {
                            long card = o1.getColumn().getSharedCardinality(o2.getColumn());
                            predicates_card.put(_hash_, card);
                        }
                    }
                }
                if (joinable && op == Operator.EQUAL && o1.getColumn().getTableName() != o2.getColumn().getTableName()) {
                    predicates_fk.add(predicateProvider.getPredicate(op, o1, o2));
                    predicates_fk.add(predicateProvider.getPredicate(op, o2, o1));
                }

            } else if (comparable) {

                predicates.add(predicateProvider.getPredicate(op, o1, o2));

            }
        }
        this.predicates.addAll(predicates);
        this.predicateGroups.add(predicates);
    }
    */
    private static final PredicateProvider predicateProvider = PredicateProvider.getInstance();
}
