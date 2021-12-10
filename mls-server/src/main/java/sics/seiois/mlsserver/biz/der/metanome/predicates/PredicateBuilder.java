package sics.seiois.mlsserver.biz.der.metanome.predicates;

import com.google.common.util.concurrent.AtomicLongMap;
import de.metanome.algorithm_integration.Operator;

import de.metanome.algorithm_integration.input.RelationalInput;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import sics.seiois.mlsserver.biz.der.bitset.IBitSet;
import sics.seiois.mlsserver.biz.der.metanome.evidenceset.IEvidenceSet;
import sics.seiois.mlsserver.biz.der.metanome.evidenceset.TroveEvidenceSet;
import sics.seiois.mlsserver.biz.der.metanome.input.ColumnPair;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.input.ParsedColumn;
import sics.seiois.mlsserver.biz.der.metanome.predicates.operands.ColumnOperand;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.model.er.MlResult;
import sics.seiois.mlsserver.utils.MlsConstant;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.model.TaskObj;
import sics.seiois.mlsserver.utils.helper.MLUtil;

public class PredicateBuilder implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(PredicateBuilder.class);

    public static HashMap<Integer, Long> predicates_card = new HashMap<>();
    private double COMPARE_AVG_RATIO = 0.3d;

    private double minimumSharedValue = 0.30d;
    private double maximumSharedValue = 0.6d;

    private boolean noCrossColumn = true;

    private Set<Predicate> predicates;

    private Collection<Collection<Predicate>> predicateGroups;

    private Set<Predicate> predicates_fk;

    private Collection<Collection<Predicate>> predicateGroupsNumericalSingleColumn;
    private Collection<Collection<Predicate>> predicateGroupsNumericalCrossColumn;
    private Collection<Collection<Predicate>> predicateGroupsCategoricalSingleColumn;
    private Collection<Collection<Predicate>> predicateGroupsCategoricalCrossColumn;
    // constant
    private Collection<Collection<Predicate>> constantPredicateGroupsNumericalSingleColumn;
    private Collection<Collection<Predicate>> constantPredicateGroupsCategoricalSingleColumn;

    // store string => predicate
    public HashMap<String, Predicate> predicateStringMap;

    // store the inverted list of ML predicates
    public Map<String, List<List<Integer>>> mlList = new HashMap<>();

    public Map<String, ArrayList<ImmutablePair<Integer, Integer>>> mlLists = new HashMap<>();

    public static String relationAttrDelimiter = "->";

    private HashMap<String, List<Predicate>> agg;

    public Map<String, List<List<Integer>>> getMLLists() {
        return mlList;
    }

    public Set<String> usefulColumns;


    /*
     * Load Evidence Set
     */

    private ArrayList<HashSet<String>> predicateStringsEvidSet;
    private ArrayList<Long> evidCounts;
    private HashSet<String> predicateStrings;

    private IEvidenceSet evidenceSet;


    public void readEvidenceSet(BufferedReader br) {
        predicateStringsEvidSet = new ArrayList<>();
        evidCounts = new ArrayList<>();
        predicateStrings = new HashSet<>();
        try {
            String line;

            while ( (line = br.readLine()) != null) {
                if(StringUtils.isEmpty(line)){
                    continue;
                }
                String evid = line.split("SUPP")[0];
                String count = line.split("SUPP")[1];
                String[] es = evid.split(";");
                HashSet<String> hs = new HashSet<>();
                for (int i = 0; i < es.length; i++) {
                    hs.add(es[i].trim());
                    predicateStrings.add(es[i].trim());
                }
                evidCounts.add(Long.parseLong(count));
                predicateStringsEvidSet.add(hs);
            }

        } catch (Exception e) {
             throw new RuntimeException(e);
        }
    }

    public void readEvidenceSetOnSpark(Input input, TaskObj task) {
//        predicateStringsEvidSet = new ArrayList<>();
//        evidCounts = new ArrayList<>();
//        predicateStrings = new HashSet<>();
        long startTime = System.currentTimeMillis();
        int eviCount = 0;
        evidenceSet = new TroveEvidenceSet();

        for (PredicateSet ps : task.getEs()) {
            if (ps.containsPS(task.getCurrentPS())) {
                eviCount++;
                evidenceSet.add(ps, task.getEs().getCount(ps));
                for (Predicate pred : ps) {
                    predicates.add(pred);//这个只要过滤后的
                }
            }
        }
        //有highselectivity的常量，需要加进去
        for(Predicate pred : task.getCurrentPS()){
            predicates.add(pred);
        }
        for(Predicate pred : task.getAddablePS()){
            predicates.add(pred);
        }
        long endTime = System.currentTimeMillis();
        logger.info("###load ES cost:{}, evidence count in this task:{}, es total count:{}", endTime - startTime, eviCount,
                task.getEs().size());
    }

    public void readEvidenceSet(String evidset_file) {
        predicateStringsEvidSet = new ArrayList<>();
        evidCounts = new ArrayList<>();
        predicateStrings = new HashSet<>();

        FileReader fr = null;
        BufferedReader br = null;
        try {
            File file = new File(evidset_file);
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String line;

            while ( (line = br.readLine()) != null) {
                if(StringUtils.isEmpty(line)){
                    continue;
                }
                String evid = line.split("SUPP")[0];
                String count = line.split("SUPP")[1];
                String[] es = evid.split(";");
                HashSet<String> hs = new HashSet<>();
                for (int i = 0; i < es.length; i++) {
                    hs.add(es[i].trim());
                    predicateStrings.add(es[i].trim());
                }
                evidCounts.add(Long.parseLong(count));
                predicateStringsEvidSet.add(hs);
            }

        } catch (FileNotFoundException e) {
            logger.error("FileNotFoundException error", e);
        } catch (IOException e) {
            logger.error("IOException error", e);
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

    public PredicateBuilder(Input input, boolean noCrossColumn, double minimumSharedValue, double maximumSharedValue, String ml_file) {
        predicates = new HashSet<>();
        predicates_fk = new HashSet<>();
        predicateGroups = new ArrayList<>();
        mlList = new HashMap<>();
        this.noCrossColumn = noCrossColumn;
        this.minimumSharedValue = minimumSharedValue;
        this.maximumSharedValue = maximumSharedValue;

        this.addPredicatesML(ml_file, input);
        constructColumnPairs(input).forEach(pair -> {
            ColumnOperand<?> o1 = new ColumnOperand<>(pair.getC1(), 0);
            addPredicates(o1, new ColumnOperand<>(pair.getC2(), 1), pair.isJoinable(), pair.isComparable());
            if (pair.getC1() != pair.getC2()) {
                if ("eid".equals(pair.getC1().getName()) && "eid".equals(pair.getC2().getName())) {
                    addPredicates(o1, new ColumnOperand<>(pair.getC2(), 0), true, false);
                } else {
                    addPredicates(o1, new ColumnOperand<>(pair.getC2(), 0), pair.isJoinable(), false);
                }
            }
        });

        dividePredicateGroupsByType();

    }

    /**
        set lists for ML predicates
     */
    public void setMLList() {
        for (Predicate p : predicates) {
            if (!p.isML()) {
                continue;
            }
            p.setMLList(mlLists.get(p.toString()));
        }
    }

    public PredicateBuilder(Input input, boolean noCrossColumn, double minimumSharedValue, double maximumSharedValue,
                            String taskId, String eidName, Set<String> usefulColumns) {
        predicates = new HashSet<>();
        predicates_fk = new HashSet<>();
        predicateGroups = new ArrayList<>();
        mlList = new HashMap<>();
        this.noCrossColumn = noCrossColumn;
        this.minimumSharedValue = minimumSharedValue;
        this.maximumSharedValue = maximumSharedValue;
        this.usefulColumns = usefulColumns;

        // this.addPredicatesML(input, taskId, eidName);
        this.addPredicatesML_new(input, taskId, eidName);
        logger.info("#### predicate(ML and MD) gene success.predicates.size={},predicates={}", predicates.size(), predicates);

        constructColumnPairs(input).forEach(pair -> {
            ColumnOperand<?> o1 = new ColumnOperand<>(pair.getC1(), 0);
            addPredicates(o1, new ColumnOperand<>(pair.getC2(), 1), pair.isJoinable(), pair.isComparable());
            if (pair.getC1() != pair.getC2()) {
                if (StringUtils.isNotEmpty(eidName) && eidName.equals(pair.getC1().getName()) &&
                        eidName.equals(pair.getC2().getName())) {
                    addPredicates(o1, new ColumnOperand<>(pair.getC2(), 0), true, false);
                } else {
                    addPredicates(o1, new ColumnOperand<>(pair.getC2(), 0), pair.isJoinable(), false);
                }
            }
        });

        logger.info("#### predicate(not include constant) gene success.predicates.size={},predicates={}", predicates.size(), predicates);
        dividePredicateGroupsByType();
    }


    private void setPredicateStringMap() {
        for (Predicate pred : predicateProvider.getPredicates_().values()) {
            predicateStringMap.put(pred.toString(), pred);
        }
    }

    public static Predicate parsePredicateString(Input input, String predicateString) {
        if ("ML".equals(predicateString.substring(0, 2))) {

            // retrieve ML ID
            String mlID = predicateString.substring(3, predicateString.lastIndexOf("("));
            int nextIndex = predicateString.lastIndexOf("(") +1;

            // ML predicates
            String ss = predicateString.substring(nextIndex, predicateString.length() - 1);
            String[] preds = ss.split(",");
            // handle the first attribute
            String relation_name1 = preds[0].trim().split("\\.")[0];
            String attr_name1 = preds[0].trim().split("\\.")[2];
            // handle the second attribute
            String relation_name2 = preds[1].trim().split("\\.")[0];
            String attr_name2 = preds[1].trim().split("\\.")[2];
            ColumnOperand<?> o1 = new ColumnOperand<>(input.getParsedColumn(relation_name1,
                                    attr_name1), 0);
            ColumnOperand<?> o2 = new ColumnOperand<>(input.getParsedColumn(relation_name2,
                                    attr_name2), 1);
            Predicate predicate = predicateProvider.getPredicate(Operator.EQUAL, o1, o2, null, "ML");
            predicate.setML();
            return predicate;
        } else {
            // Constant predicates or logic predicates
            predicateString = predicateString.trim();
            String[] preds = predicateString.split(" ");
            // solve 1st operand
            String relation_name1 = preds[0].trim().split("\\.")[0];
            if (preds[0].trim().split("\\.").length <= 1) {
                logger.info("wrong");
            }
            String index_ = preds[0].trim().split("\\.")[1];
            int index = Integer.parseInt(index_.substring(1, index_.length()));
            String attr_name1 = preds[0].trim().split("\\.")[2];
            ColumnOperand<?> o1 = new ColumnOperand<>(input.getParsedColumn(relation_name1,
                                  attr_name1), index);
            // check whether it is constant or bivariate predicate
            if (!preds[2].trim().contains(".t") && preds[2].trim().split("\\.").length != 3) {
                // constant predicate
                String op = preds[1].trim();
                StringBuffer consStr = new StringBuffer();
                for(int i = 2; i < preds.length; i++) {
                    consStr.append(preds[i] + " ");
                }
                String constant = consStr.toString().trim();

                Predicate predicate = generatePredicate(o1, o1, op, constant);
                return predicate;
            } else {
                // logic operation
                String op = preds[1].trim();
                String relation_name2 = preds[2].trim().split("\\.")[0];
                String _index_ = preds[2].trim().split("\\.")[1];
                int _index = Integer.parseInt(_index_.substring(1, _index_.length()));
                String attr_name2 = preds[2].trim().split("\\.")[2];
                ColumnOperand<?> o2 = new ColumnOperand<>(input.getParsedColumn(relation_name2, attr_name2), _index);
                Predicate predicate = generatePredicate(o1, o2, op, null);
                return predicate;
            }
        }
    }

    public static Predicate generatePredicate(ColumnOperand<?> o1, ColumnOperand<?> o2,
                                        String op, String constant) {
        Predicate p = null;
        if (o1 == null || o2 == null) {
            return p;
        }
        if ("==".equals(op)) {
            p = predicateProvider.getPredicate(Operator.EQUAL, o1, o2, constant);
        } else if("<>".equals(op)) {
            p = predicateProvider.getPredicate(Operator.UNEQUAL, o1, o2, constant);
        } else if ("<=".equals(op)) {
            p = predicateProvider.getPredicate(Operator.LESS_EQUAL, o1, o2, constant);
        } else if ("<".equals(op)) {
            p = predicateProvider.getPredicate(Operator.LESS, o1, o2, constant);
        } else if (">=".equals(op)) {
            p = predicateProvider.getPredicate(Operator.GREATER_EQUAL, o1, o2, constant);
        } else if (">".equals(op)) {
            p = predicateProvider.getPredicate(Operator.GREATER, o1, o2, constant);
        } else {
            throw new MissingFormatArgumentException("Wrong Predicate Argument");
        }

        // check constant predicates
        if (constant != null) {
            p.setConstant(constant);
        }
        return p;
    }

    public Set<Predicate> getPredicates_fk() {
        return predicates_fk;
    }

    // A_1, ..., A_20, A_10 -> p_1: A_10 = A_10, p_2: A_8 = A_10
    // X -> p_1, X' -> p_2
    private ArrayList<ColumnPair> constructColumnPairs(Input input) {
        ArrayList<ColumnPair> pairs = new ArrayList<ColumnPair>();
        for (int i = 0; i < input.getColumns().length; ++i) {
            ParsedColumn<?> c1 = input.getColumns()[i];
            if(isSkip(c1.getName())) {
                continue;
            }
            for (int j = i; j < input.getColumns().length; ++j) {
                ParsedColumn<?> c2 = input.getColumns()[j];
                if(isSkip(c2.getName())) {
                    continue;
                }
                boolean joinable = isJoinable(c1, c2);
                boolean comparable = isComparable(c1, c2);
                // if (joinable || comparable)

                // if ML is enabled, we treat it as joinable
                if (this.agg.containsKey(this.toOperands(c1, c2)) || this.agg.containsKey(this.toOperands(c2, c1))) {
                    joinable = true;
                }

                if (joinable || comparable) {
                    // pairs.add(new ColumnPair(c1, c2, joinable, comparable));
                    pairs.add(new ColumnPair(c1, c2, true, comparable));
                }
            }
        }
        return pairs;
    }

    private boolean isJoinable(ParsedColumn<?> c1, ParsedColumn<?> c2) {
        if (noCrossColumn) {
            return c1.equals(c2);
        }

        // if two columns are from the same table but not equal, return false
        if ( (!c1.equals(c2)) && c1.getTableName().equals(c2.getTableName())) {
            return false;
        }

        if (!c1.getType().equals(c2.getType())) {
            return false;
        }

        if (c1.equals(c2)) {
            return true;
        }

        // check cardinality
        //if (c1.checkHighCard() || c2.checkHighCard())
            //return false;

        double ratio = c1.getSharedPercentage(c2);
        return ratio > minimumSharedValue; //&& ratio < maximumSharedValue;
    }

    private boolean isComparable(ParsedColumn<?> c1, ParsedColumn<?> c2) {
        if (noCrossColumn) {
            return c1.equals(c2) && (c1.getType().equals(Double.class) || c1.getType().equals(Long.class));
        }

        if (c1.equals(c2)) {
            return true;
        }

        //if (c1.checkHighCard() || c2.checkHighCard())
            //return false;

        if (!c1.getType().equals(c2.getType())) {
            return false;
        }

        // if two columns are from the same table but not equal, return false
        if ( (!c1.equals(c2)) && c1.getTableName().equals(c2.getTableName())) {
            return false;
        }

        if (c1.getType().equals(Double.class) || c1.getType().equals(Long.class)) {
//            if (c1.equals(c2))
//                return true;

            double avg1 = c1.getAverage();
            double avg2 = c2.getAverage();
            double sigma1 = c1.getSigma(avg1);
            double sigma2 = c2.getSigma(avg2);
            return Math.min(avg1, avg2) / Math.max(avg1, avg2) > COMPARE_AVG_RATIO &
                    Math.min(sigma1, sigma2) / Math.max(sigma1, sigma2) > COMPARE_AVG_RATIO;
        }
        return false;
    }

    public Set<Predicate> getPredicates() {
        return predicates;
    }

    public Collection<Collection<Predicate>> getPredicateGroups() {
        return predicateGroups;
    }

    public Collection<Collection<Predicate>> getNumericalSingleColPredicates() {

        Collection<Collection<Predicate>> numericalPredicates = new ArrayList<>();

        for (Collection<Predicate> predicateGroup : getPredicateGroups()) {

            if (predicateGroup.size() == 6) {
                numericalPredicates.add(predicateGroup);
            }
        }

        return numericalPredicates;
    }

    public Collection<Collection<Predicate>> getCategoricalSingleColPredicates() {

        Collection<Collection<Predicate>> categoricalPredicates = new ArrayList<>();

        for (Collection<Predicate> predicateGroup : getPredicateGroups()) {

            if (predicateGroup.size() == 2) {
                categoricalPredicates.add(predicateGroup);
            }
        }

        return categoricalPredicates;
    }

    private void dividePredicateGroupsByType() {

        predicateGroupsNumericalSingleColumn = new ArrayList<>();
        predicateGroupsNumericalCrossColumn = new ArrayList<>();
        predicateGroupsCategoricalSingleColumn = new ArrayList<>();
        predicateGroupsCategoricalCrossColumn = new ArrayList<>();
        // constant
        constantPredicateGroupsNumericalSingleColumn = new ArrayList<>();
        constantPredicateGroupsCategoricalSingleColumn = new ArrayList<>();

        for (Collection<Predicate> predicateGroup : getPredicateGroups()) {

            if (checkConstant(predicateGroup)) {
                if (checkNumerical(predicateGroup)) {
                   constantPredicateGroupsNumericalSingleColumn.add(predicateGroup);
                } else {
                    constantPredicateGroupsCategoricalSingleColumn.add(predicateGroup);
                }
                continue;
            }
            // handle non-constant predicates
            //if (predicateGroup.size() == 6) {// numeric
            if (checkNumerical(predicateGroup)) {
                if (predicateGroup.iterator().next().isCrossColumn()) {
                    predicateGroupsNumericalCrossColumn.add(predicateGroup);
                } else {
                    predicateGroupsNumericalSingleColumn.add(predicateGroup);
                }
            }

            // if (predicateGroup.size() == 2) {// categorical
            else {
                if (predicateGroup.iterator().next().isCrossColumn()) {
                    predicateGroupsCategoricalCrossColumn.add(predicateGroup);
                } else {
                    predicateGroupsCategoricalSingleColumn.add(predicateGroup);
                }
            }

        }

    }

    private boolean checkConstant(Collection<Predicate> ps) {
        for (Predicate p : ps) {
            if (p.isConstant()) {
                return true;
            }
            // break;
        }
        return false;
    }

    private boolean checkNumerical(Collection<Predicate> ps) {
        if (ps == null) {
            return false;
        }
        boolean T = false;
        for (Predicate p : ps) {
           if (p.getOperator() != Operator.EQUAL && p.getOperator() != Operator.UNEQUAL) {
              return true;
           }
        }
        return T;
    }

    // handle constant predicates
    public Collection<Collection<Predicate>> getConstantPredicateGroupsNumericalSingleColumn() {
        return constantPredicateGroupsNumericalSingleColumn;
    }

    public Collection<Collection<Predicate>> getConstantPredicateGroupsCategoricalSingleColumn() {
        return constantPredicateGroupsCategoricalSingleColumn;
    }

    public Collection<Collection<Predicate>> getPredicateGroupsNumericalSingleColumn() {
        return predicateGroupsNumericalSingleColumn;
    }

    public Collection<Collection<Predicate>> getPredicateGroupsNumericalCrossColumn() {
        return predicateGroupsNumericalCrossColumn;
    }

    public Collection<Collection<Predicate>> getPredicateGroupsCategoricalSingleColumn() {
        return predicateGroupsCategoricalSingleColumn;
    }

    public Collection<Collection<Predicate>> getPredicateGroupsCategoricalCrossColumn() {
        return predicateGroupsCategoricalCrossColumn;
    }


    /**
     * 在这里读取ML和相似度的计算结果
     * 添加相似度和ml 谓词和索引文件pair对
     * 相似度文件格式：ML#cosine@0.9(表名0->字段名1,字段名2;表名1->字段名1,字段名2):10383|10383;10108|10108;
     * 机器学习文件格式：ML#机器学习名称(表名0->字段名1,字段名2;表名1->字段名1,字段名2):10383|10383;10108|10108;
     * @param input
     * @param taskId
     * @param eidName
     */
    private void addPredicatesML_new(Input input, String taskId, String eidName) {
        //读取的文件名固定
        String similarResultHdfsPath = PredicateConfig.MLS_TMP_HOME + taskId + (StringUtils.isEmpty(eidName) ? "/similarResult" : "/erSimilarResult");
        String mlResultHdfsPath = PredicateConfig.MLS_TMP_HOME + taskId + (StringUtils.isEmpty(eidName) ? "/crMlResult" : "/erMlResult");
        logger.info("####[ML谓词] similarResultHdfsPath:{}, mlResultHdfsPath:{}", similarResultHdfsPath, mlResultHdfsPath);

        Set<Predicate> predicates = new HashSet<>();
        this.agg = new HashMap<>();

        FileSystem hdfs;
        List<Path> mlAndSimilarResultPathList = new ArrayList<>();
        try {
            hdfs = FileSystem.get(new Configuration());
            //相似度文件处理
            Path similarResultPath = new Path(similarResultHdfsPath);
            if(hdfs.exists(similarResultPath)) {
                FileStatus[] similarResultHdfsPathArray = hdfs.listStatus(similarResultPath);
                if(similarResultHdfsPathArray != null) {
                    for(int i = 0; i < similarResultHdfsPathArray.length; i++) {
                        mlAndSimilarResultPathList.add(similarResultHdfsPathArray[i].getPath());
                    }
                    logger.info("####[similar谓词]similar file size={}", similarResultHdfsPathArray.length);
                }
            } else {
                logger.info("####[similar谓词] similarResultPath not exists.similarPath={}", similarResultPath);
            }

            //ml文件处理
            Path mlResultPath = new Path(mlResultHdfsPath);
            if(hdfs.exists(mlResultPath)) {
                FileStatus[] mlResultHdfsPathArray = hdfs.listStatus(mlResultPath);
                if(mlResultHdfsPathArray != null) {
                    for(int i = 0; i < mlResultHdfsPathArray.length; i++) {
                        mlAndSimilarResultPathList.add(mlResultHdfsPathArray[i].getPath());
                    }
                    logger.info("####[ML谓词]ml file size={}", mlResultHdfsPathArray.length);
                }
            } else {
                logger.info("####[ML谓词] mlResultPath not exists.mlPath={}", mlResultPath);
            }


        } catch (IOException e) {
            logger.info("####[ML谓词] get hdfs file error", e);
            return;
        }


        try {
            for(Path mlOrSimilarPath : mlAndSimilarResultPathList) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(mlOrSimilarPath)));
                StringBuffer sb = new StringBuffer();
                String line;

                while ( (line = br.readLine()) != null) {
                    logger.info("####[ML结果转ES] fileName:{}********,pred_line:{}", mlOrSimilarPath.toString(), line);
                    MlResult mlResult = MLUtil.fromString2Object(line, input);
                    if("ML".equals(mlResult.getOp())) {
                        // construct ML predicates
//                        ParsedColumn lcolumn = new ParsedColumn<String>(
//                                mlResult.getLeftTable(),
//                                mlResult.getLeftColumn().trim(), String.class, 0);
//                        ColumnOperand op1 = new ColumnOperand<String>(lcolumn, 0);
//                        ParsedColumn rcolumn = new ParsedColumn<String>(
//                                mlResult.getRightTable(),
//                                mlResult.getRightColumn().trim(), String.class, 1);
//                        ColumnOperand op2 = new ColumnOperand<String>(rcolumn, 1);
//                        Predicate mlp = predicateProvider.getPredicate(Operator.EQUAL, op1, op2, null, mlResult.getMlOption());

                        ColumnOperand<?> o1 = new ColumnOperand<>(input.getParsedColumn(mlResult.getLeftTable(), mlResult.getLeftColumn().trim()), 0);
                        ColumnOperand<?> o2 = new ColumnOperand<>(input.getParsedColumn(mlResult.getRightTable(), mlResult.getRightColumn().trim()), 1);
                        Predicate mlp = predicateProvider.getPredicate(Operator.EQUAL, o1, o2, null, mlResult.getMlOption());
                        logger.info("####[ML谓词] ML predicate:{},o1:{},o2:{},mlOption:{}, pairsize", mlp, o1.toString(),
                                o2.toString(), mlp.getMlOption(), mlResult.getPairList().size());
                        predicates.add(mlp);

                        // load all satisfied tuple pairs
                        if (!mlLists.containsKey(mlp.toString())) {
                            mlLists.put(mlp.toString(), new ArrayList<>());
                        }

                        // use a heuristic method
                        int ccmlcount = 0;
                        for(List<Integer> pariList : mlResult.getPairList()) {
                            if (pariList.size() < 2) {
                                continue;
                            }
                            // skip tuple pair (tid[0] >= tid[1])
                            if (pariList.get(0) >= pariList.get(1)) {
                                continue;
                            }
                            if (ccmlcount >= o1.getColumn().getValueSize() || ccmlcount >= o2.getColumn().getValueSize()) {
                                break;
                            }
                            ccmlcount++;
                            ImmutablePair<Integer, Integer> pair = new ImmutablePair<>(pariList.get(0), pariList.get(1));
                            mlLists.get(mlp.toString()).add(pair);
                        }
                    }
                }
                for(Predicate p : predicates) {
                    String k = this.toOperands(p.getOperand1(), p.getOperand2());
                    if (!agg.containsKey(k)) {
                        agg.put(k, new ArrayList<Predicate>());
                    }
                    agg.get(k).add(p);
                }
                logger.info("####[ML结果转ES] init and ML predicate:{}", predicates);
            }
        } catch (IOException e) {
            logger.error("####[ML结果转ES] init and ML predicate exception", e);
        }

    }

    /**
     * 添加相似度和ml 谓词和索引文件pair对
     * @param input
     * @param taskId
     * @param eidName
     */
    private void addPredicatesML(Input input, String taskId, String eidName) {
        String similarResultHdfsPath = PredicateConfig.MLS_TMP_HOME + taskId + (StringUtils.isEmpty(eidName) ? "/similarResult" : "/erSimilarResult");
        String mlResultHdfsPath = PredicateConfig.MLS_TMP_HOME + taskId + (StringUtils.isEmpty(eidName) ? "/crMlResult" : "/erMlResult");
        logger.info("####[ML谓词] similarResultHdfsPath:{}, mlResultHdfsPath:{}", similarResultHdfsPath, mlResultHdfsPath);

        Set<Predicate> predicates = new HashSet<>();
        this.agg = new HashMap<>();

        FileSystem hdfs;
        List<Path> mlAndSimilarResultPathList = new ArrayList<>();
        try {
            hdfs = FileSystem.get(new Configuration());
            //相似度文件处理
            Path similarResultPath = new Path(similarResultHdfsPath);
            if(hdfs.exists(similarResultPath)) {
                FileStatus[] similarResultHdfsPathArray = hdfs.listStatus(similarResultPath);
                if(similarResultHdfsPathArray != null) {
                    for(int i = 0; i < similarResultHdfsPathArray.length; i++) {
                        mlAndSimilarResultPathList.add(similarResultHdfsPathArray[i].getPath());
                    }
                    logger.info("####[similar谓词]similar file size={}", similarResultHdfsPathArray.length);
                }
            } else {
                logger.info("####[similar谓词] similarResultPath not exists.similarPath={}", similarResultPath);
            }

            //ml文件处理
            Path mlResultPath = new Path(mlResultHdfsPath);
            if(hdfs.exists(mlResultPath)) {
                FileStatus[] mlResultHdfsPathArray = hdfs.listStatus(mlResultPath);
                if(mlResultHdfsPathArray != null) {
                    for(int i = 0; i < mlResultHdfsPathArray.length; i++) {
                        mlAndSimilarResultPathList.add(mlResultHdfsPathArray[i].getPath());
                    }
                    logger.info("####[ML谓词]ml file size={}", mlResultHdfsPathArray.length);
                }
            } else {
                logger.info("####[ML谓词] mlResultPath not exists.mlPath={}", mlResultPath);
            }


        } catch (IOException e) {
            logger.info("####[ML谓词] get hdfs file error", e);
            return;
        }


        try {
            for(Path mlOrSimilarPath : mlAndSimilarResultPathList) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(mlOrSimilarPath)));
                StringBuffer sb = new StringBuffer();
                String line;

                while ( (line = br.readLine()) != null) {
                    logger.info("####[ML结果转ES] fileName:{}********,pred_line:{}", mlOrSimilarPath.toString(), line);
                    MlResult mlResult = MLUtil.fromString2Object(line, input);
                    if("ML".equals(mlResult.getOp())) {
                        // construct ML predicates
//                        ParsedColumn lcolumn = new ParsedColumn<String>(
//                                mlResult.getLeftTable(),
//                                mlResult.getLeftColumn().trim(), String.class, 0);
//                        ColumnOperand op1 = new ColumnOperand<String>(lcolumn, 0);
//                        ParsedColumn rcolumn = new ParsedColumn<String>(
//                                mlResult.getRightTable(),
//                                mlResult.getRightColumn().trim(), String.class, 1);
//                        ColumnOperand op2 = new ColumnOperand<String>(rcolumn, 1);
//                        Predicate mlp = predicateProvider.getPredicate(Operator.EQUAL, op1, op2, null, mlResult.getMlOption());

                        ColumnOperand<?> o1 = new ColumnOperand<>(input.getParsedColumn(mlResult.getLeftTable(), mlResult.getLeftColumn().trim()), 0);
                        ColumnOperand<?> o2 = new ColumnOperand<>(input.getParsedColumn(mlResult.getRightTable(), mlResult.getRightColumn().trim()), 1);
                        Predicate mlp = predicateProvider.getPredicate(Operator.EQUAL, o1, o2, null, mlResult.getMlOption());
                        logger.info("####[ML谓词] ML predicate:{},o1:{},o2:{},mlOption:{}, pairsize", mlp, o1.toString(),
                                o2.toString(), mlp.getMlOption(), mlResult.getPairList().size());
                        predicates.add(mlp);

                        // load all satisfied tuple pairs
                        if (!mlList.containsKey(mlp.toString())) {
                            mlList.put(mlp.toString(), new ArrayList<List<Integer>>());
                        }

                        for(List<Integer> pariList : mlResult.getPairList()) {
                            mlList.get(mlp.toString()).add(pariList);
                        }
                    }
                }
                for(Predicate p : predicates) {
                    String k = this.toOperands(p.getOperand1(), p.getOperand2());
                    if (!agg.containsKey(k)) {
                        agg.put(k, new ArrayList<Predicate>());
                    }
                    agg.get(k).add(p);
                }
                logger.info("####[ML结果转ES] init and ML predicate:{}", predicates);
            }
        } catch (IOException e) {
            logger.error("####[ML结果转ES] init and ML predicate exception", e);
        }

    }

    private void addPredicatesML(String pred_file, Input input) {
        logger.info("#### pred_file:{}", pred_file);
        Set<Predicate> predicates = new HashSet<>();
        this.agg = new HashMap<>();
        if (pred_file == null || "".equals(pred_file)) {
            return;
        }

        BufferedReader br = null;
        try{
            File file = new File(pred_file);
            FileReader fr = new FileReader(file);
            br = new BufferedReader(fr);
            StringBuffer sb = new StringBuffer();
            String line;

            while ( (line = br.readLine()) != null) {
                String op = line.split(";")[0];
                if ("ML".equals(op)) {
                    logger.info("#### pred_line:{}", line);
                    String names1 = line.split(";")[1];
                    String relation_name1 = names1.split(relationAttrDelimiter)[0];
                    String attr_name1 = names1.split(relationAttrDelimiter)[1];
                    String names2 = line.split(";")[2];
                    String relation_name2 = names2.split(relationAttrDelimiter)[0];
                    String attr_name2 = names2.split(relationAttrDelimiter)[1];
                    String[] tuple_pairs = line.split(";");
                    int tuple_id_start1 = input.getTupleIDStart(relation_name1);
                    int tuple_id_start2 = input.getTupleIDStart(relation_name2);
                    int offset = 3;
                    int len = tuple_pairs.length - offset;
                    // construct ML predicates
                    ColumnOperand<?> o1 = new ColumnOperand<>(input.getParsedColumn(relation_name1, attr_name1), 0);
                    ColumnOperand<?> o2 = new ColumnOperand<>(input.getParsedColumn(relation_name2, attr_name2), 1);
                    Predicate mlp = predicateProvider.getPredicate(Operator.EQUAL, o1, o2, null, "ML");
                    mlp.setML();
                    logger.info("#### ML predicate:{},o1:{},o2:{}", mlp,o1.toString(), o2.toString());
                    predicates.add(mlp);

                    // load all satisfied tuple pairs
                    if (!mlList.containsKey(mlp.toString())) {
                        mlList.put(mlp.toString(), new ArrayList<List<Integer>>());
                    }
                    for (int i = 0; i < len; i += 2) {
                        Integer tid1 = Integer.parseInt(tuple_pairs[offset + i]) + tuple_id_start1;
                        Integer tid2 = Integer.parseInt(tuple_pairs[offset + i + 1]) + tuple_id_start2;
                        List<Integer> tidList = new ArrayList<>();
                        tidList.add(tid1);
                        tidList.add(tid2);
                        mlList.get(mlp.toString()).add(tidList);
                    }

                } else {
                    // error
                    /*
                    if(op == "Constant") {
                    int col = Integer.parseInt(line.split(";")[1]);
                    String constant = line.split(";")[2];
                    String opc = line.split(";")[3];
                    ColumnOperand<?> o = new ColumnOperand<>(input.getColumns()[col], 0);
                    Predicate constantp = null;
                    if(opc == "==") {
                        constantp = predicateProvider.getPredicate(Operator.EQUAL, o, o);
                    } else if(opc == "<=") {
                        constantp = predicateProvider.getPredicate(Operator.LESS_EQUAL, o, o);
                    } else if(opc == ">=") {
                        constantp = predicateProvider.getPredicate(Operator.GREATER_EQUAL, o, o);
                    } else {
                        throw new Exception("wrong constant argument");
                    }
                    constantp.setConstant(constant);
                    predicates.add(constantp);
                    }
                     */
                }
            }
            // record all ML predicates
            //this.agg = new HashMap<>();
            for(Predicate p : predicates) {
                //String k = p.getOperands();
                String k = this.toOperands(p.getOperand1(), p.getOperand2());
                if (!agg.containsKey(k)) {
                    agg.put(k, new ArrayList<Predicate>());
                }
                agg.get(k).add(p);
            }
            logger.info("#### init and ML predicate:{}", predicates);
            /*
            for(List<Predicate> ll : agg.values()) {
                this.predicateGroups.add(ll);
            }
            */

        } catch(IOException e){
            logger.error("IOException error", e);
        } catch (Exception e) {
            logger.error("#### add ML predicate Exception", e);
        } finally{
            try {
                if(br != null){
                    br.close();
                }
            } catch (Exception e) {
                logger.error("BufferedReader close error", e);
            }
        }
    }

    private void addPredicates(ColumnOperand<?> o1, ColumnOperand<?> o2, boolean joinable, boolean comparable) {
        Set<Predicate> predicates = new HashSet<Predicate>();

        // add ML predicates
        String key = this.toOperands(o1, o2);
//        logger.info("####predicate operand:{}", key);
//        for(String k : agg.keySet()) {
//            logger.info("####ml predicate operand:{}", k);
//        }

        if (this.agg.containsKey(key) || this.agg.containsKey(this.toOperands(o2, o1))) {
            for (Predicate ml : this.agg.get(key)) {
                predicates.add(ml);
            }
        }

        for (Operator op : Operator.values()) {

            // do not consider predicates with STRING and <, <=, >, >=
            if (op != Operator.EQUAL && op != Operator.UNEQUAL) {
                if (o1.getColumn().getType() == String.class || o2.getColumn().getType() == String.class) {
                    continue;
                }
            }

            // EQUAL and UNEQUAL must be joinable, all other comparable
            // if (op == Operator.EQUAL || op == Operator.UNEQUAL) {
            if (op == Operator.EQUAL || op == Operator.UNEQUAL) {
                // if (joinable ) {
                if (joinable && (o1.getIndex() != o2.getIndex())) {
                    // predicates.add(predicateProvider.getPredicate(op, o1, o2));
                    Predicate padd = predicateProvider.getPredicate(op, o1, o2);

                    // store cardinality of EQUAL and machine learning
                    if (op == Operator.EQUAL) {

                        // check and set ML predicates
                        //if (agg.containsKey(padd.getOperands()))
                        //    padd.setML();

                        // store cardinality of EQUALITY
                        Integer _hash_ = new Integer(padd.hashCode());
                        if (predicates_card.get(_hash_) == null) {
                            long card = o1.getColumn().getSharedCardinality(o2.getColumn());
                            predicates_card.put(_hash_, card);
                        }
                    }
                    predicates.add(padd);
                }
                if (joinable && op == Operator.EQUAL && o1.getColumn().getTableName() != o2.getColumn().getTableName()) {
                    predicates_fk.add(predicateProvider.getPredicate(op, o1, o2));
                    predicates_fk.add(predicateProvider.getPredicate(op, o2, o1));
                }

            } else if (comparable) {
                predicates.add(predicateProvider.getPredicate(op, o1, o2));
            }
        }
        if (predicates.size() > 0) {
            this.predicates.addAll(predicates);
            this.predicateGroups.add(predicates);
        }
    }

    // column pair to a string
    private String toOperands(ColumnOperand<?> o1, ColumnOperand<?> o2) {
        return o1.toString() + "." + o2.toString();
    }

    private String toOperands(ParsedColumn<?> o1, ParsedColumn<?> o2) {
        return new ColumnOperand<>(o1, 0).toString() + "." + new ColumnOperand<>(o2, 1);
    }

    private static final PredicateProvider predicateProvider = PredicateProvider.getInstance();

    private boolean isSkip(String colName) {
        if (usefulColumns == null || usefulColumns.size() == 0) {
            return false;
        }
        if(usefulColumns.contains(colName)) {
            return false;
        }
        return true;
    }

}
