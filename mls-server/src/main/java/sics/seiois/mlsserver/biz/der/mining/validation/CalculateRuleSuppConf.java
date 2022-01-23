package sics.seiois.mlsserver.biz.der.mining.validation;

import de.metanome.algorithm_integration.Operator;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.backend.input.file.FileIterator;
import io.swagger.models.auth.In;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.input.Dir;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.mlsel.MLSelection;
import sics.seiois.mlsserver.biz.der.metanome.predicates.ConstantPredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.Message;
import sics.seiois.mlsserver.biz.der.mining.ParallelRuleDiscoverySampling;
import sics.seiois.mlsserver.biz.der.mining.utils.ParsedColumnLight;
import sics.seiois.mlsserver.biz.der.mining.utils.PredicateProviderIndex;
import sics.seiois.mlsserver.biz.der.mining.utils.WorkUnit;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class CalculateRuleSuppConf {

    private static Logger logger = LoggerFactory.getLogger(CalculateRuleSuppConf.class);
    private static final PredicateProviderIndex predicateProviderIndex = PredicateProviderIndex.getInstance();

    private ArrayList<Predicate> allPredicates;
    private ArrayList<Predicate> allExistPredicates;
    private Input input;
    private int maxTupleNum;
    private int maxOneRelationNum;
    private int allCount;
    private long support;
    private double confidence = 0.9;

    private ParallelRuleDiscoverySampling parallelRuleDiscoverySampling;
    private int nonConsPredicatesNum;
    private int[] nonConsPredicates;


    private void prepareAllPredicatesMultiTuples() {
        // add to Predicate Set
        for (Predicate p : this.allPredicates) {
            predicateProviderIndex.addPredicate(p);
        }

        HashMap<String, ParsedColumnLight<?>> colsMap = new HashMap<>();
        for (Predicate p : this.allPredicates) {
            String k = p.getOperand1().getColumn().toStringData();
            if (!colsMap.containsKey(k)) {
                ParsedColumnLight<?> col = new ParsedColumnLight<>(p.getOperand1().getColumn());
                colsMap.put(k, col);
            }
            k = p.getOperand2().getColumn().toStringData();
            if (!colsMap.containsKey(k)) {
                ParsedColumnLight<?> col = new ParsedColumnLight<>(p.getOperand2().getColumn());
                colsMap.put(k, col);
            }
        }
        // delete value int data of ParsedColumn
        for (Predicate p : this.allPredicates) {
            p.getOperand1().getColumn().cleanValueIntBeforeBroadCast();
            p.getOperand2().getColumn().cleanValueIntBeforeBroadCast();
        }
        // insert parsedColumnLight for each predicate
        PredicateSet ps = new PredicateSet();
        for (Predicate p : this.allPredicates) {
            // set columnLight
            String k = p.getOperand1().getColumn().toStringData();
            p.getOperand1().setColumnLight(colsMap.get(k));
            k = p.getOperand2().getColumn().toStringData();
            p.getOperand2().setColumnLight(colsMap.get(k));
            ps.add(p);
            for (int t1 = 0; t1 < this.maxTupleNum; t1++) {
                if (p.isConstant()) {
                    Predicate p_new = predicateProviderIndex.getPredicate(p, t1, t1);
                    k = p_new.getOperand1().getColumn().toStringData();
                    p_new.getOperand1().setColumnLight(colsMap.get(k));
                    k = p_new.getOperand2().getColumn().toStringData();
                    p_new.getOperand2().setColumnLight(colsMap.get(k));
                    ps.add(p_new);
                    continue;
                }
                for (int t2 = t1 + 1; t2 < this.maxTupleNum; t2++) {
                    Predicate p_new = predicateProviderIndex.getPredicate(p, t1, t2);
                    k = p_new.getOperand1().getColumn().toStringData();
                    p_new.getOperand1().setColumnLight(colsMap.get(k));
                    k = p_new.getOperand2().getColumn().toStringData();
                    p_new.getOperand2().setColumnLight(colsMap.get(k));
                    ps.add(p_new);
                }
            }
        }

        this.allExistPredicates = new ArrayList<>();
        this.nonConsPredicatesNum = 0;
        for (Predicate p : ps) {
            this.allExistPredicates.add(p);
            if (!p.isConstant()) {
                this.nonConsPredicatesNum = this.nonConsPredicatesNum + 1;
            }
        }
    }

    // remove the constant predicates related to Property_Feature dataset
    private void removePropertyFeatureCPredicates(List<Predicate> allPredicates) {
        ArrayList<Predicate> removePredicates = new ArrayList<>();
        for (Predicate p : allPredicates) {
            if (p.isConstant() && p.getOperand1().getColumn().getTableName().contains("Property_Features")) {
                removePredicates.add(p);
            }
        }
        for (Predicate p : removePredicates) {
            allPredicates.remove(p);
        }
    }

    public static Map<String, String> convert(String[] args) {
        logger.info("arguments : {}", args);
        Map<String, String> argsMap = new HashMap<>();
        for (String arg : args) {
            logger.info("argument : {}", arg);
            String[] name2value = arg.split("=");
            argsMap.put(name2value[0], name2value[1]);
        }
        return argsMap;
    }

    public void preparePredicates(String args[]) {
        logger.info("### begin to prepare predicates...");
        Map<String, String> argsMap = convert(args);
        String directory_path = argsMap.get("directory_path");
        String constant_file = argsMap.get("constant_file");
        int chunkLength = Integer.parseInt(argsMap.get("chunkLength"));
        this.maxTupleNum = Integer.parseInt(argsMap.get("maxTupleNum"));

        Boolean noCrossColumn = true;
        double minimumSharedValue = 0.30d;
        double maximumSharedValue = 0.7d;
        double rowLimit = 1.0;
        double relation_num_ratio = 1.0;
        String mlsel_file = null;
        String ml_config_file = null;
        String type_attr_file = null;
        double support_ratio = 0.0001;

        Dir directory = new Dir(directory_path, relation_num_ratio);

        ArrayList<FileReader> fileReaders = new ArrayList<>();

        try {
            Collection<RelationalInput> relations = new ArrayList<>();
            Iterator<String> iter_rname = directory.iterator_r();
            Iterator<String> iter_path = directory.iterator_a();
            while (iter_rname.hasNext()) {
                String rname = iter_rname.next();
                String rpath = iter_path.next();
                FileReader fileReader = new FileReader(rpath);
                fileReaders.add(fileReader);
                relations.add(new FileIterator(rname, fileReader,
                        new ConfigurationSettingFileInput(rpath)));
            }
            this.input = new Input(relations, rowLimit, type_attr_file);
            this.maxOneRelationNum = this.input.getMaxTupleOneRelation();
            this.allCount = this.input.getAllCount();

            PredicateBuilder predicates = new PredicateBuilder(this.input, noCrossColumn, minimumSharedValue, maximumSharedValue, ml_config_file);
            ConstantPredicateBuilder cpredicates = new ConstantPredicateBuilder(this.input, constant_file);
            logger.info("Size of the predicate space:" + (predicates.getPredicates().size() + cpredicates.getPredicates().size()));

            // construct PLI index
            this.input.buildPLIs_col_OnSpark(chunkLength);

            // load ML Selection
            MLSelection mlsel = new MLSelection();
            mlsel.configure(mlsel_file);

            // calculate support
            this.support = (long) (this.allCount * (this.allCount - 1) * support_ratio);

            this.allPredicates = new ArrayList<>();
            for (Predicate p : predicates.getPredicates()) {
                if (p.isML() || p.getOperator() == Operator.EQUAL) {
                    this.allPredicates.add(p);
                }
            }

            // set value INT for constant predicates
            HashSet<Predicate> constantPs = new HashSet<>();
            for (Predicate cp : cpredicates.getPredicates()) {
                constantPs.add(cp);
            }
            this.input.transformConstantPredicates(constantPs);

            // add constant predicates
            for (Predicate p : constantPs) {
                if (p.getConstant().equals("")) {
                    logger.info("empty value for predicate: {}", p);
                    continue;
                }
                this.allPredicates.add(p);
            }

            logger.info("allPredicates size: {}", allPredicates.size());

            this.parallelRuleDiscoverySampling = new ParallelRuleDiscoverySampling(this.allPredicates, 10000, this.maxTupleNum,
                    this.support, (float) this.confidence, this.maxOneRelationNum, this.input, this.allCount,
                    0, 0, 0, 0, 0, 0, 0, 0, 0);

            removePropertyFeatureCPredicates(this.allPredicates);
            logger.info("After remove Property_Feature constant predicates, allPredicates size: {}", this.allPredicates.size());

            prepareAllPredicatesMultiTuples();

        } catch (FileNotFoundException | InputIterationException e) {
            logger.info("Cannot load file\n");
        } finally {
            for (FileReader fileReader : fileReaders) {
                try {
                    if (fileReader != null) {
                        fileReader.close();
                    }
                } catch (Exception e) {
                    logger.error("FileReader close error", e);
                }
            }
        }
        logger.info("### finish preparing predicates!");
    }

    public int getAllPredicatesNum() {
        logger.info("allExistPredicates size: {}", this.allExistPredicates.size());
        for (int i = 0; i < this.allExistPredicates.size(); i++) {
            logger.info("{} : {}", i, this.allExistPredicates.get(i).toString());
        }
        return this.allExistPredicates.size();
    }

    public int[] getNonConstantPredicateIDs() {
        this.nonConsPredicates = new int[this.nonConsPredicatesNum];
        int k = 0;
        for (int i = 0; i < this.allExistPredicates.size(); i++) {
            if (!this.allExistPredicates.get(i).isConstant()) {
                this.nonConsPredicates[k] = i;
                k = k + 1;
            }
        }
        return this.nonConsPredicates;
    }

    // sequence: "1 3 12,5;2 1,7;..."
    public double[] getConfidence(String sequence) {
        logger.info("### begin to calculate confidence for sequence: {}", sequence);

        ArrayList<WorkUnit> workUnits = new ArrayList<>();
        ArrayList<Boolean> isConstantRHS = new ArrayList<>();
        ArrayList<Predicate> rhss = new ArrayList<>();
        for (String seq_rule : sequence.split(";")) {
            String[] lhs_ids = seq_rule.split(",")[0].split(" ");
            String rhs_id = seq_rule.split(",")[1];

            WorkUnit workunit = new WorkUnit();
            Predicate rhs = this.allExistPredicates.get(Integer.parseInt(rhs_id));
            rhss.add(rhs);
            workunit.addRHS(rhs);
            if (rhs.isConstant()) {
                isConstantRHS.add(true);
            } else {
                isConstantRHS.add(false);
            }

            for (String lhs_id : lhs_ids) {
                workunit.addCurrent(this.allExistPredicates.get(Integer.parseInt(lhs_id)));
            }
            workunit.setTransferData();
            workUnits.add(workunit);
        }

        ArrayList<WorkUnit> workUnits_new = new ArrayList<>();
        this.parallelRuleDiscoverySampling.solveSkewness(workUnits, workUnits_new);

        List<Message> messages = this.parallelRuleDiscoverySampling.runLocal_new(workUnits_new);
        logger.info("messages size: {}", messages.size());
        double[] confidences = new double[messages.size()];
        for (int i = 0; i < messages.size(); i++) {
            long lhs_supp = 0;
            Message message = messages.get(i);
            if (isConstantRHS.get(i)) {
                lhs_supp = message.getCurrentSuppCP0();
            } else {
                lhs_supp = message.getCurrentSupp();
            }
            long rule_supp = message.getAllCurrentRHSsSupport().get(rhss.get(i));
            if (lhs_supp == 0) {
                confidences[i] = 0.0;
            } else {
                confidences[i] = rule_supp * 1.0 / lhs_supp;
            }
            logger.info("rule: {} -> {}, lhs_supp: {}, supp: {}, conf: {}", workUnits.get(i).getCurrrent().toString(), rhss.get(i).toString(), lhs_supp, rule_supp, confidences[i]);
        }
        return confidences;
    }

    public static void main(String[] args) {
        logger.info("Given a rule, calculate its support and confidence");

//        String[] args_ = {"directory_path=D:\\REE\\tmp\\airports", "constant_file=D:\\REE\\tmp\\constant_airports.txt",
//                "chunkLength=200000", "maxTupleNum=2"};

        String[] args_ = {"directory_path=D:/REE/tmp/property/property", "constant_file=D:/REE/tmp/property/constant_property.txt",
                "chunkLength=200000", "maxTupleNum=2"};

//        String[] args_ = {"directory_path=D:\\REE\\tmp\\aminer_test", "constant_file=D:\\REE\\tmp\\constant_aminer.txt",
//                "chunkLength=200000", "maxTupleNum=2"};

        CalculateRuleSuppConf calculateRuleSuppConf = new CalculateRuleSuppConf();
        calculateRuleSuppConf.preparePredicates(args_);
        calculateRuleSuppConf.getAllPredicatesNum();

//        calculateRuleSuppConf.getConfidence("1 11,5");
//        calculateRuleSuppConf.getConfidence("5 6 11 12 18 19,5");
//        calculateRuleSuppConf.getConfidence("1 3 7 16,5");

        calculateRuleSuppConf.getConfidence("78 107,37");

//        calculateRuleSuppConf.getConfidence("16 30,37");

    }
}
