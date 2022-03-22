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

import java.io.*;
import java.util.*;

public class CalculateRuleSuppConf {

    private static Logger logger = LoggerFactory.getLogger(CalculateRuleSuppConf.class);
    private static final PredicateProviderIndex predicateProviderIndex = PredicateProviderIndex.getInstance();

    private ArrayList<Predicate> allPredicates;
    private ArrayList<Predicate> allExistPredicates;
    private ArrayList<Predicate> applicationRHSs;
    private String table_name;
    private Input input;
    private int maxTupleNum;
    private int maxOneRelationNum;
    private int allCount;
    private long support;
    private double confidence = 0.9;

    private ParallelRuleDiscoverySampling parallelRuleDiscoverySampling;
    private int nonConsPredicatesNum;
    private int[] nonConsPredicates;

    private long[] supports;
    private double[] confidences;

    private int filter_enum_number = 5;

    private HashMap<Integer, Predicate> idx_predicates;


    private ArrayList<Predicate> applicationDrivenSelection(List<Predicate> predicates) {
        int whole_num_nonCons = 0;
        int whole_num_cons = 0;
        for (Predicate p : predicates) {
            if (p.isConstant()) {
                whole_num_cons++;
            } else {
                whole_num_nonCons++;
            }
        }

        ArrayList<Predicate> applicationRHSs = new ArrayList<>();
        // 1 - choose the first few non-constant rhss
//        int NUM_rhs = 4;
//        if (NUM_rhs > whole_num_nonCons) {
//            NUM_rhs = whole_num_nonCons;
//        }
//        logger.info("#### choose the first {} rhss", NUM_rhs);
//        int count = 0;
//        HashMap<String, Predicate> temp = new HashMap<>();
//        for (Predicate p : predicates) {
//            if (p.isConstant()) {
//                continue;
//            }
//            if (!temp.containsKey(p.toString())) {
//                temp.put(p.toString(), p);
//            }
//        }
//
//        for (Map.Entry<String, Predicate> entry : temp.entrySet()) {
//            applicationRHSs.add(entry.getValue());
//            count++;
//            if (count == NUM_rhs) {
//                break;
//            }
//        }


        // 2 - choose some rhss with minimum support value
//        int NUM_rhs = 4;
//        if (NUM_rhs > predicates.size()) {
//            NUM_rhs = predicates.size();
//        }
//        logger.info("#### choose {} RHSs with minimum support value", NUM_rhs);
//        ArrayList<Long> allSupports = new ArrayList<>();
//        for (Predicate p : predicates) {
//            allSupports.add(p.getSupport());
//        }
//        allSupports.sort(Comparator.naturalOrder());
//        Long minSupp = allSupports.get(NUM_rhs - 1);
//        for (Predicate p : predicates) {
//            if (p.getSupport() <= minSupp) {
//                applicationRHSs.add(p);
//            }
//        }
//        if (applicationRHSs.size() > NUM_rhs) { // maybe exist redundant supp value minSupp
//            applicationRHSs.subList(0, NUM_rhs);
//        }


        // 3 - choose 4 rhss: one min-supp, one max-supp, and two random rhss
//        logger.info("#### choose 4 RHSs: one min-supp, one max-supp, and two random RHSs");
//        Collections.sort(predicates, new Comparator<Predicate>() {
//            @Override
//            public int compare(Predicate o1, Predicate o2) {
//                return o1.toString().compareTo(o2.toString());
//            }
//        });
//        Random rand = new Random();
//        rand.setSeed(1234567);
//        HashSet<Integer> random_idx = new HashSet<>();
//        while (random_idx.size() < 2) {
//            int idx = rand.nextInt(predicates.size());
//            random_idx.add(idx);
//        }
//        for (int choose_idx : random_idx) {
//            applicationRHSs.add(predicates.get(choose_idx));
//        }
//        ArrayList<Long> allSupports = new ArrayList<>();
//        for (Predicate p : predicates) {
//            allSupports.add(p.getSupport());
//        }
//        allSupports.sort(Comparator.naturalOrder());
//        Long minSupp = allSupports.get(0);
//        Long maxSupp = allSupports.get(allSupports.size() - 1);
//        if (applicationRHSs.get(0).getSupport() == minSupp || applicationRHSs.get(1).getSupport() == minSupp) {
//            minSupp = allSupports.get(1);
//        }
//        if (applicationRHSs.get(0).getSupport() == maxSupp || applicationRHSs.get(1).getSupport() == maxSupp) {
//            maxSupp = allSupports.get(allSupports.size() - 2);
//        }
//        for (Predicate p : predicates) {
//            if (applicationRHSs.contains(p)) {
//                continue;
//            }
//            if (p.getSupport() == minSupp || p.getSupport() == maxSupp) {
//                applicationRHSs.add(p);
//            }
//            if (applicationRHSs.size() == 4) {
//                break;
//            }
//        }

        // 4. randomly choose some RHS.
//        int NUM_rhs = 4;
//        if (NUM_rhs > predicates.size()) {
//            NUM_rhs = predicates.size();
//        }
//        logger.info("#### randomly choose {} RHSs", NUM_rhs);
//        Collections.sort(predicates, new Comparator<Predicate>() {
//            @Override
//            public int compare(Predicate o1, Predicate o2) {
//                return o1.toString().compareTo(o2.toString());
//            }
//        });
//        Random rand = new Random();
//        rand.setSeed(1234567);
//        HashSet<Integer> random_idx = new HashSet<>();
//        while (random_idx.size() < NUM_rhs) {
//            int idx = rand.nextInt(predicates.size());
//            random_idx.add(idx);
//        }
//        for (int choose_idx : random_idx) {
//            applicationRHSs.add(predicates.get(choose_idx));
//        }

        // 5. randomly choose (n+m) RHSs, including n nonConstant and m constant RHSs
//        int NUM_Constant = 3;
//        int NUM_NonConstant = 2;
//        if (NUM_Constant > whole_num_cons) {
//            NUM_Constant = whole_num_cons;
//        }
//        if (NUM_NonConstant > whole_num_nonCons) {
//            NUM_NonConstant = whole_num_nonCons;
//        }
//        logger.info("#### randomly choose {} RHSs, including {} nonConstant RHSs and {} constant RHSs", NUM_Constant + NUM_NonConstant, NUM_NonConstant, NUM_Constant);
//        Collections.sort(predicates, new Comparator<Predicate>() {
//            @Override
//            public int compare(Predicate o1, Predicate o2) {
//                return o1.toString().compareTo(o2.toString());
//            }
//        });
//        Random rand = new Random();
//        rand.setSeed(1234567);
//        HashSet<Integer> random_idx = new HashSet<>();
//        int constant_num = 0;
//        int nonConstant_num = 0;
//        while (random_idx.size() < (NUM_Constant + NUM_NonConstant)) {
//            int idx = rand.nextInt(predicates.size());
//            if (predicates.get(idx).isConstant()) {
//                if (constant_num < NUM_Constant && !random_idx.contains(idx)) {
//                    random_idx.add(idx);
//                    constant_num = constant_num + 1;
//                }
//            } else {
//                if (nonConstant_num < NUM_NonConstant && !random_idx.contains(idx)) {
//                    random_idx.add(idx);
//                    nonConstant_num = nonConstant_num + 1;
//                }
//            }
//        }
//        for (int choose_idx : random_idx) {
//            applicationRHSs.add(predicates.get(choose_idx));
//        }

        // 6. randomly choose some non-constant RHS.
        int NUM_NonConstant = 4;
        if (NUM_NonConstant > whole_num_nonCons) {
            NUM_NonConstant = whole_num_nonCons;
        }
        logger.info("#### randomly choose {} non-constant RHSs", NUM_NonConstant);
        Collections.sort(predicates, new Comparator<Predicate>() {
            @Override
            public int compare(Predicate o1, Predicate o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        Random rand = new Random();
        rand.setSeed(1234567);
        HashSet<Integer> random_idx = new HashSet<>();
        while (random_idx.size() < NUM_NonConstant) {
            int idx = rand.nextInt(predicates.size());
            if (!predicates.get(idx).isConstant()) {
                random_idx.add(idx);
            }
        }
        for (int choose_idx : random_idx) {
            applicationRHSs.add(predicates.get(choose_idx));
        }

        // 7. use all non-constant predicates as RHSs
//        logger.info("#### choose all non-constant RHSs");
//        for (Predicate p : predicates) {
//            if (!p.isConstant()) {
//                applicationRHSs.add(p);
//            }
//        }

        // 8. use all predicates as RHSs
//        logger.info("#### choose all RHSs");
//        for (Predicate p : predicates) {
//            applicationRHSs.add(p);
//        }

        // 9. test NCVoter
//        logger.info("#### choose voting_intention as RHS");
//        for (Predicate p : predicates) {
//            if (p.getOperand1().toString(0).equals("t0.voting_intention")) {
//                applicationRHSs.add(p);
//            }
//        }

        logger.info("applicationRHSs size : {}", applicationRHSs.size());
        for (Predicate p : applicationRHSs) {
            logger.info("applicationRHSs: {}", p.toString());
        }
        return applicationRHSs;
    }

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

    private void removeEnumPredicates(List<Predicate> allPredicates, ArrayList<Predicate> allExistPredicates) {
        ArrayList<Predicate> removePredicates = new ArrayList<>();
        for (Predicate p : allPredicates) {
            if (!p.isConstant()) {
                continue;
            }
            if (p.getOperand1().getColumnLight().getUniqueConstantNumber() < this.filter_enum_number ||
                p.getOperand2().getColumnLight().getUniqueConstantNumber() < this.filter_enum_number) {
                removePredicates.add(p);
            }
        }
        logger.info("#### Filter Enum Constant Predicates size for X: {}", removePredicates.size());
        for (Predicate p : removePredicates) {
            allPredicates.remove(p);
            allExistPredicates.remove(p);
        }
    }

    private void removeEnumPredicates(List<Predicate> allPredicates) {
        ArrayList<Predicate> removePredicates = new ArrayList<>();
        for (Predicate p : allPredicates) {
            if (p.getOperand1().getColumnLight().getUniqueConstantNumber() < this.filter_enum_number ||
                p.getOperand2().getColumnLight().getUniqueConstantNumber() < this.filter_enum_number) {
                removePredicates.add(p);
            }
        }
        logger.info("#### Filter Enum Constant Predicates size for RHSs: {}", removePredicates.size());
        for (Predicate p : removePredicates) {
            allPredicates.remove(p);
        }
    }

    /**
     * if RHS only contain non-constant predicates, then filter the irrelevant predicates
     * */
    private void filterIrrelevantPredicates(ArrayList<Predicate> applicationRHSs, List<Predicate> allPredicates) {
        // filter the predicates that are irrelevant to rhs
        boolean allNonConstant = true;
        HashSet<String> relationRHS = new HashSet<>();
        for (Predicate p : applicationRHSs) {
            if (p.isConstant()) {
                allNonConstant = false;
                break;
            }
            relationRHS.add(p.getOperand1().getColumn().getTableName());
            relationRHS.add(p.getOperand2().getColumn().getTableName());
        }
//        for (String name : relationRHS) {
//            logger.info("Table name: {}", name);
//        }
        if (allNonConstant) {
            ArrayList<Predicate> removePredicates = new ArrayList<>();
            for (Predicate p : allPredicates) {
                String name_1 = p.getOperand1().getColumn().getTableName();
                String name_2 = p.getOperand2().getColumn().getTableName();
//                logger.info("currPredicate: {}, name_1: {}, name_2: {}", p, name_1, name_2);
                if (!relationRHS.contains(name_1) || !relationRHS.contains(name_2)) {
                    removePredicates.add(p);
                }
            }
            logger.info("#### Filter Irrelevant Predicates size: {}", removePredicates.size());
            for (Predicate p : removePredicates) {
//                logger.info("remove predicate: {}", p);
                allPredicates.remove(p);
            }
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
        this.filter_enum_number = Integer.parseInt(argsMap.get("filterEnumNumber"));

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
                    0, 0, 0, 0, 0, 0, 0, 0, 0, filter_enum_number);

            this.table_name = "";
            for (String name : this.input.getNames()) {
                this.table_name += name;
                this.table_name += "_";
            }
            this.table_name = this.table_name.substring(0, this.table_name.length() - 1); // remove last "_"
            logger.info("#### table_name: {}", this.table_name);

            if (table_name.contains("Property_Features")) {
                removePropertyFeatureCPredicates(this.allPredicates);
            }

            this.prepareAllPredicatesMultiTuples();

            List<Predicate> tmp_allPredicates = new ArrayList<>();
            for (Predicate p : this.allPredicates) {
                tmp_allPredicates.add(p);
            }
            // remove constant predicates of enumeration type for RHS
            removeEnumPredicates(tmp_allPredicates);
            this.applicationRHSs = new ArrayList<>();
            this.applicationRHSs = this.applicationDrivenSelection(tmp_allPredicates);

            // remove predicates that are irrelevant to RHSs
            if (this.maxTupleNum <= 2) {
                filterIrrelevantPredicates(applicationRHSs, this.allPredicates);
            }

            // remove constant predicates of enumeration type for X
            removeEnumPredicates(this.allPredicates, this.allExistPredicates);

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
//        logger.info("allExistPredicates size: {}", this.allExistPredicates.size());
//        for (int i = 0; i < this.allExistPredicates.size(); i++) {
//            logger.info("{} : {}", i, this.allExistPredicates.get(i).toString());
//        }
        return this.allExistPredicates.size();
    }

    public String getAllPredicates() {
        String res = "";
        for (Predicate p : this.allExistPredicates) {
            res += p.toString();
            res += ";";
        }
        return res;
    }

    public String getApplicationRHSs() {
        String rhs_sequence = "";
        for (Predicate p : this.applicationRHSs) {
            rhs_sequence += p.toString();
            rhs_sequence += ";";
        }
        return rhs_sequence;
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

    public void readAllPredicates(String predicates_path) throws IOException {
        this.idx_predicates = new HashMap<>();
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(predicates_path));
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        String line;
        int k = 0;
        while ((line = bReader.readLine()) != null) {
            Predicate p = PredicateBuilder.parsePredicateString(this.input, line);
            this.idx_predicates.put(k, p);
            k++;
        }
    }

    public long[] getSupports() {
        return this.supports;
    }

    public double[] getConfidences() {
        return this.confidences;
    }

    // sequence: "1 3 12,5;2 1,7;..."
    // each rule with one rhs
    public void getSupportConfidence(String sequence) {
        logger.info("### begin to calculate confidence for sequence: {}", sequence);

        ArrayList<WorkUnit> workUnits = new ArrayList<>();
        ArrayList<Boolean> isConstantRHS = new ArrayList<>();
        ArrayList<Predicate> rhss = new ArrayList<>();
        for (String seq_rule : sequence.split(";")) {
            String[] lhs_ids = seq_rule.split(",")[0].split(" ");
            String rhs_id = seq_rule.split(",")[1];

            WorkUnit workunit = new WorkUnit();
//            Predicate rhs = this.allExistPredicates.get(Integer.parseInt(rhs_id));
            Predicate rhs = this.idx_predicates.get(Integer.parseInt(rhs_id)); // read from all Predicates file
            rhss.add(rhs);
            workunit.addRHS(rhs);
            if (rhs.isConstant()) {
                isConstantRHS.add(true);
            } else {
                isConstantRHS.add(false);
            }

            for (String lhs_id : lhs_ids) {
//                workunit.addCurrent(this.allExistPredicates.get(Integer.parseInt(lhs_id)));
                workunit.addCurrent(this.idx_predicates.get(Integer.parseInt(lhs_id))); // read from all Predicates file
            }
            workunit.setTransferData();
            workUnits.add(workunit);
        }

        ArrayList<WorkUnit> workUnits_new = new ArrayList<>();
        this.parallelRuleDiscoverySampling.solveSkewness(workUnits, workUnits_new);

        List<Message> messages = this.parallelRuleDiscoverySampling.runLocal_new(workUnits_new);
        logger.info("messages size: {}", messages.size());
        this.supports = new long[messages.size()];
        this.confidences = new double[messages.size()];
        for (int i = 0; i < messages.size(); i++) {
            long lhs_supp = 0;
            Message message = messages.get(i);
            if (isConstantRHS.get(i)) {
                lhs_supp = message.getCurrentSuppCP0();
            } else {
                lhs_supp = message.getCurrentSupp();
            }
            long rule_supp = message.getAllCurrentRHSsSupport().get(rhss.get(i));
            supports[i] = rule_supp;
            if (lhs_supp == 0) {
                confidences[i] = 0.0;
            } else {
                confidences[i] = rule_supp * 1.0 / lhs_supp;
            }
            logger.info("rule: {} -> {}, lhs_supp: {}, supp: {}, conf: {}", workUnits.get(i).getCurrrent().toString(), rhss.get(i).toString(), lhs_supp, rule_supp, confidences[i]);
        }
    }

    public static void main(String[] args) throws IOException {
        logger.info("Given a rule, calculate its support and confidence");

        String[] args_ = {"directory_path=D:\\REE\\tmp\\airports\\dataset", "constant_file=D:\\REE\\tmp\\constant_airports.txt",
                "chunkLength=200000", "maxTupleNum=2", "filterEnumNumber=5"};

        String predicates_path = "D:\\REE\\tmp\\allPredicates_5Enum_5%\\airports_predicates.txt";

//        String[] args_ = {"directory_path=D:\\REE\\tmp\\property_bak\\property", "constant_file=D:\\REE\\tmp\\property_bak\\constant_property.txt",
//                "chunkLength=200000", "maxTupleNum=2"};

//        String[] args_ = {"directory_path=D:\\REE\\tmp\\aminer_test", "constant_file=D:\\REE\\tmp\\constant_aminer.txt",
//                "chunkLength=200000", "maxTupleNum=2"};

        CalculateRuleSuppConf calculateRuleSuppConf = new CalculateRuleSuppConf();

        calculateRuleSuppConf.preparePredicates(args_);
        calculateRuleSuppConf.getAllPredicatesNum();

        calculateRuleSuppConf.readAllPredicates(predicates_path);

        logger.info("allPredicates: {}", calculateRuleSuppConf.getAllPredicates());
        logger.info("all applicationRHSs: {}", calculateRuleSuppConf.getApplicationRHSs());

//        calculateRuleSuppConf.getSupportConfidence("1 11,5");
//        calculateRuleSuppConf.getSupportConfidence("16 8 19 20,5");
        calculateRuleSuppConf.getSupportConfidence("16 8 25 33,5");
//        calculateRuleSuppConf.getSupportConfidence("5 6 11 12 18 19,5");
//        calculateRuleSuppConf.getSupportConfidence("1 3 7 16,5");

//        calculateRuleSuppConf.getSupportConfidence("78 107,37");
//        calculateRuleSuppConf.getSupportConfidence("9 140,37");


//        calculateRuleSuppConf.getSupportConfidence("16 30,37");

        logger.info("supports: {}, confidences: {}", calculateRuleSuppConf.getSupports(), calculateRuleSuppConf.getConfidences());

    }
}
