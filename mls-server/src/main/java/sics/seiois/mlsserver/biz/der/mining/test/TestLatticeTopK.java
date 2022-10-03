package sics.seiois.mlsserver.biz.der.mining.test;

import de.metanome.algorithm_integration.Operator;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.backend.input.file.FileIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraint;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraintSet;
import sics.seiois.mlsserver.biz.der.metanome.input.Dir;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.mlsel.MLSelection;
import sics.seiois.mlsserver.biz.der.metanome.predicates.ConstantPredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateBuilder;
import sics.seiois.mlsserver.biz.der.mining.ParallelRuleDiscoverySampling;
import sics.seiois.mlsserver.biz.der.mining.model.MLPFilterClassifier;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

public class TestLatticeTopK {


    public static final String NO_CROSS_COLUMN = "NO_CROSS_COLUMN";
    public static final String CROSS_COLUMN_STRING_MIN_OVERLAP = "CROSS_COLUMN_STRING_MIN_OVERLAP";
    public static final String APPROXIMATION_DEGREE = "APPROXIMATION_DEGREE";
    public static final String CHUNK_LENGTH = "CHUNK_LENGTH";
    public static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    public static final String INPUT = "INPUT";
    private static Logger log = LoggerFactory.getLogger(TestLatticeTopK.class);

    public static void main(String args[]) {

        Boolean noCrossColumn = Boolean.TRUE;
        double minimumSharedValue = 0.30d;
        double maximumSharedValue = 0.7d;
        int round = 1;
        String output_file = "D:/REE/tmp/ncvoter/topk/rules/rules_topk_allFiltering.txt"; //""D:/REE/tmp/airports/topk/rules/rules_topk_allFiltering.txt"; //"D:/REE/tmp/inspection/rules/camera_ready/rules_test.csv"; //  //
                //  // "D:/REE/tmp/ncvoter/rules/camera_ready/rules_test.txt"; // "D:/REE/tmp/airports/rules/camera_ready/rules_round" + Integer.toString(round) + ".txt"; //"D:/REE/tmp/inspection/rules/rules_rs_round8.txt"; //"; //"D:/REE/tmp/airports/rules/rules_round2.txt";
        String directory_path = "D:/REE/tmp/ncvoter/topk/ncvoter/"; // "D:/REE/tmp/airports/topk/airports/"; //"D:/REE/tmp/property/samples/property_RW_ROUND" + Integer.toString(round); // "D:/REE/tmp/inspection/inspection/filtered_columns/"; //  //
                //"D:/REE/tmp/property/test/"; //"D:/REE/tmp/ncvoter/datasets/small_test/"; // "D:/REE/tmp/airports/airports_RW_ROUND" + Integer.toString(round) + "/"; // "D:/REE/tmp/inspection/copy/inspection_RS_ROUND8/"; //D:/REE/tmp/airports/airports_RS_ROUND8/"; //"D:/REE/tmp/property/property"; //""D:/REE/tmp/ncvoter/"; // "D:/REE/tmp/airports/"; // "D:/REE/tmp/user_info";
        // String directory_path =  "D:/REE/tmp/property/samples/property_RS_ROUND3/"; //""D:/REE/tmp/ncvoter/"; //  // "D:/REE/tmp/user_info";
        String constant_file = "D:/REE/tmp/ncvoter/ncvoter_constants_predicates.txt"; // "D:/REE/tmp/airports/constant_airports_new.txt"; //"D:/REE/tmp/inspection/constant_inspection.txt"; //; // // // "D:/REE/tmp/ncvoter/ncvoter_constant_predicates.txt"; //"D:/REE/tmp/airports/constant_airports_new.txt"; //"D:/REE/tmp/inspection/constant_inspection.txt"; // "D:/REE/tmp/property/constant_property.txt"; //D:/REE/tmp/ncvoter_constants/ncvoter_constants_predicates.txt"; //";

//        String output_file = args[0];
//        String directory_path = args[1];
//        String constant_file = args[2];

        double rowLimit = 1.0;
        double errorThreshold = 0.9;
        noCrossColumn = true;
        double support_ratio = 0.000001;
        String fk_file = null;
        String mlsel_file = null;
        double relation_num_ratio = 1.0;
        double support_pre_ratio = support_ratio;
        String ml_config_file = null;
        String type_attr_file = null;
        int maxTupleNum = 2;
        int K = 10;
        float w_supp = 0.4f;
        float w_conf = 0.2f;
        float w_diver = 0.2f;
        float w_succ = 0.1f;
        float w_sub = 0.1f;
        int ifPrune = 1;

        int if_conf_filter = 0;
        int if_cluster_workunits = 0;

        int filter_enum_number = 2;

        // whether to use reinforcement learning for predicate association computation
        // top-K rule interestingness
        String predicatesHashIDFile = "D:/REE/tmp/ncvoter/topk/ncvoter_topk/ncvoter_predicates.txt"; //"D:/REE/tmp/airports/topk/airports_topk/airports_predicates.txt";
        String topKOption = "allFiltering"; // { allFiltering, partialFiltering, noFiltering }
        String tokenToIDFile = "D:/REE/tmp/ncvoter/topk/ncvoter_topk/tokenVobs.txt"; //"D:/REE/tmp/airports/topk/airports_topk/tokenVobs.txt";;
        String interestingnessModelFile = "D:/REE/tmp/ncvoter/topk/ncvoter_topk/interestingnessModel.txt"; // "D:/REE/tmp/airports/topk/airports_topk/interestingnessModel.txt";
        String filterRegressionFile = "D:/REE/tmp/ncvoter/topk/ncvoter_topk/filterRegressionModel.txt"; // "D:/REE/tmp/airports/topk/airports_topk/filterRegressionModel.txt";

        Dir directory = new Dir(directory_path, relation_num_ratio);

        ArrayList<FileReader> fileReaders = new ArrayList<FileReader>();

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
            //Input input = new Input(relations, rowLimit);
            Input input = new Input(relations, rowLimit, type_attr_file);
            int maxOneRelationNum = input.getMaxTupleOneRelation();
            int allCount = input.getAllCount();

            //PredicateBuilder predicates = new PredicateBuilder(input, noCrossColumn, minimumSharedValue, maximumSharedValue);
            PredicateBuilder predicates = new PredicateBuilder(input, noCrossColumn, minimumSharedValue, maximumSharedValue, ml_config_file);
            ConstantPredicateBuilder cpredicates = new ConstantPredicateBuilder(input, constant_file);
            log.info("Size of the predicate space: {}, constant size: {}, non-constant size: {}", (predicates.getPredicates().size() + cpredicates.getPredicates().size()),
                    cpredicates.getPredicates().size(), predicates.getPredicates().size());

            // construct PLI index
            // input.buildPLIs_col();
            input.buildPLIs_col_OnSpark(1000000);

            // load ML Selection
            MLSelection mlsel = new MLSelection();
            mlsel.configure(mlsel_file);

            // calculate support
            long rsize = input.getLineCount();
            long support = (long) (rsize * (rsize - 1) * support_ratio);

            log.info("Support is " + support);

            ArrayList<Predicate> allPredicates = new ArrayList<>();
            for (Predicate p : predicates.getPredicates()) {
                if (p.isML() || p.getOperator() == Operator.EQUAL) {
                    allPredicates.add(p);
                }
            }
            log.info("#### non-constant Predicates size: {}", allPredicates.size());

            // set value INT for constant predicates
            HashSet<Predicate> constantPs = new HashSet<>();
            for (Predicate cp : cpredicates.getPredicates()) {
                constantPs.add(cp);
            }
            input.transformConstantPredicates(constantPs);
            log.info("#### constant Predicates size: {}", constantPs.size());

            // add constant predicates
            for (Predicate p : constantPs) {
                allPredicates.add(p);
            }

            long runTime = System.currentTimeMillis();
            ParallelRuleDiscoverySampling parallelRuleDiscoverySampling;
            log.info("filter_enum_number: {}", filter_enum_number);
            parallelRuleDiscoverySampling = new ParallelRuleDiscoverySampling(allPredicates, K, maxTupleNum, support, (float)errorThreshold,
                    maxOneRelationNum, input, allCount, w_supp, w_conf, w_diver, w_succ, w_sub, ifPrune, if_conf_filter, 0.001f, if_cluster_workunits, filter_enum_number,
                    topKOption, tokenToIDFile, interestingnessModelFile, filterRegressionFile, predicatesHashIDFile, false);

            parallelRuleDiscoverySampling.levelwiseRuleDiscoveryLocal();
            // Get top-K rules
//            DenialConstraintSet rees = parallelRuleDiscoverySampling.getTopKREEs();
            ArrayList<DenialConstraint> rees = parallelRuleDiscoverySampling.getTopKREEs_new();

            System.out.printf("Total running time: %s\n", System.currentTimeMillis() - runTime);
            System.out.printf("# of All REEs %s\n", rees.size());
            FileOutputStream outputStream = new FileOutputStream(output_file);
            OutputStreamWriter osw = new OutputStreamWriter(outputStream);
            int c_ree = 1;
            for (DenialConstraint ree : rees) {
                if (ree == null) {
                    continue;
                }
                System.out.printf("REE: %s, supp=%d, conf=%f, score=%f\n", ree.toString(), ree.getSupport(),ree.getConfidence(), ree.getInterestingnessScore());
                String oo = String.format("REE: %s, supp=%d, conf=%f, score=%f\n", ree.toString(), ree.getSupport(),ree.getConfidence(), ree.getInterestingnessScore());
                osw.write(oo);
                c_ree++;
            }
            osw.close();

        } catch (FileNotFoundException | InputIterationException e) {
            log.info("Cannot load file\n");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            for (FileReader fileReader : fileReaders) {
                try {
                    if (fileReader != null) {
                        fileReader.close();
                    }
                } catch (Exception e) {
                    log.error("FileReader close error", e);
                }
            }
        }
    }
}
