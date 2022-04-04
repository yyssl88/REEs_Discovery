package sics.seiois.mlsserver.biz.der.mining.model;

import de.metanome.algorithm_integration.Operator;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.backend.input.file.FileIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.input.Dir;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.predicates.ConstantPredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateBuilder;
import sics.seiois.mlsserver.biz.der.mining.test.TestLatticeSampling;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

public class testMatrix {

    private static Logger log = LoggerFactory.getLogger(TestLatticeSampling.class);

    public static void main(String[] args) {

        // test airports

        Boolean noCrossColumn = Boolean.TRUE;
        double minimumSharedValue = 0.30d;
        double maximumSharedValue = 0.7d;
        int round = 2;
        String directory_path = "D:/REE/tmp/airports/airports/datasets/"; // "D:/REE/tmp/airports/airports_RW_ROUND" + Integer.toString(round) + "/"; // "D:/REE/tmp/inspection/copy/inspection_RS_ROUND8/"; //D:/REE/tmp/airports/airports_RS_ROUND8/"; //"D:/REE/tmp/property/property"; //""D:/REE/tmp/ncvoter/"; // "D:/REE/tmp/airports/"; // "D:/REE/tmp/user_info";
        String constant_file = "D:/REE/tmp/airports/constant_airports_new.txt"; //"D:/REE/tmp/airports/constant_airports_new.txt"; //"D:/REE/tmp/inspection/constant_inspection.txt"; // "D:/REE/tmp/property/constant_property.txt"; //D:/REE/tmp/ncvoter_constants/ncvoter_constants_predicates.txt"; //";
        Dir directory = new Dir(directory_path, 1.0);
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
            String type_file = null;
            Input input = new Input(relations, 1.0, type_file);
            int maxOneRelationNum = input.getMaxTupleOneRelation();
            int allCount = input.getAllCount();

            //PredicateBuilder predicates = new PredicateBuilder(input, noCrossColumn, minimumSharedValue, maximumSharedValue);
            PredicateBuilder predicates = new PredicateBuilder(input, noCrossColumn, minimumSharedValue, maximumSharedValue, null);
            ConstantPredicateBuilder cpredicates = new ConstantPredicateBuilder(input, constant_file);
            log.info("Size of the predicate space:" + (predicates.getPredicates().size() + cpredicates.getPredicates().size()));

            // construct PLI index
            // input.buildPLIs_col();
            input.buildPLIs_col_OnSpark(1000000);
            ArrayList<Predicate> allPredicates = new ArrayList<>();
            for (Predicate p : predicates.getPredicates()) {
                if (p.isML() || p.getOperator() == Operator.EQUAL) {
                    allPredicates.add(p);
                }
            }
            // set value INT for constant predicates
            HashSet<Predicate> constantPs = new HashSet<>();
            for (Predicate cp : cpredicates.getPredicates()) {
                constantPs.add(cp);
            }
            input.transformConstantPredicates(constantPs);
            // add constant predicates
            for (Predicate p : constantPs) {
                allPredicates.add(p);
            }

            // Rule Interestingness Model
            String model_file = "D:/REE/REE_discovery_code_and_plot/REEs_model_data/model.txt";
            String tokens_file = "D:/REE/REE_discovery_code_and_plot/REEs_model_data/tokenVobs.txt";
            InterestingnessModel interestingness = new InterestingnessModel(tokens_file, model_file);

            // generate one testing data
            ArrayList<Predicate> reeLHS = new ArrayList<>();
            Predicate rhs = null;
            reeLHS.add(allPredicates.get(1));
            reeLHS.add(allPredicates.get(10));
            rhs = allPredicates.get(5);

            // test Rule Interestingness model
            double[][] reeObj = new double[1][3];
            for (int i = 0; i < 3; i++) {
                reeObj[0][i] = 0.0;
            }
            double interestingnessScore = interestingness.run(reeLHS, rhs, reeObj);
            System.out.println(interestingnessScore);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InputIterationException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

//
//        /*
//            test DQN Classifier
//         */
//        String model_file = "D:/REE/REE_discovery_code_and_plot/REEs_model_data/model.txt";
//        MLPFilterClassifier mlpFilterClassifier = new MLPFilterClassifier(model_file);
//
//        int feature_num = mlpFilterClassifier.getFeature_num();
//        int action_num = mlpFilterClassifier.getAction_num();
//
//        double[][] features = new double[1][feature_num];
//        features[0][10] = 1.0;
//        features[0][5] =  1.0;
//
//        // double[][] qvalues = null;
//
//        long startTime = System.currentTimeMillis();
//        for (int i = 0; i <1000000; i++) {
//            boolean res = mlpFilterClassifier.run(features);
//            System.out.println(res);
//        }

    }
}
