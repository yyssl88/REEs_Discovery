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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

public class TestLatticeSampling {


    public static final String NO_CROSS_COLUMN = "NO_CROSS_COLUMN";
    public static final String CROSS_COLUMN_STRING_MIN_OVERLAP = "CROSS_COLUMN_STRING_MIN_OVERLAP";
    public static final String APPROXIMATION_DEGREE = "APPROXIMATION_DEGREE";
    public static final String CHUNK_LENGTH = "CHUNK_LENGTH";
    public static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    public static final String INPUT = "INPUT";
    private static Logger log = LoggerFactory.getLogger(TestLatticeSampling.class);

    public static void main(String args[]) {

        Boolean noCrossColumn = Boolean.TRUE;
        double minimumSharedValue = 0.30d;
        double maximumSharedValue = 0.7d;
        String directory_path =  "D:/REE/tmp/ncvoter/"; // "D:/REE/tmp/airports/"; // "D:/REE/tmp/user_info";
        String constant_file = ""; //""D:/REE/tmp/airports/constant_airports.txt";
        double rowLimit = 1.0;
        double errorThreshold = 0.9;
        noCrossColumn = false;
        double support_ratio = 0.001;
        String fk_file = null;
        String mlsel_file = null;
        double relation_num_ratio = 1.0;
        double support_pre_ratio = support_ratio;
        String ml_config_file = null;
        String type_attr_file = null;
        int maxTupleNum = 3;
        int K = 10000;
        float w_supp = 0.4f;
        float w_conf = 0.2f;
        float w_diver = 0.2f;
        float w_succ = 0.1f;
        float w_sub = 0.1f;
        int ifPrune = 1;

        int if_conf_filter = 1;
        int if_cluster_workunits = 1;

        // whether to use reinforcement learning for predicate association computation
        int ifRL = 0;
        int ifOnlineTrainRL = 0;
        int ifOfflineTrainStage = 1; // for offline RL, if at training stage
//        String PI_path = "/opt/anaconda3/envs/py36/bin/python";
        String PI_path = "C:\\Users\\hanzy\\Anaconda3\\envs\\py36\\python.exe"; // python interpreter path
//        String PI_path = "C:\\Users\\wangys\\Anaconda3\\python.exe"; // ""C:\\Users\\hanzy\\Anaconda3\\envs\\py36\\python.exe"; // python interpreter path
        String RL_code_path = "./PredicateAssociation/";
        int N = 10;
        int DeltaL = 3;
        // parameters for RL model
        float learning_rate = 0.01f;
        float reward_decay = 0.9f;
        float e_greedy = 0.9f;
        int replace_target_iter = 300;
        int memory_size = 500;
        int batch_size = 32;

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
            log.info("Size of the predicate space:" + (predicates.getPredicates().size() + cpredicates.getPredicates().size()));

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

            long runTime = System.currentTimeMillis();
            ParallelRuleDiscoverySampling parallelRuleDiscoverySampling;
            if (ifRL == 0) {
                parallelRuleDiscoverySampling = new ParallelRuleDiscoverySampling(allPredicates, K, maxTupleNum,
                        support, (float) errorThreshold, maxOneRelationNum, input, allCount,
                        w_supp, w_conf, w_diver, w_succ, w_sub, ifPrune, if_conf_filter, 0.001f, if_cluster_workunits);
            } else {
                parallelRuleDiscoverySampling = new ParallelRuleDiscoverySampling(allPredicates, K, maxTupleNum,
                        support, (float)errorThreshold, maxOneRelationNum, input, allCount,
                        w_supp, w_conf, w_diver, w_succ, w_sub, ifPrune, if_conf_filter, 0.001f, if_cluster_workunits,
                        ifRL, ifOnlineTrainRL, ifOfflineTrainStage, PI_path, RL_code_path,N, DeltaL,
                        learning_rate, reward_decay, e_greedy, replace_target_iter, memory_size, batch_size);
            }

            parallelRuleDiscoverySampling.levelwiseRuleDiscoveryLocal();
            // Get top-K rules
            DenialConstraintSet rees = parallelRuleDiscoverySampling.getTopKREEs();

            System.out.printf("Total running time: %s\n", System.currentTimeMillis() - runTime);
            System.out.printf("# of All REEs %s\n", rees.size());
            int c_ree = 1;
            for (DenialConstraint ree : rees) {
                if (ree == null) {
                    continue;
                }
                System.out.println(ree.toString());
                c_ree++;
            }

        } catch (FileNotFoundException | InputIterationException e) {
            log.info("Cannot load file\n");
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
