package sics.seiois.mlsserver.biz.der.mining.test;

import de.metanome.algorithm_integration.Operator;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.backend.input.file.FileIterator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.REEFinderEvidSet;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraint;
import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraintSet;
import sics.seiois.mlsserver.biz.der.metanome.input.Dir;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.input.InputLight;
import sics.seiois.mlsserver.biz.der.metanome.mlsel.MLSelection;
import sics.seiois.mlsserver.biz.der.metanome.predicates.ConstantPredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.recovery.ConstantRecovery;
import sics.seiois.mlsserver.model.PredicateConfig;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;


public class TestConstantRecovery {


    public static final String NO_CROSS_COLUMN = "NO_CROSS_COLUMN";
    public static final String CROSS_COLUMN_STRING_MIN_OVERLAP = "CROSS_COLUMN_STRING_MIN_OVERLAP";
    public static final String APPROXIMATION_DEGREE = "APPROXIMATION_DEGREE";
    public static final String CHUNK_LENGTH = "CHUNK_LENGTH";
    public static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    public static final String INPUT = "INPUT";
    private static Logger log = LoggerFactory.getLogger(TestConstantRecovery.class);

    private static DenialConstraintSet loadREEs(String rees_path, Input input) throws Exception {
        DenialConstraintSet rees = new DenialConstraintSet();
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(rees_path));
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        String line;
        String valid_prefix = "Rule : ";
        while ((line = bReader.readLine()) != null) {
            if (line.length() <= valid_prefix.length() || (!line.startsWith(valid_prefix))) {
                continue;
            } else {
                DenialConstraint ree = parseOneREE(line.trim(), input);
                if (ree != null) {
                    rees.add(ree);
                }
            }
        }
        return rees;
    }

    public static DenialConstraint parseOneREE(String rule, Input input) {
        String[] pstrings = rule.split(",")[0].split("->");
        String[] xStrings = pstrings[0].split("\\^");
        String[] xString = xStrings[xStrings.length - 1].trim().split("  ");
        PredicateSet ps = new PredicateSet();
        for (int i = 0; i < xString.length; i++) {
            ps.add(PredicateBuilder.parsePredicateString(input, xString[i].trim()));
        }
        String rhs = pstrings[pstrings.length - 1].trim();
        ps.addRHS(PredicateBuilder.parsePredicateString(input, rhs.substring(0, rhs.length() - 1)));
        DenialConstraint ree = new DenialConstraint(ps);
        return ree;
    }

    public static void main(String args[]) {

        Boolean noCrossColumn = Boolean.TRUE;
        double minimumSharedValue = 0.30d;
        double maximumSharedValue = 0.7d;
        String directory_path = "D:\\REE\\tmp\\airports";
        String constant_file = "D:\\REE\\tmp\\constant_airports.txt";
        String ree_sample_file = "D:\\REE\\tmp\\outputResult_airports__RW__SRatio0.1__ROUND1_PREEMiner_NORM_vary_sr_Figa__supp0.0001_conf0.9_N100_DeltaL3_tnum2_topK10000_processor20.txt";
        double rowLimit = 1.0;
        double errorThreshold = 0.9;
        noCrossColumn = false;
        double support_ratio = 0.00001;
        String fk_file = null;
        String mlsel_file = null;
        double relation_num_ratio = 1.0;
        double support_pre_ratio = support_ratio;
        String ml_config_file = null;
        String type_attr_file = null;
        int maxTupleNum = 2;
        int K = 10000;
        float w_supp = 0.4f;
        float w_conf = 0.2f;
        float w_diver = 0.2f;
        float w_succ = 0.1f;
        float w_sub = 0.1f;
        int ifPrune = 1;

        // whether to use reinforcement learning for predicate association computation
        int ifRL = 0;
//        String PI_path = "/opt/anaconda3/envs/py36/bin/python";
//        String PI_path = "C:\\Users\\hanzy\\Anaconda3\\envs\\py36\\python.exe"; // python interpreter path
        String PI_path = "C:\\Users\\wangys\\Anaconda3\\python.exe"; // ""C:\\Users\\hanzy\\Anaconda3\\envs\\py36\\python.exe"; // python interpreter path
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

            // load rees of samples from file
            long runTime = System.currentTimeMillis();

            DenialConstraintSet reesStart = loadREEs(ree_sample_file, input);

            ConstantRecovery constantRecovery = new ConstantRecovery(reesStart, allPredicates, maxTupleNum, new InputLight(input),
                    support, (float)errorThreshold, maxOneRelationNum, allCount);
            constantRecovery.recoveryLocal();
            // Get rules
            DenialConstraintSet rees = constantRecovery.getREEsResults();

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
        } catch (Exception e) {
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
