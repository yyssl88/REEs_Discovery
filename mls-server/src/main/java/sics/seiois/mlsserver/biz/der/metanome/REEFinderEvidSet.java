package sics.seiois.mlsserver.biz.der.metanome;

import com.sics.seiois.client.model.mls.ColumnInfo;
import com.sics.seiois.client.model.mls.JoinInfos;
import com.sics.seiois.client.model.mls.TableInfo;
import com.sics.seiois.client.model.mls.TableInfos;

import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.DenialConstraintResultReceiver;
import de.metanome.backend.input.file.FileIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import sics.seiois.mlsserver.biz.der.metanome.evidenceset.IEvidenceSet;
import sics.seiois.mlsserver.biz.der.metanome.input.Collective;
import sics.seiois.mlsserver.biz.der.metanome.input.Dir;
import sics.seiois.mlsserver.biz.der.metanome.input.Input;
import sics.seiois.mlsserver.biz.der.metanome.predicates.ConstantPredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.PredicateBuilder;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.utils.RuntimeParamUtil;

public class REEFinderEvidSet {

    public static final String NO_CROSS_COLUMN = "NO_CROSS_COLUMN";
    public static final String CROSS_COLUMN_STRING_MIN_OVERLAP = "CROSS_COLUMN_STRING_MIN_OVERLAP";
    public static final String APPROXIMATION_DEGREE = "APPROXIMATION_DEGREE";
    public static final String CHUNK_LENGTH = "CHUNK_LENGTH";
    public static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    public static final String INPUT = "INPUT";
    private static Logger log = LoggerFactory.getLogger(REEFinderEvidSet.class);

    private PredicateBuilder predicates;
    private ConstantPredicateBuilder cpredicates;
    private Input input;
    private Set<String> usefulColumns = new HashSet<>();


    private String algOption;

    public void setAlgOption(String algOption) {
        this.algOption = algOption;
    }

    public PredicateBuilder getPredicateBuilder() {
        return this.predicates;
    }

    public ConstantPredicateBuilder getConstantPredicateBuilder() {
        return this.cpredicates;
    }

    public Input getInput() {
        return this.input;
    }

    public void generateInput(String taskId, TableInfos tableInfos, JoinInfos joinInfos, SparkSession spark, List<String> constantPredicateList,
                              PredicateConfig config, String eidName) {
        Map<String, String> tbl_path = new HashMap<String, String>();

        double relation_num_ratio = 1;
        double rowLimit = 1;

        double minimumSharedValue = 0.30d;
        double maximumSharedValue = 0.7d;
        Boolean noCrossColumn = config.isCrossColumnFlag() ? false : true;


        for (TableInfo tbl : tableInfos.getTableInfoList()) {
            tbl_path.put(tbl.getTableName(), tbl.getTableDataPath());
            for(ColumnInfo columnInfo : tbl.getColumnList()) {
                if (columnInfo.getSkip() == false) {
                    usefulColumns.add(columnInfo.getColumnName());
                }
            }
        }

        Dir directory = new Dir(tbl_path, relation_num_ratio);
        FileSystem hdfs = null;
        try {
            Collection<RelationalInput> relations = new ArrayList<>();
            Iterator<String> iter_rname = directory.iterator_r();
            Iterator<String> iter_path = directory.iterator_a();
            hdfs = FileSystem.get(new Configuration());
            while (iter_rname.hasNext()) {
                String rname = iter_rname.next();
                String rpath = iter_path.next();
                org.apache.hadoop.fs.Path pathD = new Path(rpath);
                FSDataInputStream fsin = hdfs.open(pathD);
                BufferedReader br = new BufferedReader(new InputStreamReader(fsin));
                relations.add(new FileIterator(rname, br,
                        new ConfigurationSettingFileInput(rpath)));
            }
            //这里有个bug，先排个序，不排序，生成的谓词没有关联性
            ((ArrayList)relations).sort(new Comparator<RelationalInput>() {
                @Override
                public int compare(RelationalInput o1, RelationalInput o2) {
                    int rst = o1.relationName().compareTo(o2.relationName());
                    return rst;
                }
            });
            input = new Input(relations, rowLimit, tableInfos);

            // construct PLI index
            input.buildPLIs_col_OnSpark(spark, joinInfos, config);

            input.setTaskId(taskId);


            predicates = new PredicateBuilder(input, noCrossColumn, minimumSharedValue, maximumSharedValue, taskId, eidName, usefulColumns);
            log.info("####level:{}, predicate:{}", predicates.getPredicates(), PredicateSet.indexProvider);
            // set ML lists for ML predicates
            predicates.setMLList();

            ConstantPredicateBuilder.setHighRatio(RuntimeParamUtil.canGet(spark.conf().get("runtimeParam"), "highSelectivityRatio") ? Double.valueOf(RuntimeParamUtil.getRuntimeParam(spark.conf().get("runtimeParam"), "highSelectivityRatio")) : 0);

            //将常数谓词也加载到PredicateBuilder-predicates属性中
            cpredicates = new ConstantPredicateBuilder(input, constantPredicateList);
            log.info("Size of the constant predicate space:" + cpredicates.getPredicates().size());
        } catch (Exception e) {
            log.error("####[生成input]异常", e);
            throw new RuntimeException(e);
        }
    }
}
