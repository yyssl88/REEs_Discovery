package sics.seiois.mlsserver.biz.der.metanome.input;

import com.sics.seiois.client.model.mls.*;

import com.sics.seiois.common.utils.StringUtils;
import de.metanome.algorithm_integration.Operator;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import sics.seiois.mlsserver.biz.der.metanome.helpers.IndexProvider;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLI;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLISection;
import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.TupleIDProvider;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.utils.MlsConstant;

public class Input implements Serializable {

    private static final long serialVersionUID = 5961087336068687061L;
    private static final Logger log = LoggerFactory.getLogger(Input.class);
    private static String JOIN_DELIMITER = MlsConstant.RD_JOINTABLE_SPLIT_STR;//"_AND_";
    private static String TBL_COL_DELIMITER = "__";
    private int lineCount;
    private int[] lineCounts;
    private int[] colCounts;
    private int[] lineCounts_col;

    public static final String SAMPLE_RELATION_TID = "___";

    private List<ParsedColumn<?>> parsedColumns;

    private Map<String, PLI> storedPli = new HashMap<>();//把parsedcolumn的pli都存储到这里
    private List<List<PLISection>> storedPlis = new ArrayList<>();//把pli都存储到这里
    private int interval = 10000;
    private List<Integer> pliSizes = new ArrayList<>();

    private String name;
    private List<String> names;
    private List<NewTBLInfo> newTblList = new ArrayList<>();
    private List<Map<Integer, String>> tids_rowids = new ArrayList<>();

    private Map<Integer, List<Integer>> newMapping;//老的tid，新的tid列表
    private String taskId;

    // record the types of each columns
    public static String relationAttrDelimiter = "->";
    public HashMap<String, String> typeMap;


    IndexProvider<String> providerS = new IndexProvider<>();
    IndexProvider<Long> providerL = new IndexProvider<>();
    IndexProvider<Double> providerD = new IndexProvider<>();

    public IndexProvider<String> getProviderS() {
        return this.providerS;
    }

    public IndexProvider<Long> getProviderL() {
        return this.providerL;
    }

    public IndexProvider<Double> getProviderD() {
        return this.providerD;
    }


    // get the maximum number of tuples in one relation
    public int getMaxTupleOneRelation() {
        int maxV = 0;
        for (int i = 0; i < this.lineCounts.length; i++) {
            maxV = Math.max(maxV, this.lineCounts[i]);
        }
        return maxV;
    }

    // get the total number of tuples among all relations
    public int getAllCount() {
        return this.lineCount;
    }


    public Input() throws InputIterationException {
    }

    public Input cloneInput(Input input) throws InputIterationException {

        Input cleanOne = new Input();
        cleanOne.lineCount = input.getLineCount();
        cleanOne.lineCounts = input.getLineCounts();
        cleanOne.colCounts = input.getColCounts();
        cleanOne.lineCounts_col = input.getLineCounts_col();
        cleanOne.name = input.getName();
        cleanOne.names = input.getNames();
        return cleanOne;

    }

    public Input(RelationalInput relationalInput, int rowLimit) throws InputIterationException {
        final int columnCount = relationalInput.numberOfColumns();
        Column[] columns = new Column[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            columns[i] = new Column(relationalInput.relationName(), relationalInput.columnNames().get(i));
        }

        int lineCount = 0;
        while (relationalInput.hasNext()) {
            List<String> line = relationalInput.next();
            for (int i = 0; i < columnCount; ++i) {
                columns[i].addLine(line.get(i));
            }
            ++lineCount;
            if (rowLimit > 0 && lineCount >= rowLimit) {
                break;
            }
        }
        this.lineCount = lineCount;

        parsedColumns = new ArrayList<>(columns.length);
        createParsedColumns(relationalInput, columns);

        name = relationalInput.relationName();
    }

    public Input(Collection<RelationalInput> relationalInputs, double rowLimit) throws InputIterationException {
        loadData(relationalInputs, rowLimit);
    }

    public Input(Collection<RelationalInput> relationalInputs, double rowLimit, Map<String, String> columnTypeMap) throws InputIterationException {
        // load type file
        this.typeMap = null == columnTypeMap ? new HashMap<String, String>() : (HashMap<String, String>) columnTypeMap;
        loadData(relationalInputs, rowLimit);
    }


    public Input(Collection<RelationalInput> relationalInputs, double rowLimit, String type_file) throws InputIterationException {
        // load type file
        this.loadTypeFile(type_file);
        loadData(relationalInputs, rowLimit);
    }


    public Input(Collection<RelationalInput> relationalInputs, double rowLimit, TableInfos tableInfos) throws InputIterationException {
        // load type file
        this.loadTypeFile(tableInfos);
        loadData(relationalInputs, rowLimit);
    }

    private void loadData(Collection<RelationalInput> relationalInputs, double rowLimit) throws InputIterationException {
        int columnCount = 0; //relationalInput.numberOfColumns();
        for (RelationalInput relationalInput : relationalInputs)
            columnCount += relationalInput.numberOfColumns();

        Column[] columns = new Column[columnCount];


        int cid = 0;
        for (RelationalInput relationalInput : relationalInputs) {
            for (int i = 0; i < relationalInput.numberOfColumns(); ++i) {
                columns[cid] = new Column(relationalInput.relationName(), relationalInput.columnNames().get(i));
                String _key_ = relationalInput.relationName() + this.relationAttrDelimiter + relationalInput.columnNames().get(i);
                if (this.typeMap != null && this.typeMap.containsKey(_key_)) {
                    columns[cid].setType(this.typeMap.get(_key_));
                }
                cid++;
            }
        }

        int rid = 0;
        int scc = 0;
        int lineCount = 0;
        lineCounts = new int[relationalInputs.size()];
        colCounts = new int[relationalInputs.size()];
        lineCounts_col = new int[columns.length];
        names = new ArrayList<>(relationalInputs.size());
        name = "";
        int cc = 0;

        long heapFreeSize = Runtime.getRuntime().freeMemory();
        log.debug("MMM1:before load input,heap free size {} MB", heapFreeSize / 1024 / 1024);

        for (RelationalInput relationalInput : relationalInputs) {
            lineCount = 0;
            /**
             while (relationalInput.hasNext()) {
             List<String> line = relationalInput.next();
             for (int i = 0; i < relationalInput.numberOfColumns(); ++i) {
             columns[scc + i].addLine(line.get(i));
             }
             ++lineCount;
             if (rowLimit > 0 && lineCount >= rowLimit)
             break;
             }*/

            int printSize = 1000000;
            List<List<String>> lines = new ArrayList<>();
            while (relationalInput.hasNext()) {
                lines.add(relationalInput.next());

                ++lineCount;
                if (lineCount > printSize) {
                    log.info(">>>>load data is {} line", lineCount);
                    printSize += 1000000;
                }
            }

            log.info(">>>> read line size : {}", lineCount);
            int rowLimit_ = (int) (rowLimit * lineCount);
            if (rowLimit_ == 0) rowLimit_ = 1;
            Iterator<List<String>> iter_r = lines.iterator();
            int count_row = 0;
            while (iter_r.hasNext()) {
                if (count_row >= rowLimit_)
                    break;
                List<String> line = iter_r.next();
                for (int i = 0; i < relationalInput.numberOfColumns(); ++i) {
                    columns[scc + i].addLine(line.get(i));
                }
                count_row++;
            }
            lineCount = count_row;
            log.info(">>>> read row size : {}", count_row);
            lineCounts[rid] = lineCount;
            for (int j = 0; j < relationalInput.numberOfColumns(); j++) {
                lineCounts_col[cc++] = lineCount;
            }
            names.add(relationalInput.relationName());
            colCounts[rid] = relationalInput.numberOfColumns();
            name += relationalInput.relationName();
            rid++;
            scc += relationalInput.numberOfColumns();
        }
        this.lineCount = 0; //lineCount;
        for (int i = 0; i < lineCounts.length; i++) this.lineCount += lineCounts[i];

        parsedColumns = new ArrayList<>(columns.length);
        createParsedColumnsCollective(columns);
        log.info(">>>> finish create input");
        //name = relationalInput.relationName();
    }

    // load the types of each columns
    public void loadTypeFile(String type_file) {

        this.typeMap = new HashMap<>();

        if (type_file == null) {
            return;
        }

        FileReader fr = null;
        BufferedReader br = null;
        try {
            File file = new File(type_file);
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String line;

            while ((line = br.readLine()) != null) {
                String relation_name = line.split(";")[0];
                String attr_name = line.split(";")[1];
                String type = line.split(";")[2];

                String _k_ = relation_name + this.relationAttrDelimiter + attr_name;
                if (!this.typeMap.containsKey(_k_)) {
                    this.typeMap.put(_k_, type);
                }
            }

        } catch (FileNotFoundException e) {
            log.error("can not find type file", e);
        } catch (IOException e) {
            log.error("IOException error", e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception e) {
                log.error("BufferedReader close error", e);
            }

            try {
                if (fr != null) {
                    fr.close();
                }
            } catch (Exception e) {
                log.error("FileReader close error", e);
            }
        }
    }

    // load the types of each columns
    public void loadTypeFile(TableInfos tableInfos) {
        this.typeMap = new HashMap<String, String>();
        for (TableInfo tableInfo : tableInfos.getTableInfoList()) {
            String relation_name = tableInfo.getTableName();
            List<ColumnInfo> cols = tableInfo.getColumnList();
            for (ColumnInfo col : cols) {
                String _k_ = relation_name + this.relationAttrDelimiter + col.getColumnName();
                if (!this.typeMap.containsKey(_k_)) {
                    this.typeMap.put(_k_, col.getColumnType());
                }
            }
        }
    }

    public int getPliSize(int rid) {
        return pliSizes.get(rid);

    }

    private void createParsedColumns(RelationalInput relationalInput, Column[] columns) {
        int i = 0;
        for (Column c : columns) {
            switch (c.getType()) {
                case LONG: {
                    ParsedColumn<Long> parsedColumn = new ParsedColumn<Long>(relationalInput.relationName(), c.getName(),
                            Long.class, i);

                    for (int l = 0; l < lineCount; ++l) {
                        parsedColumn.addLine(c.getLong(l));
                    }
                    parsedColumns.add(parsedColumn);
                }
                break;
                case NUMERIC: {
                    ParsedColumn<Double> parsedColumn = new ParsedColumn<Double>(relationalInput.relationName(),
                            c.getName(), Double.class, i);

                    for (int l = 0; l < lineCount; ++l) {
                        parsedColumn.addLine(c.getDouble(l));
                    }
                    parsedColumns.add(parsedColumn);
                }
                break;
                case STRING: {
                    ParsedColumn<String> parsedColumn = new ParsedColumn<String>(relationalInput.relationName(),
                            c.getName(), String.class, i);

                    for (int l = 0; l < lineCount; ++l) {
                        parsedColumn.addLine(c.getString(l));
                    }
                    parsedColumns.add(parsedColumn);
                }
                break;
                default:
                    break;
            }

            ++i;
        }
    }

    private void createParsedColumnsCollective(Column[] columns) {
        int i = 0;
        for (Column c : columns) {
            switch (c.getType()) {
                case LONG: {
                    ParsedColumn<Long> parsedColumn = new ParsedColumn<Long>(c.getTableName(), c.getName(),
                            Long.class, i);

                    for (int l = 0; l < lineCounts_col[i]; ++l) {
                        parsedColumn.addLine(c.getLong(l));
                    }
                    parsedColumns.add(parsedColumn);
                }
                break;
                case NUMERIC: {
                    ParsedColumn<Double> parsedColumn = new ParsedColumn<Double>(c.getTableName(),
                            c.getName(), Double.class, i);

                    for (int l = 0; l < lineCounts_col[i]; ++l) {
                        parsedColumn.addLine(c.getDouble(l));
                    }
                    parsedColumns.add(parsedColumn);
                }
                break;
                case STRING: {
                    ParsedColumn<String> parsedColumn = new ParsedColumn<String>(c.getTableName(),
                            c.getName(), String.class, i);

                    for (int l = 0; l < lineCounts_col[i]; ++l) {
                        // intern 字符串减少内存
                        parsedColumn.addLine(c.getString(l).intern());
                    }
                    parsedColumns.add(parsedColumn);
                }
                break;
                default:
                    break;
            }

            ++i;
        }

        // 减少内存
        for (ParsedColumn<?> parsedColumn : parsedColumns) {
            ((ArrayList<?>) parsedColumn.getValues()).trimToSize();
            parsedColumn.getValuesInt().trimToSize();
        }
    }

    public int getLineCount() {
        return lineCount;
    }

    public int getLineCount(int rid) {
        return lineCounts[rid];
    }

    public int[] getLineCounts_col() {
        return this.lineCounts_col;
    }

    public ParsedColumn<?>[] getColumns() {
        return parsedColumns.toArray(new ParsedColumn[0]);
    }

    public ParsedColumn<?> getParsedColumn(String relation_name, String name) {
        for (ParsedColumn<?> pc : this.parsedColumns) {
            if (pc.getTableName().equals(relation_name) && pc.getName().equals(name)) {
                return pc;
            }
        }

        if (name.contains(",")) {
            return new ParsedColumn<>(relation_name, name, String.class, 0);
        }
        return null;
    }

    public int getColumnNum() {
        return parsedColumns.size();
    }

    public String getName() {
        return name;
    }

    public String getName(int rid) {
        return names.get(rid);
    }

    public Input(RelationalInput relationalInput) throws InputIterationException {
        this(relationalInput, -1);
    }

    public int[][] getInts() {
        final int COLUMN_COUNT = parsedColumns.size();
        final int ROW_COUNT = getLineCount();

        int[][] input2s = new int[ROW_COUNT][COLUMN_COUNT];
        IndexProvider<String> providerS = new IndexProvider<>();
        IndexProvider<Long> providerL = new IndexProvider<>();
        IndexProvider<Double> providerD = new IndexProvider<>();
        for (int col = 0; col < COLUMN_COUNT; ++col) {

            if (parsedColumns.get(col).getType() == String.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][col] = providerS.getIndex((String) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else if (parsedColumns.get(col).getType() == Double.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][col] = providerD.getIndex((Double) parsedColumns.get(col).getValue(line)).intValue();

                }
            } else if (parsedColumns.get(col).getType() == Long.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][col] = providerL.getIndex((Long) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else {
                log.error("Wrong type! " + parsedColumns.get(col).getValue(0).getClass().getName());
            }
        }
        providerS = IndexProvider.getSorted(providerS);
        providerL = IndexProvider.getSorted(providerL);
        providerD = IndexProvider.getSorted(providerD);
        for (int col = 0; col < COLUMN_COUNT; ++col) {
            if (parsedColumns.get(col).getType() == String.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][col] = providerS.getIndex((String) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else if (parsedColumns.get(col).getType() == Double.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][col] = providerD.getIndex((Double) parsedColumns.get(col).getValue(line)).intValue();

                }
            } else if (parsedColumns.get(col).getType() == Long.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][col] = providerL.getIndex((Long) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else {
                log.error("Wrong type!");
            }
        }

        return input2s;
    }

    public void buildPLIs() {

        long time = System.currentTimeMillis();

        final int COLUMN_COUNT = parsedColumns.size();
        final int ROW_COUNT = getLineCount();

        List<Integer> tIDs = new TupleIDProvider(ROW_COUNT).gettIDs(); // to save integers storage

        int[][] inputs = getInts();

        for (int col = 0; col < COLUMN_COUNT; ++col) {

            TIntSet distincts = new TIntHashSet();
            for (int line = 0; line < ROW_COUNT; ++line) {
                distincts.add(inputs[line][col]);
            }

            int[] distinctsArray = distincts.toArray();

            // need to sort for doubles and integers
            if (!(parsedColumns.get(col).getType() == String.class)) {
                Arrays.sort(distinctsArray);// ascending is the default
            }

            TIntIntMap translator = new TIntIntHashMap();
            for (int position = 0; position < distinctsArray.length; position++) {
                translator.put(distinctsArray[position], position);
            }

            List<Set<Integer>> setPlis = new ArrayList<>();
            for (int i = 0; i < distinctsArray.length; i++) {
                setPlis.add(new TreeSet<Integer>());
            }

            for (int line = 0; line < ROW_COUNT; ++line) {
                Integer tid = tIDs.get(line);
                setPlis.get(translator.get(inputs[line][col])).add(tid);
            }

            int values[] = new int[ROW_COUNT];
            for (int line = 0; line < ROW_COUNT; ++line) {
                values[line] = inputs[line][col];
            }

            PLI pli;

            if (!(parsedColumns.get(col).getType() == String.class)) {
                pli = new PLI(setPlis, ROW_COUNT, true, values);
            } else {
                pli = new PLI(setPlis, ROW_COUNT, false, values);

            }

            parsedColumns.get(col).setPLI(pli);

        }

        log.info("Time to build plis: " + (System.currentTimeMillis() - time));

    }

    public int[][] getInts_col_s(int col_start, int rid) {
        final int COLUMN_COUNT = colCounts[rid]; //parsedColumns.size();
        final int ROW_COUNT = lineCounts[rid];

        int[][] input2s = new int[ROW_COUNT][COLUMN_COUNT];
        IndexProvider<String> providerS = new IndexProvider<>();
        IndexProvider<Long> providerL = new IndexProvider<>();
        IndexProvider<Double> providerD = new IndexProvider<>();
        //for (int col = col_start; col < COLUMN_COUNT; ++col) {
        for (int ii = 0; ii < COLUMN_COUNT; ++ii) {
            int col = col_start + ii;
            if (parsedColumns.get(col).getType() == String.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerS.getIndex((String) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else if (parsedColumns.get(col).getType() == Double.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerD.getIndex((Double) parsedColumns.get(col).getValue(line)).intValue();

                }
            } else if (parsedColumns.get(col).getType() == Long.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerL.getIndex((Long) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else {
                log.error("Wrong type! " + parsedColumns.get(col).getValue(0).getClass().getName());
            }
        }
        providerS = IndexProvider.getSorted(providerS);
        providerL = IndexProvider.getSorted(providerL);
        providerD = IndexProvider.getSorted(providerD);
        //for (int col = 0; col < COLUMN_COUNT; ++col) {
        for (int ii = 0; ii < COLUMN_COUNT; ++ii) {
            int col = col_start + ii;
            if (parsedColumns.get(col).getType() == String.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerS.getIndex((String) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else if (parsedColumns.get(col).getType() == Double.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerD.getIndex((Double) parsedColumns.get(col).getValue(line)).intValue();

                }
            } else if (parsedColumns.get(col).getType() == Long.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerL.getIndex((Long) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else {
                log.error("Wrong type!");
            }
        }

        return input2s;
    }


    public void getInts_col_s_part1_fix(int col_start, int rid) {
        final int COLUMN_COUNT = colCounts[rid]; //parsedColumns.size();
        final int ROW_COUNT = lineCounts[rid];

        int[][] input2s = new int[ROW_COUNT][COLUMN_COUNT];
        //IndexProvider<String> providerS = new IndexProvider<>();
        //IndexProvider<Long> providerL = new IndexProvider<>();
        //IndexProvider<Double> providerD = new IndexProvider<>();
        //for (int col = col_start; col < COLUMN_COUNT; ++col) {
        for (int ii = 0; ii < COLUMN_COUNT; ++ii) {
            int col = col_start + ii;
            if (parsedColumns.get(col).getType() == String.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerS.getIndex((String) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else if (parsedColumns.get(col).getType() == Double.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerD.getIndex((Double) parsedColumns.get(col).getValue(line)).intValue();

                }
            } else if (parsedColumns.get(col).getType() == Long.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerL.getIndex((Long) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else {
                log.error("Wrong type! " + parsedColumns.get(col).getValue(0).getClass().getName());
            }
        }
    }


    public int[][] getInts_col_s_part1(int col_start, int rid) {
        final int COLUMN_COUNT = colCounts[rid]; //parsedColumns.size();
        final int ROW_COUNT = lineCounts[rid];

        int[][] input2s = new int[ROW_COUNT][COLUMN_COUNT];
        //IndexProvider<String> providerS = new IndexProvider<>();
        //IndexProvider<Long> providerL = new IndexProvider<>();
        //IndexProvider<Double> providerD = new IndexProvider<>();
        //for (int col = col_start; col < COLUMN_COUNT; ++col) {
        for (int ii = 0; ii < COLUMN_COUNT; ++ii) {
            int col = col_start + ii;
            if (parsedColumns.get(col).getType() == String.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerS.getIndex((String) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else if (parsedColumns.get(col).getType() == Double.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerD.getIndex((Double) parsedColumns.get(col).getValue(line)).intValue();

                }
            } else if (parsedColumns.get(col).getType() == Long.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    input2s[line][ii] = providerL.getIndex((Long) parsedColumns.get(col).getValue(line)).intValue();
                }
            } else {
                log.error("Wrong type! " + parsedColumns.get(col).getValue(0).getClass().getName());
            }
        }
        return input2s;
    }

    public int[][] getInts_col_s_part2(int col_start, int rid, int[][] input2s) {
        final int COLUMN_COUNT = colCounts[rid]; //parsedColumns.size();
        final int ROW_COUNT = lineCounts[rid];

        //for (int col = 0; col < COLUMN_COUNT; ++col) {
        for (int ii = 0; ii < COLUMN_COUNT; ++ii) {
            int col = col_start + ii;
            if (parsedColumns.get(col).getType() == String.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    String val = (String) parsedColumns.get(col).getValue(line);
                    if (StringUtils.isBlank(val)) {
                        input2s[line][ii] = -1;
                        continue;
                    }
                    input2s[line][ii] = providerS.getIndex(val).intValue();
                }
            } else if (parsedColumns.get(col).getType() == Double.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    Double val = (Double) parsedColumns.get(col).getValue(line);
                    if (val == null) {
                        input2s[line][ii] = -1;
                        continue;
                    }
                    input2s[line][ii] = providerD.getIndex(val).intValue();
                }
            } else if (parsedColumns.get(col).getType() == Long.class) {
                for (int line = 0; line < ROW_COUNT; ++line) {
                    Long val = (Long) parsedColumns.get(col).getValue(line);
                    if (val == null) {
                        input2s[line][ii] = -1;
                        continue;
                    }
                    input2s[line][ii] = providerL.getIndex(val).intValue();
                }
            } else {
                log.error("Wrong type!");
            }
        }

        return input2s;
    }

    public void buildPLIs_col_bySection(int col_start, int tuple_id_start, int rid, TableData tableData) {
        long time = System.currentTimeMillis();

        final int COLUMN_COUNT = colCounts[rid]; // parsedColumns.size();
        final int ROW_COUNT = lineCounts[rid];

        List<Integer> tIDs = new TupleIDProvider(tuple_id_start, ROW_COUNT).gettIDs(); // to save integers storage

        //int[][] inputs = getInts_col_s(col_start, rid);
        int[][] inputs = new int[ROW_COUNT][COLUMN_COUNT];
        inputs = getInts_col_s_part2(col_start, rid, inputs);
        //获取各表的tid字段和关联字段的值，这些值都是内部字典的索引数据
        if (tableData != null) {
            getTableData(col_start, tableData, COLUMN_COUNT, ROW_COUNT, tIDs, inputs);
        }
//        } else {//就没有关联表，全部是单表，或者部分表跟别的表没关联关系
        NewTBLInfo tblInfo = new NewTBLInfo();
        tblInfo.setTblName(names.get(rid));
        tblInfo.setTidStart(tuple_id_start);
        tblInfo.setTupleNum(ROW_COUNT);
        newTblList.add(tblInfo);


        Integer pliSize = 0;
        Map<Integer, String> tidToRowid = new HashMap<>();
        //for (int col = 0; col < COLUMN_COUNT; ++col) {
        for (int ii = 0; ii < COLUMN_COUNT; ii++) {
            List<PLISection> plis = storedPlis.get(rid);
            int col = col_start + ii;
            ParsedColumn<?> ps = parsedColumns.get(col);

            Class type = ps.getType();
            String key = ps.getTableName() + PredicateConfig.TBL_COL_DELIMITER + ps.getName();

            TIntSet distincts = new TIntHashSet();

            Integer values[] = new Integer[ROW_COUNT];
            // add tuple ID: value mapping
            for (int line = 0; line < ROW_COUNT; ++line) {
                distincts.add(inputs[line][ii]);
                values[line] = inputs[line][ii];
            }

            int[] distinctsArray = distincts.toArray();
            // need to sort for doubles and integers
            if (!(type == String.class)) {
                Arrays.sort(distinctsArray);// ascending is the default
            }

            TIntIntMap translator = new TIntIntHashMap();
            for (int position = 0; position < distinctsArray.length; position++) {
                translator.put(distinctsArray[position], distinctsArray.length - position - 1);
            }

            int dataIndex = 0;
            int pliIndex = 0;

            if ("row_id".equals(ps.getName())) {
                for (int line = dataIndex; line < ROW_COUNT; line++) {
                    tidToRowid.put(tIDs.get(line) - tuple_id_start, ps.getValue(line).toString());
                }
            }

//            log.info(">>>> show row count : {} and interval : {} ", ROW_COUNT, interval);
            while (dataIndex < ROW_COUNT) {
                Map<Integer, Set<Integer>> setPlis = new HashMap<>();
                Map<Integer, Integer> tuples = new HashMap<>();

                int end = Math.min(dataIndex + interval, ROW_COUNT);
                for (int line = dataIndex; line < end; line++) {
                    Integer tid = tIDs.get(line) - tuple_id_start;

                    if (!setPlis.containsKey(translator.get(inputs[line][ii]))) {
                        setPlis.put(translator.get(inputs[line][ii]), new TreeSet<>());
                    }
                    setPlis.get(translator.get(inputs[line][ii])).add(tid);
                    tuples.put(tid, inputs[line][ii]);
                }

                PLI pli;
                if (type == String.class) {
                    pli = new PLI(setPlis, ROW_COUNT, false, values);
                } else {
                    pli = new PLI(setPlis, ROW_COUNT, true, values);
                }

                if (plis.size() <= pliIndex) {
                    PLISection section = new PLISection(pliIndex);
                    plis.add(section);
                }
                plis.get(pliIndex).putPLI(key, pli);
                plis.get(pliIndex).putTupleValues(key, tuples);

                // set tidstart and tidend, added by Yaoshu
                plis.get(pliIndex).setTidStart(dataIndex);
                plis.get(pliIndex).setTidEnd(end);

//                log.info(">>>> show section size : {} and pli Index : {} and end size : {}", storedPlis.size(), pliIndex, end);
                pliIndex++;
                dataIndex = end;
            }

            pliSize = Math.max(pliIndex, pliSize);

            ps.setPliSections(plis);
//            ps.cleanBeforeBoradCast();
        }

        tids_rowids.add(tidToRowid);
        pliSizes.add(pliSize);
        log.info("Time to build plis: " + (System.currentTimeMillis() - time));
    }

    private void getTableData(int col_start, TableData tableData, int COLUMN_COUNT, int ROW_COUNT, List<Integer> tIDs, int[][] inputs) {
        for (String fk : tableData.getFkList()) {
            for (int ii = 0; ii < COLUMN_COUNT; ++ii) {
                int col = col_start + ii;
                if (parsedColumns.get(col).getName().equals(fk)) {//把位置信息记录下来
                    tableData.getColLocation().add(col);
                }
            }
        }

        //添加数据
        List<Row> rows = new ArrayList<>();
        for (int line = 0; line < ROW_COUNT; ++line) {
            List<Integer> valueList = new ArrayList<>();
            valueList.add(tIDs.get(line));
            for (int col = 0; col < tableData.getColLocation().size(); ++col) {
                valueList.add(inputs[line][tableData.getColLocation().get(col) - col_start]);
            }
            Row row1 = RowFactory.create(valueList.toArray());
            rows.add(row1);
        }
        tableData.setRows(rows);
        log.debug("### raw data:{}", tableData.toString());
    }

    /**
     * Collectively construct PLI indexes
     **/
    public void buildPLIs_col_OnSpark(SparkSession spark, JoinInfos joinInfos, PredicateConfig config) {
        // construct PLIs for all tables
        int col_start = 0;
        int tuple_id_start = 0;
        interval = config.getChunkLength();

        // record all values
        for (int rid = 0; rid < colCounts.length; rid++) {
            getInts_col_s_part1_fix(col_start, rid);
            col_start += colCounts[rid];
            tuple_id_start += lineCounts[rid];
        }

        col_start = 0;
        tuple_id_start = 0;
        List<TableData> tblList = new ArrayList<>();
        //处理多表关联的情况,生成单表的schema
        // createTableDataMeta(joinInfos, tblList);

        providerS = IndexProvider.getSorted(providerS);
        providerL = IndexProvider.getSorted(providerL);
        providerD = IndexProvider.getSorted(providerD);

        for (int rid = 0; rid < colCounts.length; rid++) {
            TableData tblData = getTbl(rid, tblList);
            storedPlis.add(new ArrayList<>());
            buildPLIs_col_bySection(col_start, tuple_id_start, rid, tblData);
            // update
            col_start += colCounts[rid];
            tuple_id_start += lineCounts[rid];
        }

        // set encoded integer values for each column
        for (ParsedColumn<?> pc : parsedColumns) {
            pc.setValuesInt(providerS, providerD, providerL);
        }

        //清空原始数据,释放内存
        cleanOriginData();
    }

    public void buildPLIs_col_OnSpark(int chunkLength) {
        // construct PLIs for all tables
        int col_start = 0;
        int tuple_id_start = 0;
        interval = chunkLength;

        // record all values
        for (int rid = 0; rid < colCounts.length; rid++) {
            getInts_col_s_part1_fix(col_start, rid);
            col_start += colCounts[rid];
            tuple_id_start += lineCounts[rid];
        }

        col_start = 0;
        tuple_id_start = 0;
        List<TableData> tblList = new ArrayList<>();
        //处理多表关联的情况,生成单表的schema
        // createTableDataMeta(joinInfos, tblList);

        providerS = IndexProvider.getSorted(providerS);
        providerL = IndexProvider.getSorted(providerL);
        providerD = IndexProvider.getSorted(providerD);

        for (int rid = 0; rid < colCounts.length; rid++) {
            TableData tblData = getTbl(rid, tblList);
            storedPlis.add(new ArrayList<>());
            buildPLIs_col_bySection(col_start, tuple_id_start, rid, tblData);
            // update
            col_start += colCounts[rid];
            tuple_id_start += lineCounts[rid];
        }

        // set encoded integer values for each column
        for (ParsedColumn<?> pc : parsedColumns) {
            pc.setValuesInt(providerS, providerD, providerL);
        }

        //清空原始数据,释放内存
        cleanOriginData();
    }



    private void cleanOriginData() {
        int col_start = 0;
        for (int rid = 0; rid < colCounts.length; rid++) {
            final int COLUMN_COUNT = colCounts[rid];
            for (int ii = 0; ii < COLUMN_COUNT; ++ii) {
                int col = col_start + ii;
                parsedColumns.get(col).cleanBeforeBoradCast();
            }
            col_start += colCounts[rid];
        }
    }



    private void fillNewTbl_bySection(SparkSession spark, List<JoinInfo> joins, List<TableData> tblList) {
        Map<String, List<List<Integer>>> newTbls = new HashMap<>();
        for (JoinInfo join : joins) {
            //执行全关联
            Dataset<Row> ds = spark.sql(join.getGenerateDataSQL());
            //ds.write().format("csv").option("header","true").save("/tmp/rulefind/1334/fullouterjoin/fullouter.result");
            List<Row> rst = ds.collectAsList();
            log.info("###joinTable:{}, sparksql:{},result size:{}, all names:{}", join.getTableName(),
                    join.getGenerateDataSQL(), rst.size(), names);

            long row_count = rst.size();
            int sectionSize = (int) Math.ceil(row_count / interval);
            List<PLISection> newSections = new ArrayList<>();
            for (int i = 0; i < sectionSize; i++) {
                newSections.add(new PLISection(i));
            }

            //遍历每条记录，将它们分拣到不同的table中
            newTbls = getNewTbls(rst);
            Map<String, List<Map<Integer, Integer>>> oldTid2New = new HashMap<>();

            int tuple_id_start = 0;
            int tuple_size = 0;
            int pliSize = 0;
            for (int rid = 0; rid < newTblList.size(); rid++) {
                NewTBLInfo tblInfo = newTblList.get(rid);
                tuple_id_start = Math.max((tblInfo.getTidStart() + tblInfo.getTupleNum()), tuple_id_start);
            }

            //遍历旧表更新tid
            for (String joinName : newTbls.keySet()) {
                String[] tableNames = joinName.split(JOIN_DELIMITER);
                //取新旧表tid，一个新tid对应多个联表的旧tid
                List<List<Integer>> newTid2Old = newTbls.get(joinName);

                Map<Integer, Integer> indexMap = new HashMap<>();
                for (int index = 0; index < tableNames.length; index++) {
                    List<Map<Integer, Integer>> old2News = new ArrayList<>();
                    for (int jndex = 0; jndex < names.size(); jndex++) {
                        NewTBLInfo tblInfo = newTblList.get(jndex);
                        if (tableNames[index].equals(names.get(jndex))) {
                            indexMap.put(jndex, index);

                            int newTids = 0;
                            tuple_size = Math.max(tuple_size, newTid2Old.size());
                            //将每个旧表的tid与新的tid对应
                            while (newTids < newTid2Old.size()) {
                                int size = 0;
                                Map<Integer, Integer> old2New = new HashMap<>();
                                //按分片来切分tid，一个分片的新tid放到一个map中
                                while (size < interval && newTids < newTid2Old.size()) {
                                    old2New.put(newTid2Old.get(newTids++).get(index) - tblInfo.getTidStart(), newTids);
                                    size++;
                                }
                                old2News.add(old2New);
                            }
                            //按表名区分各表的旧tid与新的tid的对应
                            oldTid2New.put(tableNames[index], old2News);
                            break;
                        }
                    }

                }

                int col_start = 0;
                //按已分好的切片创建新的表数据
                for (Integer jndex : indexMap.keySet()) {
                    List<PLISection> sections = storedPlis.get(jndex);

                    final int COLUMN_COUNT = colCounts[jndex];

                    List<Map<Integer, Integer>> old2New = oldTid2New.get(names.get(jndex));
                    pliSize = Math.max(pliSize, old2New.size());
                    for (int ii = 0; ii < COLUMN_COUNT; ++ii) {
                        int col = col_start + ii;
                        String key = parsedColumns.get(col).getTableName() + PredicateConfig.TBL_COL_DELIMITER +
                                parsedColumns.get(col).getName();
                        Class type = parsedColumns.get(col).getType();

                        Collection<Integer> values;
                        for (int i = 0; i < old2New.size(); i++) {
                            Map<Integer, Set<Integer>> setPlis = new HashMap<>();
                            Map<Integer, Integer> sectionNewTid = old2New.get(i);
                            for (int pliIndex = 0; pliIndex < sections.size(); pliIndex++) {
                                PLI pli = sections.get(pliIndex).getPLI(key);
                                if (pli == null || pli.getPlis() == null) {
                                    continue;
                                }
                                //更新tid的信息
                                values = pli.getValues();

                                for (Integer value : values) {
                                    setPlis.putIfAbsent(value, new HashSet<>());
                                    List<Integer> oldTids = pli.getTpIDsForValue(value);
                                    if (oldTids != null) {
                                        Set<Integer> set = setPlis.get(value);
                                        for (Integer oldTid : oldTids) {
                                            set.add(sectionNewTid.get(oldTid));
                                        }
                                    }
                                }
                            }
                            PLI newPli = new PLI(setPlis, row_count, type == String.class, setPlis.keySet());
                            newSections.get(i).putPLI(key, newPli);
                        }
                    }
                    col_start += colCounts[jndex];
                }
            }
//            log.debug("###NewTableNames:{}, full outer join record:\n{} " ,newTbls.keySet(), sbRow.toString());
            names.add(join.getTableName());
            storedPlis.add(newSections);
            pliSizes.add(pliSize);

            NewTBLInfo tblInfo = new NewTBLInfo();
            tblInfo.setTblName(join.getTableName());
            tblInfo.setTidStart(tuple_id_start);
            tblInfo.setTupleNum(tuple_size);
            newTblList.add(tblInfo);

            log.info(">>>>show fk table name:{}, pliSize:{}, newSections:{}", join.getTableName(), pliSize, newSections.size());
        }

    }

    private Map<String, List<List<Integer>>> getNewTbls(List<Row> rst) {
        Map<String, List<List<Integer>>> newTbls = new HashMap<>();
        for (Row row : rst) {
            StringBuffer sb = new StringBuffer();
            List<Integer> tuple = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                if (row.get(i) != null) {
                    String colName = row.schema().fieldNames()[i];
                    sb.append(colName.substring(0, colName.indexOf(TBL_COL_DELIMITER)));
                    sb.append(JOIN_DELIMITER);
                    tuple.add(row.getInt(i));
                }
            }
            String matchTbl = sb.substring(0, sb.length() - JOIN_DELIMITER.length());
            newTbls.putIfAbsent(matchTbl, new ArrayList<>());
            newTbls.get(matchTbl).add(tuple);
        }

        return newTbls;
    }





    private TableData getTbl(int rid, List<TableData> tblList) {
        for (TableData tbl : tblList) {
            if (tbl.getTableName().equals(names.get(rid))) {
                return tbl;
            }
        }
        return null;
    }


    public void buildPLIs_col() {
        // construct PLIs for all tables
        int col_start = 0;
        int tuple_id_start = 0;

        col_start = 0;
        tuple_id_start = 0;

        providerS = IndexProvider.getSorted(providerS);
        providerL = IndexProvider.getSorted(providerL);
        providerD = IndexProvider.getSorted(providerD);

        for (int rid = 0; rid < colCounts.length; rid++) {
            buildPLIs_col_s(col_start, tuple_id_start, rid);
            // update
            col_start += colCounts[rid];
            tuple_id_start += lineCounts[rid];
        }

        // set encoded integer values for each column
        for (ParsedColumn<?> pc : parsedColumns) {
            pc.setValuesInt(providerS, providerD, providerL);
        }
    }


    public void buildPLIs_col_s(int col_start, int tuple_id_start, int rid) {

        long time = System.currentTimeMillis();

        final int COLUMN_COUNT = colCounts[rid]; // parsedColumns.size();
        final int ROW_COUNT = lineCounts[rid];


        List<Integer> tIDs = new TupleIDProvider(tuple_id_start, ROW_COUNT).gettIDs(); // to save integers storage

        //int[][] inputs = getInts_col_s(col_start, rid);
        int[][] inputs = getInts_col_s_part1(col_start, rid);
        inputs = getInts_col_s_part2(col_start, rid, inputs);

        //for (int col = 0; col < COLUMN_COUNT; ++col) {
        for (int ii = 0; ii < COLUMN_COUNT; ii++) {
            int col = col_start + ii;
            TIntSet distincts = new TIntHashSet();
            for (int line = 0; line < ROW_COUNT; ++line) {
                distincts.add(inputs[line][ii]);
            }

            int[] distinctsArray = distincts.toArray();

            // need to sort for doubles and integers
            if (!(parsedColumns.get(col).getType() == String.class)) {
                Arrays.sort(distinctsArray);// ascending is the default
            }

            TIntIntMap translator = new TIntIntHashMap();
            for (int position = 0; position < distinctsArray.length; position++) {
                translator.put(distinctsArray[position], position);
            }

            List<Set<Integer>> setPlis = new ArrayList<>();
            for (int i = 0; i < distinctsArray.length; i++) {
                setPlis.add(new TreeSet<Integer>());
            }

            for (int line = 0; line < ROW_COUNT; ++line) {
                Integer tid = tIDs.get(line);
                setPlis.get(translator.get(inputs[line][ii])).add(tid);
            }

            int values[] = new int[ROW_COUNT];
            for (int line = 0; line < ROW_COUNT; ++line) {
                values[line] = inputs[line][ii];
            }

            PLI pli;

            if (!(parsedColumns.get(col).getType() == String.class)) {
                pli = new PLI(setPlis, ROW_COUNT, true, values, tuple_id_start);
            } else {
                //System.out.println(parsedColumns.get(col).getTableName() + "." + parsedColumns.get(col).getName());
                //System.out.println(parsedColumns.get(col).Cardinality());
                pli = new PLI(setPlis, ROW_COUNT, false, values, tuple_id_start);
            }
            parsedColumns.get(col).setPLI(pli);
        }
        log.info("Time to build plis: " + (System.currentTimeMillis() - time));
    }


    public String getRelationNameTID(Integer tid) {
        int tuple_id_start = 0;
        int tid_ = tid.intValue();
        for (int rid = 0; rid < colCounts.length; rid++) {
            if (rid == 0) {
                if (tid_ >= 0 && tid_ < lineCounts[rid]) {
                    return names.get(rid);
                }
            } else {
                if (tid_ >= tuple_id_start && tid_ < (tuple_id_start + lineCounts[rid])) {
                    return names.get(rid);
                }
            }
            tuple_id_start += lineCounts[rid];
        }
        return names.get(names.size() - 1);
    }

    public int getTupleIDStart(String relation_name) {
        int tuple_id_start = 0;
        for (int rid = 0; rid < names.size(); rid++) {
            if (names.get(rid).equals(relation_name)) {
                return tuple_id_start;
            }
            tuple_id_start += lineCounts[rid];
        }
        return tuple_id_start;
    }

    public int getRowCount(String relation_name) {
        for (int rid = 0; rid < names.size(); rid++) {
            if (names.get(rid).equals(relation_name)) {
                return lineCounts[rid];
            }
        }
        return 0;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }

    public int[] getLineCounts() {
        return lineCounts;
    }

    public void setLineCounts(int[] lineCounts) {
        this.lineCounts = lineCounts;
    }

    public int[] getColCounts() {
        return colCounts;
    }

    public void setColCounts(int[] colCounts) {
        this.colCounts = colCounts;
    }

    public void setLineCounts_col(int[] lineCounts_col) {
        this.lineCounts_col = lineCounts_col;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }

    public List<NewTBLInfo> getNewTblList() {
        return this.newTblList;
    }

    /*
     * transform constant in constant predicates to integer
     * ONLY for EQUALITY
     * */
    public void transformConstantPredicates(Set<Predicate> constantPredicates, IndexProvider<String> providerS,
                                            IndexProvider<Long> providerL, IndexProvider<Double> providerD) {
        for (Predicate p : constantPredicates) {
            if (p.getOperator() != Operator.EQUAL) {
                continue;
            }

            // check types and transform
            if (p.getOperand1().getColumn().getType() == String.class) {
                Integer constantInt = providerS.transform(p.getConstant());
                p.setConstantInt(constantInt);
            } else if (p.getOperand1().getColumn().getType() == Long.class) {
                Long c = Long.parseLong(p.getConstant());
                Integer constantInt = providerL.transform(c);
                p.setConstantInt(constantInt);
            } else if (p.getOperand1().getColumn().getType() == Double.class) {
                Double c = Double.parseDouble(p.getConstant());
                Integer constantInt = providerD.transform(c);
                p.setConstantInt(constantInt);
            }
        }
    }


    public void transformConstantPredicates(Set<Predicate> constantPredicates) {
        for (Predicate p : constantPredicates) {
            if (p.getOperator() != Operator.EQUAL) {
                continue;
            }

            // check types and transform
            if (p.getOperand1().getColumn().getType() == String.class) {
                Integer constantInt = providerS.transform(p.getConstant());
                p.setConstantInt(constantInt);
            } else if (p.getOperand1().getColumn().getType() == Long.class) {
                Long c = Long.parseLong(p.getConstant());
                Integer constantInt = providerL.transform(c);
                p.setConstantInt(constantInt);
            } else if (p.getOperand1().getColumn().getType() == Double.class) {
                Double c = Double.parseDouble(p.getConstant());
                Integer constantInt = providerD.transform(c);
                p.setConstantInt(constantInt);
            }
        }
    }

    public Map<Integer, List<Integer>> getNewMapping() {
        return newMapping;
    }

    public void setStoredPli(Map<String, PLI> storedPli) {
        this.storedPli = storedPli;
    }

    public Map<String, PLI> getStoredPli() {
        return storedPli;
    }

    public List<PLISection> getSectionPli(int rid) {
        return storedPlis.get(rid);
    }

    public void setSectionPli(List<List<PLISection>> pliSections) {
        storedPlis = pliSections;
    }

    public List<ParsedColumn<?>> getParsedColumns() {
        return parsedColumns;
    }

    public void setParsedColumns(List<ParsedColumn<?>> parsedColumns) {
        this.parsedColumns = parsedColumns;
    }


    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Map<Integer, String> getTidToRowid(int rid) {
        return tids_rowids.get(rid);
    }

    public List<Map<Integer, String>> getTidRowidList() {
        return tids_rowids;
    }

    // sample with pivotal tuples
    public void sample(Collection<String> pivotals, double sample_ratio) {
        int total_line = this.getLineCount();
        int sample_num = (int) (total_line * sample_ratio);
        int pivotal_num = pivotals.size();
        int avg_per_one = (int) Math.ceil(sample_num * 1.0 / pivotal_num);

        for (String tupleInfo : pivotals) {
            String relation = tupleInfo.split(SAMPLE_RELATION_TID)[0];
            int tid_start = this.getTupleIDStart(relation);
            int tid_offset = Integer.parseInt(tupleInfo.split(SAMPLE_RELATION_TID)[1]);
            int tid = tid_start + tid_offset;

        }
    }
}


