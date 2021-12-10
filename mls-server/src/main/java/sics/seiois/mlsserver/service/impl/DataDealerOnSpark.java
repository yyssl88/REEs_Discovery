package sics.seiois.mlsserver.service.impl;

import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.client.model.mls.*;
import org.apache.spark.sql.AnalysisException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.mock.RuleFindRequestMock;
import sics.seiois.mlsserver.model.PredicateConfig;

import java.util.*;

public class DataDealerOnSpark {

    private static final Logger logger = LoggerFactory.getLogger(DataDealerOnSpark.class);
    public final static String CSV_TYPE = "csv";
    private static final String  TBL_DELIMITER = "_AND_";
    private static final String TBL_COL = "__";


    /**
     * 只返回join后的表
     * @param tableInfos
     * @param taskId
     * @return
     * @throws AnalysisException
     */
    public static JoinInfos getJoinTablesOnly(TableInfos tableInfos, String taskId) {
        logger.info("****input table sizes: " + tableInfos.getTableInfoList().size());
        JoinInfos joinInfos = new JoinInfos();
        List<JoinInfo> joinInfoList = new ArrayList<>();

        TableInfos tableInfosNew = new TableInfos();
        List<TableInfo> tableInfoNew = new ArrayList<>();
        tableInfosNew.setTableInfoList(tableInfoNew);

//        for (int i = 0; i < tableInfos.getTableInfoList().size(); i++) {
//            String tablename = tableInfos.getTableInfoList().get(i).getTableName();
//            String tableLocation = tableInfos.getTableInfoList().get(i).getTableDataPath();
//
//            if (tableLocation.endsWith(CSV_TYPE)) {
//                //创建表
//                logger.info("****a:");
//                spark.read().format("com.databricks.spark.csv").option("header", "true").load(tableLocation).createOrReplaceTempView(tablename);
//            } else {
//                logger.info("****b:");
//                spark.read().format("orc").load(tableLocation).createOrReplaceTempView(tablename);
//            }
//            logger.info("****表格" + tablename);
//        }

        HashMap<String, TableInfo> name2Table = new HashMap<>();
        for (int i = 0; i < tableInfos.getTableInfoList().size(); i++) {
            name2Table.put(tableInfos.getTableInfoList().get(i).getTableName(), tableInfos.getTableInfoList().get(i));
        }

        Map<String, List<String>> mapFkTable = new HashMap<>();
        HashMap<String, List<String>> mapTableAndFk = new HashMap<>();
        Map<String, Set<String>> mapFkRel = new HashMap<>();
        List<String> joinTableSet = getJoinTables(tableInfos, mapFkTable, mapTableAndFk, mapFkRel);

//        for (int x = 0; x < tableInfos.getTableInfoList().size(); x++) {
//            TableInfo oriInfo = tableInfos.getTableInfoList().get(x);
//            JoinInfo joinInfoOri = new JoinInfo();
//            joinInfoOri.setTableName(oriInfo.getTableName());
//            joinInfoOri.setColumnList(oriInfo.getColumnList());
        //joinInfoOri.setTableDataPath("/tmp/rulefind/" + taskId + "/" + oriInfo.getTableName() + ".csv");
//            joinInfoOri.setTableDataPath("/tmp/rulefind/" + taskId + "/" + oriInfo.getTableName() + ".orc");
//            joinInfoOri.setTableDataPath(oriInfo.getTableDataPath());
//            joinInfoList.add(joinInfoOri);

//            Dataset<Row> dsOri = spark.sql("select * from " + oriInfo.getTableName());
//            dsOri.createOrReplaceGlobalTempView(oriInfo.getTableName());
        //dsOri.write().format("csv").option("header","true").save(joinInfoOri.getTableDataPath());
//            dsOri.write().format("orc").save(joinInfoOri.getTableDataPath());
//            dsOri.show(5);
//        }

        logger.info("******all join tables:" + joinTableSet.toString());
        for (String joinTable : joinTableSet) {
            String[] tables = joinTable.split("_AND_");//"_AND_");
            StringBuffer sql = new StringBuffer("select ");
            StringBuffer select = new StringBuffer();
            StringBuffer join = new StringBuffer();
            StringBuffer on = new StringBuffer();
            //StringBuffer on = new StringBuffer(" on ");
            List<ColumnInfo> columnInfoList = new ArrayList<>();
            getSql(name2Table, tables, select, join, on, columnInfoList);
            JoinInfo joinInfo = new JoinInfo();

            joinInfo.setTableName(joinTable);
            joinInfo.setTableDataPath(PredicateConfig.MLS_TMP_HOME + taskId + "/" + joinTable + ".csv");
//            joinInfo.setTableDataPath("/tmp/rulefind/" + taskId + "/" + joinTable + ".orc");
            joinInfo.setColumnList(columnInfoList);
            joinInfo.setFkRelation(mapFkRel);

            sql.append(select.substring(0, select.length() - 1) + " from " + tables[0] + join);
            //sql += " from " + tables[0]  + join + on;
            logger.info("****SQL:" + sql.toString());
            joinInfo.setGenerateDataSQL(sql.toString());
            joinInfo.setTableNumber(tables.length);
            joinInfoList.add(joinInfo);

//            Dataset<Row> ds = spark.sql(sql.toString());
//            ds.createOrReplaceGlobalTempView(joinTable);
//            //ds.write().format("csv").option("header","true").save(joinInfo.getTableDataPath());
//            ds.write().format("orc").save(joinInfo.getTableDataPath());
//            ds.show(5);
//            spark.read().format("orc").load(joinInfo.getTableDataPath()).createOrReplaceTempView(joinInfo.getTableName());
        }

        for (JoinInfo join : joinInfoList) {
            logger.info("***joinInfos:" + join.getTableName());
        }
        List<String> dropUselessTbls = needDropUselessData(tableInfos);
        joinInfos.setJoinInfoList(joinInfoList);

        if(joinInfoList.size() == 0){
            return joinInfos;
        }

        //例如TLSP,SN,PM,NQ,CD则需要将TLSPNMQ构建成一个full out join大表，CD是单独的表
        mergeJoinTable(joinInfos,dropUselessTbls);

        for (JoinInfo join : joinInfos.getJoinInfoList()) {
            logger.info("***merged joinInfos:{}, sql:{}", join.getTableName(), join.getGenerateDataSQL());
        }

        return joinInfos;
    }

    private static List<String> needDropUselessData(TableInfos tableInfos) {
        List<String> tbls = new ArrayList<>();
        for (TableInfo tableInfo : tableInfos.getTableInfoList()) {
            if (tableInfo.isDropUselessData()) {
                tbls.add(tableInfo.getTableName());
            }
        }
        return tbls;
    }

    private static void mergeJoinTable(JoinInfos joinInfos,List<String> dropUselessTbls) {
        List<JoinInfo> joins = joinInfos.getJoinInfoList();
        //从小到大
        joins.sort(new Comparator<JoinInfo>() {
            @Override
            public int compare(JoinInfo o1, JoinInfo o2) {
                return Integer.compare(o1.getTableNumber(), o2.getTableNumber());
            }
        });
        //直接根据fk的信息，生成所有可能的joinTable,因为是full out join，所以，左右两边都是可以互换的
        List<JoinInfo> newJoins = new ArrayList<>();
        generateLinkedTable(newJoins, joins.get(0).getFkRelation(), dropUselessTbls);
        joinInfos.setJoinInfoList(newJoins);
    }

    private static void generateLinkedTable(List<JoinInfo> newJoins, Map<String, Set<String>> fkRelation, List<String> dropUselessTbls) {
        //找到所有的表
        Set<String> allTables = getAllTables(fkRelation);
        //从每个表出发，找到能组装的最大的链表，将已经组装的删除掉
        /**
         * 各种链接情况，但是不支持环，就是链接回自己
         * A-B-D-E-F
         *  \C  \K
         */
        List<String> tblList = new ArrayList<>(allTables);
        for (int i = tblList.size() - 1; i >= 0; ) {
            String startOne = tblList.get(i);
            if (dropUselessTbls.contains(startOne)) {
                i--;
                continue;
            }
            StringBuffer select = new StringBuffer();
            select.append("select ");
            StringBuffer on = new StringBuffer();
            on.append(" from ");
            Set<String> connectedTbls = new HashSet<>();//已经被链接过的表
            //接下来要遍历的表，结构类似一个线段，起始点，只有一个起点，后边节点有两个元素，第一个元素类似线段的起点，第二个元素类似线段的终点
            List<List<String>> pendingQueue = new ArrayList<>();
            //把第一个点，放到pendingqueue中，
            List<String> startNode = new ArrayList<>();
            startNode.add(startOne);
            pendingQueue.add(startNode);
            logger.info("###start node:{}",startNode);
            while (pendingQueue.size() > 0) {
                pendingQueue = processPendingQueue(pendingQueue, fkRelation, select, on, connectedTbls, dropUselessTbls);
                logger.info("###Next Pending Queue:{}", pendingQueue);
            }
            //已经串起来的都删除
            tblList.removeIf(tbl -> connectedTbls.contains(tbl));

            StringBuffer mergeTblName = new StringBuffer();
            for(String name : connectedTbls){
                mergeTblName.append(name);
                mergeTblName.append(PredicateConfig.JOIN_TABLE_DELIMITER);
            }
            String selectStr = select.substring(0, select.length() - 2);//把最后的, 去掉
            JoinInfo mergedJoinTable = new JoinInfo();
            mergedJoinTable.setTableNumber(connectedTbls.size());
            mergedJoinTable.setTableName(mergeTblName.substring(0, mergeTblName.length() - TBL_DELIMITER.length()));
            mergedJoinTable.setGenerateDataSQL(selectStr + on.toString());
            mergedJoinTable.setFkRelation(fkRelation);
            newJoins.add(mergedJoinTable);

            i = tblList.size() -1;
        }

    }

    private static List<List<String>> processPendingQueue(List<List<String>> pendingQueue,
                                                    Map<String, Set<String>> fkRelation,
                                                    StringBuffer select, StringBuffer on, Set<String> connectedTbls, List<String> dropUselessTbls) {
        List<List<String>> nextPendingQueue = new ArrayList<>();
        for(List<String> tblNode : pendingQueue) {
            genSubSql(select, on, tblNode, connectedTbls, dropUselessTbls);
            addToConnected(connectedTbls, tblNode);
            Set<List<String>> connTbls = getConnTbls(tblNode, fkRelation, connectedTbls);
            addToNextPendingQueue(connTbls, nextPendingQueue);
        }
        return nextPendingQueue;
    }

    private static void genSubSql(StringBuffer select, StringBuffer on, List<String> tblNode,
                                   Set<String> connectedTbls, List<String> dropUselessTbls) {
        //第一个节点
        if (tblNode.size() == 1) {
            select.append(tblNode.get(0) + ".tid as " + tblNode.get(0) + TBL_COL + "tid, ");
            on.append(tblNode.get(0));
            return;
        }
        String[] tblColLeft = tblNode.get(0).split(TBL_COL);
        //只处理连接的右表
        String[] tblColRight = tblNode.get(1).split(TBL_COL);
        select.append(tblColRight[0] + ".tid as " + tblColRight[0] + TBL_COL + "tid, ");
        if (dropUselessTbls.contains(tblColRight[0])) {
            on.append(" left outer join " + tblColRight[0] + " on " +
                    tblColLeft[0] + "." + tblColLeft[1] + "=" + tblColRight[0] + "." + tblColRight[1]);
        } else {
            on.append(" full outer join " + tblColRight[0] + " on " +
                    tblColLeft[0] + "." + tblColLeft[1] + "=" + tblColRight[0] + "." + tblColRight[1]);
        }


    }

    private static void addToNextPendingQueue(Set<List<String>> connTbls, List<List<String>> nextPendingQueue) {
        /**
         * A-(B,C), D-(B,C), 这种情况，就要把，B，C只加一次
         */
        for (List<String> line : connTbls) {
            boolean contained = false;
            for (List<String> newLine : nextPendingQueue) {
                if (line.get(1).split(TBL_COL)[0].equals(newLine.get(1).split(TBL_COL)[0])) {
                    contained = true;
                }
            }
            if (contained == false) {
                nextPendingQueue.add(line);
            }
        }
    }

    private static void addToConnected(Set<String> connectedTbls, List<String> node) {
        if (node.size() == 1) {
            connectedTbls.add(node.get(0));
        } else {
            connectedTbls.add(node.get(1).split(TBL_COL)[0]);
        }
    }

    private static Set<List<String>> getConnTbls(List<String> node, Map<String, Set<String>> fkRelation,
                                                 Set<String> connectedTbls) {
        //第一个节点特殊处理，里边的值就是tablename，没有列信息
        String one = "";
        if (node.size() == 1) {
            one = node.get(0);
        } else {
            one = node.get(1).split(TBL_COL)[0];
        }
        //这里的list只有2个元素，第一个是left，第二个是rigt，left是起点，right是连接的点
        Set<List<String>> secondList = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : fkRelation.entrySet()) {
            //如果左边相同，就直接把右边的加进去
            if (entry.getKey().split(TBL_COL)[0].equals(one)) {
                for (String right : entry.getValue()) {
                    List<String> fk = new ArrayList<>();
                    fk.add(entry.getKey());
                    fk.add(right);//新增加的点
                    //已经处理过的节点，比如A-B-C，在处理B的时候，不要再取到A了
                    if (!connectedTbls.contains(right.split(TBL_COL)[0])) {
                        secondList.add(fk);
                    }
                }
            }
            //如果右边相同，就把右边的加到第一个，左边的加到第二个
            for (String right : entry.getValue()) {
                if (right.split(TBL_COL)[0].equals(one)) {
                    List<String> fk = new ArrayList<>();
                    fk.add(right);
                    fk.add(entry.getKey());//新增加的点
                    if (!connectedTbls.contains(entry.getKey().split(TBL_COL)[0])) {
                        secondList.add(fk);
                    }
                }
            }
        }
        return secondList;
    }

    private static Set<String> getAllTables(Map<String, Set<String>> fkRelation) {
        Set<String> tables = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : fkRelation.entrySet()) {
            tables.add(entry.getKey().split(TBL_COL)[0]);
            for (String right : entry.getValue()) {
                tables.add(right.split(TBL_COL)[0]);
            }
        }
        return tables;
    }

    private static void getSql(HashMap<String, TableInfo> mapName2Table, String[] tables, StringBuffer select, StringBuffer join, StringBuffer on, List<ColumnInfo> columnInfoList) {
        int i = 0;
        for (String table : tables) {
            TableInfo tableInfo = mapName2Table.get(table);
            select.append(tableInfo.getTableName() + "." + "tid" + " AS " + tableInfo.getTableName() + "__" +
                    "tid" + ",");
            ColumnInfo info = new ColumnInfo();
            info.setColumnName(tableInfo.getTableName() + "__" + "tid");
            info.setColumnType("int");
            info.setModelStorePath("");
            info.setModelId("");
            columnInfoList.add(info);
            if (i > 0) {
                join.append(" full outer join " + tableInfo.getTableName() + " on ");
                for (ColumnInfo col : mapName2Table.get(tables[0]).getColumnList()) {
                    if (col.getFkMsgList() != null) {
                        for(FKMsg fkMsg : col.getFkMsgList()) {
                            if (fkMsg.getFkTableName().equals(tableInfo.getTableName())) {
                                on.append(mapName2Table.get(tables[0]).getTableName() + "." + col.getColumnName() + "=" +
                                        fkMsg.getFkTableName() + "." + fkMsg.getFkColumnName() + ",");
                            }
                        }
                    }
                }
                if (on.length() != 0) {
                    join.append(on.substring(0, on.length() - 1));
                    on.delete(0, on.length());
                }
            }
            i++;
        }
    }

    @NotNull
    private static List<String> getJoinTables(TableInfos tableInfos, Map<String, List<String>> mapFkTable,
                                                 HashMap<String, List<String>> mapTableAndFk, Map<String, Set<String>> mapFkRel) {
        //把所有表的关联的表都找出来，放在mapFkTable中，比如t1->t2,t3
        for (TableInfo tableInfo : tableInfos.getTableInfoList()) {
            List<String> listFkTable = new ArrayList<>();
            List<String> listFkOn = new ArrayList<>();
            for (ColumnInfo col : tableInfo.getColumnList()) {
                if (col.getFkMsgList() != null) {
                    for (FKMsg fkMsg : col.getFkMsgList()) {
                        String colName = tableInfo.getTableName() + "." + col.getColumnName();
                        String colAlias = tableInfo.getTableName() + "__" + col.getColumnName();
                        String FkColumn = fkMsg.getFkColumnName();
                        String FkTable = fkMsg.getFkTableName();
                        String FkAlias = FkTable + "__" + FkColumn;
                        mapFkRel.putIfAbsent(colAlias,new HashSet<>());
                        mapFkRel.get(colAlias).add(FkAlias);
                        String On = colName + "=" + FkTable + "." + FkColumn;
                        listFkTable.add(FkTable);
                        listFkOn.add(On);
                        mapFkTable.put(tableInfo.getTableName(), listFkTable);
                    }
                }
            }
            mapTableAndFk.put(tableInfo.getTableName(), listFkOn);
        }

        //对于t1->t2,t3， 先进行t1_t2联表,再进行深度关联递归调用
        Set<String> joinTableSet = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : mapFkTable.entrySet()) {
            String join = entry.getKey();
            String horizon = join;//横向添加  t1-t2-t3
            for (String innerTable : entry.getValue()) {
                FkJoinCul(join, innerTable, mapFkTable, joinTableSet);
            }
            for (String innerTable : entry.getValue()) {
                FkJoinCul(horizon, innerTable, mapFkTable, joinTableSet);
                horizon = horizon + TBL_DELIMITER + innerTable;
            }
        }
        List<String> ordedJoinTbls = new ArrayList<>(joinTableSet);
        ordedJoinTbls.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        return ordedJoinTbls;
    }

    public static void FkJoinCul(String table1, String table2, Map<String, List<String>> mapFk, Set<String> result) {
        String FkRelation = "";
        int L = PredicateConfig.getL();
        //int L = 3;
        logger.info("****L is: " + String.valueOf(L));

        String join = table1 + "_AND_" + table2;
        if (join.split("_AND_").length < L) {
            if (mapFk.get(table2) == null) {
                result.add(join);
                return;
            }
            for (int i = 0; i < mapFk.get(table2).size(); i++) {
                FkJoinCul(join, mapFk.get(table2).get(i), mapFk, result);
            }
        } else if (join.split("_AND_").length == L) {
            result.add(join);
            return;
        } else if (join.split("_AND_").length > L) {
            return;
        }
    }


    public static void main(String[] args) throws Exception {
        RuleDiscoverExecuteRequest req = RuleFindRequestMock.mockRuleFindReqest("imdb");
        JoinInfos joins = DataDealerOnSpark.getJoinTablesOnly(req.getTableInfos(), req.getTaskId());

//        System.out.println(joinTbl.getJoinInfoList());
    }
}
