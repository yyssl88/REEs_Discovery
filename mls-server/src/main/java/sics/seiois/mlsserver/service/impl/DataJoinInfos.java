package sics.seiois.mlsserver.service.impl;

import com.sics.seiois.client.model.mls.*;
import org.apache.spark.sql.AnalysisException;
import org.jetbrains.annotations.NotNull;
import sics.seiois.mlsserver.model.PredicateConfig;
import sics.seiois.mlsserver.utils.MlsConstant;

import java.util.*;

public class DataJoinInfos {
    public final static String CSV_TYPE = "csv";

    public static JoinInfos Joins(TableInfos tableInfos, String taskId) throws AnalysisException {

        JoinInfos joinTableInfoSet = new JoinInfos();

        List<JoinInfo> joinInfoList = new ArrayList<>();

        //形成tableName->TableInfo的map
        HashMap<String, TableInfo> mapNameOfTable = new HashMap<>();
        for(int i = 0; i < tableInfos.getTableInfoList().size(); i++){
            mapNameOfTable.put(tableInfos.getTableInfoList().get(i).getTableName(),tableInfos.getTableInfoList().get(i));
        }

        Map<String,List<String>> mapFkTable = new HashMap<>();
        HashMap<String, List<String>> mapTableAndFk = new HashMap<>();
        HashMap<String,Set<String>> mapFkRel = new HashMap<>();
        //得到联表所需诸元
        HashSet<String> joinTableSet = getInfoOfTables(tableInfos, mapFkTable, mapTableAndFk, mapFkRel);

        //把原表的tableInfos转成joinInfos
        for(int x = 0; x < tableInfos.getTableInfoList().size(); x++){
            TableInfo oriInfo = tableInfos.getTableInfoList().get(x);
            JoinInfo joinInfoOri = new JoinInfo();
            joinInfoOri.setTableName(oriInfo.getTableName());
            joinInfoOri.setColumnList(oriInfo.getColumnList());
            //joinInfoOri.setTableDataPath("/tmp/rulefind/" + taskId + "/" + oriInfo.getTableName() + ".csv");
            joinInfoOri.setTableDataPath("/tmp/rulefind/" + taskId + "/" + oriInfo.getTableName() + ".orc");
            joinInfoList.add(joinInfoOri);

        }

        //联表sql处理，得到所有的联表，并且得到的联表存入joinInfoList里面
        Map<String, String> tableSql = new HashMap<String, String>();
        for(String joinTable : joinTableSet) {
            String [] tables = joinTable.split(MlsConstant.RD_JOINTABLE_SPLIT_STR);
            StringBuffer sql = new StringBuffer("select ");
            StringBuffer select = new StringBuffer();
            StringBuffer join = new StringBuffer();
            StringBuffer on = new StringBuffer();
            List<ColumnInfo> columnInfoList = new ArrayList<>();
            //得到联表sql语句
            getSql(mapNameOfTable, tables, select, join, on, columnInfoList);
            JoinInfo joinInfo = new JoinInfo();

            joinInfo.setTableName(joinTable);
            //joinInfo.setTableDataPath("/tmp/rulefind/" + taskId + "/" + joinTable + ".csv");
            joinInfo.setTableDataPath("/tmp/rulefind/" + taskId + "/" + joinTable + ".orc");
            joinInfo.setColumnList(columnInfoList);
            joinInfo.setFkRelation(mapFkRel);
            joinInfoList.add(joinInfo);

            sql.append(select.substring(0, select.length() - 1) + " from " + tables[0]  + join);
            tableSql.put(joinTable, sql.toString());
        }
        joinTableInfoSet.setJoinInfoList(joinInfoList);

        return joinTableInfoSet;
    }

    private static void getSql(HashMap<String, TableInfo> mapNameOfTable, String[] tables, StringBuffer select, StringBuffer join, StringBuffer on, List<ColumnInfo> columnInfoList) {
        int i = 0;
        List<ColumnInfo> colList = new ArrayList<>();
        HashMap<ColumnInfo,String> mapColTable = new HashMap<>();
        for (String table : tables) {
            TableInfo tableInfo = mapNameOfTable.get(table);
            for (ColumnInfo col : tableInfo.getColumnList()) {
                select.append(tableInfo.getTableName() + "." + col.getColumnName() + " AS " + tableInfo.getTableName()+ "__" + col.getColumnName() + ",");
                ColumnInfo info = new ColumnInfo();
                info.setColumnName(tableInfo.getTableName()+ "__" + col.getColumnName());
                info.setColumnType(col.getColumnType());
                info.setModelStorePath(col.getModelStorePath());
                info.setModelId(col.getModelId());
                columnInfoList.add(info);
                colList.add(col);
                mapColTable.put(col,tableInfo.getTableName());
            }
            if(i>0) {
                join.append(" inner join " + tableInfo.getTableName() + " on ");

                //List<ColumnInfo> colList = new ArrayList<>();
                /*for (ColumnInfo col : mapNameOfTable.get(tables[i-1]).getColumnList()){
                    colList.add(col);
                    if(i>1){
                        for (ColumnInfo colcolle : colList){
                            if(colcolle.getFkMsg() != null){
                                if(colcolle.getFkMsg().getFkTableName().equals(tableInfo.getTableName())){
                                    on.append(mapNameOfTable.get(tables[i-1]).getTableName() + "." + colcolle.getColumnName() + "=" + colcolle.getFkMsg().getFkTableName() + "." + colcolle.getFkMsg().getFkColumnName() + " and ");
                                }
                            }
                        }
                    }else{
                        if(col.getFkMsg() != null){
                            if(col.getFkMsg().getFkTableName().equals(tableInfo.getTableName())){
                                on.append(mapNameOfTable.get(tables[0]).getTableName() + "." + col.getColumnName() + "=" + col.getFkMsg().getFkTableName() + "." + col.getFkMsg().getFkColumnName() + " and ");
                            }
                        }
                    }
                }*/
                for (ColumnInfo colcolle : colList) {
                    if (colcolle.getFkMsgList() != null) {
                        List<FKMsg> fkList = colcolle.getFkMsgList();
                        for(FKMsg fkMsg : fkList) {
                            if (fkMsg.getFkTableName().equals(tableInfo.getTableName())) {
                                on.append(mapColTable.get(colcolle) + "." + colcolle.getColumnName() + "=" +
                                        fkMsg.getFkTableName() + "." + fkMsg.getFkColumnName() + " and ");
                            }
                        }
                    }
                }
                if(on.length() >= 5){
                    join.append(on.substring(0,on.length() - 5));
                    on.delete(0,on.length());
                }
            }
            i++;
        }
    }

    @NotNull
    private static HashSet<String> getInfoOfTables(TableInfos tableInfos, Map<String, List<String>> mapFkTable,
                                                   HashMap<String, List<String>> mapTableAndFk,
                                                   HashMap<String, Set<String>> mapFkRel) {
        for (TableInfo tableInfo : tableInfos.getTableInfoList()) {
            List<String> listFkTable = new ArrayList<>();
            List<String> listFkOn = new ArrayList<>();
            for (ColumnInfo col : tableInfo.getColumnList()) {
                List<FKMsg> fkMsgList = col.getFkMsgList();
                if (fkMsgList != null) {
                    for (FKMsg fkMsg : fkMsgList) {
                        String colName = tableInfo.getTableName() + "." + col.getColumnName();
                        String colAlias = tableInfo.getTableName() + "__" + col.getColumnName();
                        String FkColumn = fkMsg.getFkColumnName();
                        String FkTable = fkMsg.getFkTableName();
                        String FkAlias = FkTable + "__" + FkColumn;
                        mapFkRel.putIfAbsent(colAlias, new HashSet<>());
                        mapFkRel.get(colAlias).add(FkAlias);
                        String On = colName + "=" + FkTable + "." + FkColumn;
                        if (listFkTable.size() != 0) {
                            int count = 0;
                            for (String fktable : listFkTable) {
                                if (!FkTable.equals(fktable)) {
                                    count++;
                                }
                            }
                            if (count == listFkTable.size()) {
                                listFkTable.add(FkTable);
                            }
                        } else {
                            listFkTable.add(FkTable);
                        }
                        //listFkTable.add(FkTable);
                        listFkOn.add(On);
                        mapFkTable.put(tableInfo.getTableName(), listFkTable);
                    }
                }
            }
            mapTableAndFk.put(tableInfo.getTableName(), listFkOn);
        }

        HashSet<String> joinTableSet = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : mapFkTable.entrySet()) {
            String join = entry.getKey();
            for (String innerTable : entry.getValue()) {
                join += MlsConstant.RD_JOINTABLE_SPLIT_STR + innerTable;
                joinTableSet.add(entry.getKey() + MlsConstant.RD_JOINTABLE_SPLIT_STR + innerTable);
                joinTableSet.add(join);

                FkJoinCul(entry.getKey() + MlsConstant.RD_JOINTABLE_SPLIT_STR + innerTable, innerTable, mapFkTable, joinTableSet);
            }

        }
        return joinTableSet;
    }

    public static void FkJoinCul(String table1, String table2, Map<String,List<String>> mapFk, Set<String> result){
        String FkRelation = "";
        int L = PredicateConfig.getL();
        //int L = 3;

        if(mapFk.get(table2) == null){
            return;
        }

        for(int i = 0; i < mapFk.get(table2).size(); i++){
            FkRelation = table1 + MlsConstant.RD_JOINTABLE_SPLIT_STR + mapFk.get(table2).get(i);
            if (L < (FkRelation.split(MlsConstant.RD_JOINTABLE_SPLIT_STR).length + 1)) {
                FkJoinCul(FkRelation, mapFk.get(table2).get(i), mapFk, result);
            } else {
                result.add(FkRelation);
                return;
            }
        }
        result.add(FkRelation);
    }
}
