package sics.seiois.mlsserver.biz.mock;

import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.client.model.mls.*;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sics.seiois.mlsserver.model.PredicateExpressionModel;
import sics.seiois.mlsserver.utils.helper.MlFactory;
import sics.seiois.mlsserver.utils.helper.SimilarAlgorithmsFactory;

/**
 * Created by friendsyh on 2020/5/14.
 */
public class RuleFindRequestMock {

    private static final Logger logger = LoggerFactory.getLogger(RuleFindRequestMock.class);

    public static void main(String[] args) throws Exception {

        RuleDiscoverExecuteRequest request = mockRuleFindReqest("");
//        TableInfo tableInfo = request.getTableInfo();

        //生成谓词集合测试
//        EvidenceGenerateMain.generateES(request.toString());

        //生成sql测试
//        System.out.println(EvidenceGenerate.genEnvidenceSetSQL(tableInfo, predicateMap));
//        System.out.println(predicateMap);

//        List<REEFinderColMainTestAfterEvid.RuleRow> rules = REEFinderColMainTestAfterEvid.findRule("D:\\temp",
//                Double.parseDouble(request.getFtr()),Double.parseDouble(request.getCr()), "D:\\temp\\my_evidset.txt", "nomlsel");
//        System.out.println(rules);

//        Map<String, String> stringStringMap = RuleFinder.readFileAndSetSparkParam("D:\\config\\spark\\spark-defaults.conf");
//        System.out.println(stringStringMap);

        System.out.println(mockRuleFindReqest("userInfo"));

    }
    public static RuleDiscoverExecuteRequest mockRuleFindReqest(String dataset) {
        RuleDiscoverExecuteRequest ruleRequest = new RuleDiscoverExecuteRequest();
//        mock1MillionMutiTable(ruleRequest);
//        mockWanyiMutiTable(ruleRequest);
        switch (dataset) {
            case "aminer" :
                String directory = "hdfs:///tmp/aminer/us_csv/";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer_ml" :
                directory = "hdfs:///tmp/aminer/us_csv/";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor_ml(ruleRequest);
                break;
            case "aminerlarge" :
                directory = "hdfs:///tmp/aminer/csv/";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer2" :
                directory = "hdfs:///tmp/zhangjun/aminer_sample2";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer4" :
                directory = "hdfs:///tmp/zhangjun/aminer_sample4";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer6" :
                directory = "hdfs:///tmp/zhangjun/aminer_sample6";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer8" :
                directory = "hdfs:///tmp/zhangjun/aminer_sample8";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminersmall" :
                mockPaperAuthorSmall(ruleRequest);
                break;
            case "imdb":
                directory = "hdfs:///tmp/imdb";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb2":
                directory = "hdfs:///tmp/zhangjun/imdb_sample2";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb4":
                directory = "hdfs:///tmp/zhangjun/imdb_sample4";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb6":
                directory = "hdfs:///tmp/zhangjun/imdb_sample6";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb8":
                directory = "hdfs:///tmp/zhangjun/imdb_sample8";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb100":
                directory = "hdfs:///tmp/imdb100";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "adults":
                mockAdults(ruleRequest);
                break;
            case "adults_ml":
                mockAdults_ml(ruleRequest);
                break;
            case "airports":
                mockAirports(ruleRequest);
                break;
            case "airports_ml":
                mockAirports_ml(ruleRequest);
                break;
            case "airportsSampling":
                mockSampleData(ruleRequest, ruleRequest.getTaskId());
                break;
            case "hospital":
                mockHospital(ruleRequest);
                break;
            case "hospital_ml":
                mockHospital_ml(ruleRequest);
                break;
            case "hospitalSampling":
                mockSampleData(ruleRequest, ruleRequest.getTaskId());
                break;
            case "tax1000w":
                mockTax1000w(ruleRequest);
                break;
            case "tax1000wSampling":
                mockSampleData(ruleRequest, ruleRequest.getTaskId());
                break;
            case "tax200w":
                mockTax200w(ruleRequest);
                break;
            case "tax200wSampling":
                mockSampleData(ruleRequest, ruleRequest.getTaskId());
                break;
            case "tax400w":
                mockTax400w(ruleRequest);
                break;
            case "tax400wSampling":
                mockSampleData(ruleRequest, ruleRequest.getTaskId());
                break;
            case "tax600w":
                mockTax600w(ruleRequest);
                break;
            case "tax600wSampling":
                mockSampleData(ruleRequest, ruleRequest.getTaskId());
                break;
            case "tax800w":
                mockTax800w(ruleRequest);
                break;
            case "tax800wSampling":
                mockSampleData(ruleRequest, ruleRequest.getTaskId());
                break;
            case "tax100w":
                mockTax100w(ruleRequest);
                break;
            case "tax10w":
                mockTax10w(ruleRequest);
                break;
            case "flight":
                mockFlight(ruleRequest);
                break;
            case "chess":
                mockChess(ruleRequest);
                break;
            case "aminerSample_0.2":
                mockPaperAuthorSample(ruleRequest, dataset);
                break;
            case "aminerSample_0.4":
                mockPaperAuthorSample(ruleRequest, dataset);
                break;
            case "aminerSample_0.6":
                mockPaperAuthorSample(ruleRequest, dataset);
                break;
            case "aminerSample_0.8":
                mockPaperAuthorSample(ruleRequest, dataset);
                break;
            case "ncvoter":
                mockNcvoter(ruleRequest);
                break;
            case "ncvoterSampling":
                mockSampleData(ruleRequest, ruleRequest.getTaskId());
                break;
            case "inspection":
                mockInspection(ruleRequest);
                break;
            case "inspection_ml":
                mockInspection_ml(ruleRequest);
                break;
            case "inspectionSampling":
                mockSampleData(ruleRequest, ruleRequest.getTaskId());
                break;
            case "hepatitis":
                mockHepatitis(ruleRequest);
                break;
            case "order":
                mockOrder(ruleRequest);
                break;
            case "order10w":
                mockOrder10w(ruleRequest);
                break;
            case "order1q":
                mockOrder1q(ruleRequest);
                break;
            case "order100w":
                mockOrder100w(ruleRequest);
                break;
            case "order200w":
                mockOrder200w(ruleRequest);
                break;
            case "udf2":
                directory = "hdfs:///tmp/zhangjun/udf_sample2";
                ruleRequest.setDimensionID(directory);
                mockUdf2(ruleRequest);
                break;
            case "udf4":
                directory = "hdfs:///tmp/zhangjun/udf_sample4";
                ruleRequest.setDimensionID(directory);
                mockUdf4(ruleRequest);
                break;
            case "udf6":
                directory = "hdfs:///tmp/zhangjun/udf_sample6";
                ruleRequest.setDimensionID(directory);
                mockUdf6(ruleRequest);
                break;
            case "udf8":
                directory = "hdfs:///tmp/zhangjun/udf_sample8";
                ruleRequest.setDimensionID(directory);
                mockUdf8(ruleRequest);
                break;
            case "udf10":
                directory = "hdfs:///tmp/zhangjun/udf_sample10";
                ruleRequest.setDimensionID(directory);
                mockUdf10(ruleRequest);
                break;
            case "productshop":
                mockTwoTableJoin(ruleRequest);
                break;
            case "fdReducedTable":
                mockFdReducedTable(ruleRequest);
                break;
            case "userInfo":
                mockUserInfo(ruleRequest);
                break;
            case "acmTableA":
                mockAcmTableA(ruleRequest);
                break;
            case "shaidi":
//                mockShaidi(ruleRequest);
                mockShaidi_day(ruleRequest);
                break;
            case "deepmatcher":
                mockDeepMatcher(ruleRequest);
                break;
            case "contacts":
                mockContact(ruleRequest);
                break;
            case "TableAB":
                mockAcmTableAB(ruleRequest);
                break;
            case "casorgan":
                mockCasorgan(ruleRequest);
                break;
            case "casorg10w":
                mockCasorgan10w(ruleRequest);
                break;
            case "casorgcn":
                mockCasorgcn(ruleRequest);
                break;
            case "casorgclean":
                mockCasorgClean(ruleRequest);
                break;
            case "star":
                mockStar(ruleRequest);
                break;
            default:
        }

//        mockTwoTableJoin(ruleRequest);
//        mockMutiTable(ruleRequest);
//        mockTbl1WTable(ruleRequest);
//        mockTable(ruleRequest);
//        mockUserTable(ruleRequest);
//        mockAcmTable(ruleRequest);
//        mockChessTable(ruleRequest);
//        mockWbcTable(ruleRequest);
        return ruleRequest;
    }

    /*
     * mock 规则发现接口的请求参数
     * @return
     */
    public static RuleDiscoverExecuteRequest mockRuleFindReqest(String dataset, String dataOption) {
        RuleDiscoverExecuteRequest ruleRequest = new RuleDiscoverExecuteRequest();
//        mock1MillionMutiTable(ruleRequest);
//        mockWanyiMutiTable(ruleRequest);
        switch (dataset) {
            case "aminer" :
                String directory = "hdfs:///tmp/aminer/us_csv/";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer_ml" :
                directory = "hdfs:///tmp/aminer/us_csv/";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor_ml(ruleRequest);
                break;
            case "aminerSampling" :
                directory = "hdfs:///tmp/aminer/us_csv/";
                ruleRequest.setDimensionID(directory);
                String[] options = dataOption.split("__");
                mockPaperAuthorSampling(ruleRequest, options);
                break;
            case "aminerlarge" :
                directory = "hdfs:///tmp/aminer/csv/";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer2" :
                directory = "hdfs:///tmp/zhangjun/aminer_sample2";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer4" :
                directory = "hdfs:///tmp/zhangjun/aminer_sample4";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer6" :
                directory = "hdfs:///tmp/zhangjun/aminer_sample6";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminer8" :
                directory = "hdfs:///tmp/zhangjun/aminer_sample8";
                ruleRequest.setDimensionID(directory);
                mockPaperAuthor(ruleRequest);
                break;
            case "aminersmall" :
                mockPaperAuthorSmall(ruleRequest);
                break;
            case "imdb":
                directory = "hdfs:///tmp/imdb";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb2":
                directory = "hdfs:///tmp/zhangjun/imdb_sample2";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb4":
                directory = "hdfs:///tmp/zhangjun/imdb_sample4";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb6":
                directory = "hdfs:///tmp/zhangjun/imdb_sample6";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb8":
                directory = "hdfs:///tmp/zhangjun/imdb_sample8";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "imdb100":
                directory = "hdfs:///tmp/imdb100";
                ruleRequest.setDimensionID(directory);
                mockIMDB(ruleRequest);
                break;
            case "adults":
                mockAdults(ruleRequest);
                break;
            case "adults_ml":
                mockAdults_ml(ruleRequest);
                break;
            case "airports":
                mockAirports(ruleRequest);
                break;
            case "airports_ml":
                mockAirports_ml(ruleRequest);
                break;
            case "airportsSampling":
                mockSampleData(ruleRequest, dataOption);
                break;
            case "hospital":
                mockHospital(ruleRequest);
                break;
            case "hospital_ml":
                mockHospital_ml(ruleRequest);
                break;
            case "hospitalSampling":
                mockSampleData(ruleRequest, dataOption);
                break;
            case "tax1000w":
                mockTax1000w(ruleRequest);
                break;
            case "tax1000wSampling":
                mockSampleData(ruleRequest, dataOption);
                break;
            case "tax200w":
                mockTax200w(ruleRequest);
                break;
            case "tax200wSampling":
                mockSampleData(ruleRequest, dataOption);
                break;
            case "tax400w":
                mockTax400w(ruleRequest);
                break;
            case "tax400wSampling":
                mockSampleData(ruleRequest, dataOption);
                break;
            case "tax600w":
                mockTax600w(ruleRequest);
                break;
            case "tax600wSampling":
                mockSampleData(ruleRequest, dataOption);
                break;
            case "tax800w":
                mockTax800w(ruleRequest);
                break;
            case "tax800wSampling":
                mockSampleData(ruleRequest, dataOption);
                break;
            case "tax100w":
                mockTax100w(ruleRequest);
                break;
            case "tax10w":
                mockTax10w(ruleRequest);
                break;
            case "flight":
                mockFlight(ruleRequest);
                break;
            case "chess":
                mockChess(ruleRequest);
                break;
            case "aminerSample_0.2":
                mockPaperAuthorSample(ruleRequest, dataset);
                break;
            case "aminerSample_0.4":
                mockPaperAuthorSample(ruleRequest, dataset);
                break;
            case "aminerSample_0.6":
                mockPaperAuthorSample(ruleRequest, dataset);
                break;
            case "aminerSample_0.8":
                mockPaperAuthorSample(ruleRequest, dataset);
                break;
            case "ncvoter":
                mockNcvoter(ruleRequest);
                break;
            case "ncvoterSampling":
                mockSampleData(ruleRequest, dataOption);
                break;
            case "inspection":
                mockInspection(ruleRequest);
                break;
            case "inspection_ml":
                mockInspection_ml(ruleRequest);
                break;
            case "inspectionSampling":
                mockSampleData(ruleRequest, dataOption);
                break;
            case "property" :
                directory = "hdfs:///tmp/zhangjun/property/";
                ruleRequest.setDimensionID(directory);
                mockProperty(ruleRequest, null);
                break;
            case "propertySampling" :
                directory = "hdfs:///tmp/zhangjun/property/";
                ruleRequest.setDimensionID(directory);
                mockSampleData(ruleRequest, dataOption);
                break;
            case "hepatitis":
                mockHepatitis(ruleRequest);
                break;
            case "order":
                mockOrder(ruleRequest);
                break;
            case "order10w":
                mockOrder10w(ruleRequest);
                break;
            case "order1q":
                mockOrder1q(ruleRequest);
                break;
            case "order100w":
                mockOrder100w(ruleRequest);
                break;
            case "order200w":
                mockOrder200w(ruleRequest);
                break;
            case "udf2":
                directory = "hdfs:///tmp/zhangjun/udf_sample2";
                ruleRequest.setDimensionID(directory);
                mockUdf2(ruleRequest);
                break;
            case "udf4":
                directory = "hdfs:///tmp/zhangjun/udf_sample4";
                ruleRequest.setDimensionID(directory);
                mockUdf4(ruleRequest);
                break;
            case "udf6":
                directory = "hdfs:///tmp/zhangjun/udf_sample6";
                ruleRequest.setDimensionID(directory);
                mockUdf6(ruleRequest);
                break;
            case "udf8":
                directory = "hdfs:///tmp/zhangjun/udf_sample8";
                ruleRequest.setDimensionID(directory);
                mockUdf8(ruleRequest);
                break;
            case "udf10":
                directory = "hdfs:///tmp/zhangjun/udf_sample10";
                ruleRequest.setDimensionID(directory);
                mockUdf10(ruleRequest);
                break;
            case "productshop":
                mockTwoTableJoin(ruleRequest);
                break;
            case "fdReducedTable":
                mockFdReducedTable(ruleRequest);
                break;
            case "userInfo":
                mockUserInfo(ruleRequest);
                break;
            case "acmTableA":
                mockAcmTableA(ruleRequest);
                break;
            case "shaidi":
//                mockShaidi(ruleRequest);
                mockShaidi_day(ruleRequest);
                break;
            case "deepmatcher":
                mockDeepMatcher(ruleRequest);
                break;
            case "contacts":
                mockContact(ruleRequest);
                break;
            case "TableAB":
                mockAcmTableAB(ruleRequest);
                break;
            case "casorgan":
                mockCasorgan(ruleRequest);
                break;
            case "casorg10w":
                mockCasorgan10w(ruleRequest);
                break;
            case "casorgcn":
                mockCasorgcn(ruleRequest);
                break;
            case "casorgclean":
                mockCasorgClean(ruleRequest);
                break;
            case "star":
                mockStar(ruleRequest);
                break;
            default:
        }

//        mockTwoTableJoin(ruleRequest);
//        mockMutiTable(ruleRequest);
//        mockTbl1WTable(ruleRequest);
//        mockTable(ruleRequest);
//        mockUserTable(ruleRequest);
//        mockAcmTable(ruleRequest);
//        mockChessTable(ruleRequest);
//        mockWbcTable(ruleRequest);
        return ruleRequest;
    }

    private static void mockUdf10(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo table1 = getUdf(ruleRequest.getDimensionID(), "table1");
        TableInfo table2 = getUdf(ruleRequest.getDimensionID(), "table2");
        TableInfo table3 = getUdf(ruleRequest.getDimensionID(), "table3");
        TableInfo table4 = getUdf(ruleRequest.getDimensionID(), "table4");
        TableInfo table5 = getUdf(ruleRequest.getDimensionID(), "table5");
        TableInfo table6 = getUdf(ruleRequest.getDimensionID(), "table6");
        TableInfo table7 = getUdf(ruleRequest.getDimensionID(), "table7");
        TableInfo table8 = getUdf(ruleRequest.getDimensionID(), "table8");
        TableInfo table9 = getUdf(ruleRequest.getDimensionID(), "table9");
        TableInfo table10 = getUdf(ruleRequest.getDimensionID(), "table10");

        SetTable1Fk(table1);
        SetTable5Fk(table5);

        List<TableInfo> listable = new ArrayList<>();
        listable.add(table1);
        listable.add(table2);
        listable.add(table3);
        listable.add(table4);
        listable.add(table5);
        listable.add(table6);
        listable.add(table7);
        listable.add(table8);
        listable.add(table9);
        listable.add(table10);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static void SetTable5Fk(TableInfo table5) {
        List<ColumnInfo> list1 = table5.getColumnList();
        for (int i = 0; i < list1.size(); i++) {
            ColumnInfo c1 = list1.get(i);
            if (c1.getColumnName().equals("attr1")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("attr1");
                fkMsg.setFkTableName("table6");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
        }
    }

    private static void mockUdf8(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo table1 = getUdf(ruleRequest.getDimensionID(), "table1");
        TableInfo table2 = getUdf(ruleRequest.getDimensionID(), "table2");
        TableInfo table3 = getUdf(ruleRequest.getDimensionID(), "table3");
        TableInfo table4 = getUdf(ruleRequest.getDimensionID(), "table4");
        TableInfo table5 = getUdf(ruleRequest.getDimensionID(), "table5");
        TableInfo table7 = getUdf(ruleRequest.getDimensionID(), "table7");
        TableInfo table8 = getUdf(ruleRequest.getDimensionID(), "table8");
        TableInfo table10 = getUdf(ruleRequest.getDimensionID(), "table10");

        SetTable1Fk(table1);

        List<TableInfo> listable = new ArrayList<>();
        listable.add(table1);
        listable.add(table2);
        listable.add(table3);
        listable.add(table4);
        listable.add(table5);
        listable.add(table7);
        listable.add(table8);
        listable.add(table10);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static void SetTable1Fk(TableInfo table1) {
        List<ColumnInfo> list1 = table1.getColumnList();
        for (int i = 0; i < list1.size(); i++) {
            ColumnInfo c1 = list1.get(i);
            if (c1.getColumnName().equals("attr1")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("attr2");
                fkMsg.setFkTableName("table2");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
        }
    }

    private static void mockUdf6(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo table1 = getUdf(ruleRequest.getDimensionID(), "table1");
        TableInfo table3 = getUdf(ruleRequest.getDimensionID(), "table3");
        TableInfo table4 = getUdf(ruleRequest.getDimensionID(), "table4");
        TableInfo table10 = getUdf(ruleRequest.getDimensionID(), "table10");
        TableInfo table5 = getUdf(ruleRequest.getDimensionID(), "table5");
        TableInfo table7 = getUdf(ruleRequest.getDimensionID(), "table7");

        List<TableInfo> listable = new ArrayList<>();
        listable.add(table1);
        listable.add(table3);
        listable.add(table4);
        listable.add(table10);
        listable.add(table5);
        listable.add(table7);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static void mockUdf4(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo table3 = getUdf(ruleRequest.getDimensionID(), "table3");
        TableInfo table10 = getUdf(ruleRequest.getDimensionID(), "table10");
        TableInfo table5 = getUdf(ruleRequest.getDimensionID(), "table5");
        TableInfo table7 = getUdf(ruleRequest.getDimensionID(), "table7");

        List<TableInfo> listable = new ArrayList<>();
        listable.add(table3);
        listable.add(table10);
        listable.add(table5);
        listable.add(table7);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static void mockUdf2(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo table3 = getUdf(ruleRequest.getDimensionID(), "table3");
        TableInfo table10 = getUdf(ruleRequest.getDimensionID(), "table10");
        List<TableInfo> listable = new ArrayList<>();
        listable.add(table3);
        listable.add(table10);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getUdf(String path, String tableName) {
        TableInfo table1 = new TableInfo();
        table1.setTableName(tableName);
        table1.setTableDataPath(path + "/" + tableName + ".csv");

        String header = "attr1,attr2,attr3,attr4,attr5,attr6,attr7,attr8";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockTax10w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getTax10w();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);
        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/"+ruleRequest.getTaskId() +"/result.ree");
    }

    private static TableInfo getTax10w() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tax10w");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/tax_10w.csv");

        String header = "fname,lname,gender,areacode,phone,city,state,zip,maritalstatus,haschild,salary,rate,singleexemp,marriedexemp,childexemp";
//        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,double,int,int,int";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";
        constructTable(table1, header, type);
        return table1;
    }

    private static void mockTax100w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getTax100w();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getTax100w() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tax100w");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/tax_100w.csv");

        String header = "fname,lname,gender,areacode,phone,city,state,zip,maritalstatus,haschild,salary,rate,singleexemp,marriedexemp,childexemp";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,double,int,int,int";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockTaxSampling(RuleDiscoverExecuteRequest ruleRequest, String[] options, int size) {
        TableInfos tables = new TableInfos();
        TableInfo tax = getTaxSampling(options, size);
        List<TableInfo> listable = new ArrayList<>();
        listable.add(tax);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result_" + generateDataMark(options) + ".ree");
    }

    private static TableInfo getTaxSampling(String[] options, int size) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tax" + size + "w");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/" + "tax_" + size + "w__" + generateDataMark(options) + ".csv");

        String header = "fname,lname,gender,areacode,phone,city,state,zip,maritalstatus,haschild,salary,rate,singleexemp,marriedexemp,childexemp";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,double,int,int,int";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockTax1000w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getTax1000w();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getTax1000w() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tax1000w");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/tax_1000w.csv");

        String header = "fname,lname,gender,areacode,phone,city,state,zip,maritalstatus,haschild,salary,rate,singleexemp,marriedexemp,childexemp";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,double,int,int,int";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockTax200w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getTax200w();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getTax200w() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tax200w");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/tax_200w.csv");

        String header = "fname,lname,gender,areacode,phone,city,state,zip,maritalstatus,haschild,salary,rate,singleexemp,marriedexemp,childexemp";
//        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,double,int,int,int";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";
        constructTable(table1, header, type);
        return table1;
    }

    private static void mockTax400w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getTax400w();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getTax400w() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tax400w");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/tax_400w.csv");

        String header = "fname,lname,gender,areacode,phone,city,state,zip,maritalstatus,haschild,salary,rate,singleexemp,marriedexemp,childexemp";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,double,int,int,int";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockTax600w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getTax600w();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getTax600w() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tax600w");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/tax_600w.csv");

        String header = "fname,lname,gender,areacode,phone,city,state,zip,maritalstatus,haschild,salary,rate,singleexemp,marriedexemp,childexemp";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,double,int,int,int";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockTax800w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getTax800w();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getTax800w() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tax800w");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/tax_800w.csv");

        String header = "fname,lname,gender,areacode,phone,city,state,zip,maritalstatus,haschild,salary,rate,singleexemp,marriedexemp,childexemp";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,double,int,int,int";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockOrder(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo hepatitis = getOrder();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(hepatitis);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }


    private static TableInfo getOrder() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("order");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/order_processed.csv");

        String header = "id,distributeno,sellerid,ticketid,bizid,sellernickname,pen,receivername,receivermobile,receiveraddress,geohash,classify";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockOrder10w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo hepatitis = getOrder10w();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(hepatitis);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }


    private static TableInfo getOrder10w() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("order");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/order_processed_10w.csv");

        String header = "id,distributeno,sellerid,ticketid,bizid,sellernickname,pen,receivername,receivermobile,receiveraddress,geohash,classify";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockOrder1q(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo hepatitis = getOrder1q();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(hepatitis);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }


    private static TableInfo getOrder1q() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("order");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/order_processed_1q.csv");

        String header = "id,distributeno,sellerid,ticketid,bizid,sellernickname,pen,receivername,receivermobile,receiveraddress,geohash,classify";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockOrder100w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo hepatitis = getOrderByNum("100w");
        List<TableInfo> listable = new ArrayList<>();
        listable.add(hepatitis);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static void mockOrder200w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo hepatitis = getOrderByNum("200w");
        List<TableInfo> listable = new ArrayList<>();
        listable.add(hepatitis);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }


    private static TableInfo getOrderByNum(String num) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("order");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/order_processed_" + num + ".csv");

        String header = "id,distributeno,sellerid,ticketid,bizid,sellernickname,pen,receivername,receivermobile,receiveraddress,geohash,classify";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockShaidi(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo hepatitis = getShaidi();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(hepatitis);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }


    private static TableInfo getShaidi() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("shaidi");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/shaidi_month.csv");

        String header = "platform,date,number_of_platform_device_connections,number_of_industrial_models,number_of_platform_microservices,number_of_industrial_apps,number_of_apps_developed_based_on_the_platform,number_of_cloud_software,number_of_general_industrial_apps,light_industrial_equipment_connection,mining_equipment_connection,agricultural_and_forestry_machinery_connected,building_materials_equipment_connection,instrumentation_connection,transportation_equipment_connection,chemical_equipment_connection,power_equipment_connection,other_connection,smelting_equipment_connection,electrical_and_electronic_equipment_connection,construction_machinery_connection,logistics_equipment_connection,machine_tool_connection,robot_connect,driver_equipment_connection,henan,heilongjiang,xinjiang,gansu,tianjin,yunnan,guangdong,hunan,fujian,sichuan,liaoning,shaanxi,jiangsu,qinghai,shanghai,shandong,hebei,guangxi,beijing,jiangxi,zhejiang,hubei,ningxia,guizhou,anhui,neimongolia,tibet,shanxi,chongqing,hainan,jilin,data_algorithm_model,rd_simulation_model,industry_mechanism_model,business_process_model,safe_production,energy_conservation,quality_control,supply_chain_management,rd_design,manufacturing,operation_management,warehouse_logistics,operation_and_maintenance_service,mining,black_metal,non_ferrous_metals,petrochemical_industry,building_materials,medicine,textile,home_appliances,food,tobacco,light_industry,mechanical,car,aerospace,ship,rail,electronic,electricity_heat_and_gas,construction_industry,agriculture,service_industry,other";
        String type = "varchar(20),varchar(20),int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockShaidi_day(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo hepatitis = getShaidi_day();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(hepatitis);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }


    private static TableInfo getShaidi_day() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("shaidi");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/shaidi_day_data.csv");

        String header = "platform,date,actual_cloud_usage_of_the_platform,total_platform_data,daily_operation_quantity_of_equipment,microservice_calls,total_number_of_industrial_app_subscriptions,total_amount_of_industrial_app_subscription,daily_active_number_of_industrial_apps,number_of_platform_device_connections,light_industrial_equipment_operation,mining_equipment_operation,agricultural_and_forestry_machinery_operation,building_materials_equipment_operation,instrumentation_operation,transportation_equipment_operation,chemical_equipment_operation,power_equipment_operation1,other_operation,smelting_equipment_operation,electrical_and_electronic_equipment_operation,construction_machinery_operation,logistics_equipment_operation,machine_tool_operation,robot_running,power_equipment_operation,light_industrial_equipment_connection,mining_equipment_connection,agricultural_and_forestry_machinery_connected,building_materials_equipment_connection,instrumentation_connection,transportation_equipment_connection,chemical_equipment_connection,power_equipment_connection1,other_connection,smelting_equipment_connection,electrical_and_electronic_equipment_connection,construction_machinery_connection,logistics_equipment_connection,machine_tool_connection,robot_connect,power_equipment_connection,henan,heilongjiang,xinjiang,gansu,tianjin,yunnan,guangdong,hunan,fujian,sichuan,liaoning,shaanxi,jiangsu,qinghai,shanghai,shandong,hebei,guangxi,beijing,jiangxi,zhejiang,hubei,ningxia,guizhou,anhui,neimongo,tibet,shanxi,chongqing,hainan,jilin";
        String type = "varchar(20),varchar(20),int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockDeepMatcher(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo hepatitis = getDeepMatcher();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(hepatitis);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }


    private static TableInfo getDeepMatcher() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("deepmatch");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/testdata/eidmatch.csv");

        String header = "id,contrib_institution_display,contrib_country_bg,contrib_state_bg,contrib_city_bg,contrib_addr_line,eid,kid";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        for(ColumnInfo columnInfo: table1.getColumnList()) {
            if(!columnInfo.getColumnName().equals("eid")) {
                columnInfo.setTarget(false);
            }
        }
        return table1;
    }

    private static void mockCasorgan(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo casorgan = getCasorgan();
//        TableInfo casorgan = getCasorganaddr();
        for(ColumnInfo columnInfo : casorgan.getColumnList()) {
            if("contrib_institution_display".equals(columnInfo.getColumnName())){
                columnInfo.setModelId("001");
            }
        }

        List<TableInfo> listable = new ArrayList<>();
        listable.add(casorgan);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId());

        List<ModelPredicate> models = new ArrayList<>();
        List<ModelColumns> columns = new ArrayList<>();
        ModelColumns col = new ModelColumns();
        col.setTableName("casorgan");
        col.setColumnName("contrib_institution_display");
        columns.add(col);

        ModelPredicate model = SimilarAlgorithmsFactory.buildCosine(0.7);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.7);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaro(0.7);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildLevenshtein(0.7);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildCosine(0.8);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.8);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaro(0.8);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildLevenshtein(0.8);
        model.setColumns(columns);
        models.add(model);

//        ModelColumns col1 = new ModelColumns();
//        col1.setTableName("casorgan");
//        col1.setColumnName("contrib_addr_line");
//        columns = new ArrayList<>();
//        columns.add(col1);
//
//        model = SimilarAlgorithmsFactory.buildCosine(0.9);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildJaccard(0.9);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildJaro(0.9);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildLevenshtein(0.9);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildCosine(0.8);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildJaccard(0.8);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildJaro(0.8);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildLevenshtein(0.8);
//        model.setColumns(columns);
//        models.add(model);

        ruleRequest.setModelPredicats(models);

//        ErRuleConfig config = new ErRuleConfig();
//        config.setUse(true);
//        ruleRequest.setErRuleConfig(config);
//        ruleRequest.setSimilarityConfig(similarityConfig);
    }

    private static void mockCasorgan10w(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo casorgan = getCasorg10W();
        for(ColumnInfo columnInfo : casorgan.getColumnList()) {
            if("contrib_institution_display".equals(columnInfo.getColumnName())){
                columnInfo.setModelId("001");
            }
        }

        List<TableInfo> listable = new ArrayList<>();
        listable.add(casorgan);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId());

        List<ModelPredicate> models = new ArrayList<>();
        ruleRequest.setModelPredicats(models);
        List<ModelColumns> columns = new ArrayList<>();
        ModelColumns col = new ModelColumns();
        col.setTableName("casorg_10w");
        col.setColumnName("contrib_institution_display");
        columns.add(col);

        ModelPredicate model = SimilarAlgorithmsFactory.buildCosine(0.85);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.85);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaro(0.85);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildLevenshtein(0.85);
        model.setColumns(columns);
        models.add(model);

        model = MlFactory.buildSentenceBertSingle();
        model.setColumns(columns);
        models.add(model);


        /** name字段配置 **/
        List<ModelColumns> columns1 = new ArrayList<>();
        ModelColumns col1 = new ModelColumns();
        col1.setTableName("casorg_10w");
        col1.setColumnName("name");
        columns1.add(col);

        model = SimilarAlgorithmsFactory.buildCosine(0.85);
        model.setColumns(columns1);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.85);
        model.setColumns(columns1);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaro(0.85);
        model.setColumns(columns1);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildLevenshtein(0.85);
        model.setColumns(columns1);
        models.add(model);

//        ErRuleConfig config = new ErRuleConfig();
//        config.setUse(true);
//        ruleRequest.setErRuleConfig(config);
//        ruleRequest.setSimilarityConfig(similarityConfig);
    }

    private static void mockCasorgcn(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo casorgan = getCasorgcn();
//        TableInfo casorgan = getCasorganaddr();
        for(ColumnInfo columnInfo : casorgan.getColumnList()) {
            if("contrib_institution_display".equals(columnInfo.getColumnName())){
                columnInfo.setModelId("001");
            }
        }

        List<TableInfo> listable = new ArrayList<>();
        listable.add(casorgan);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/ruleresult");

        List<ModelPredicate> models = new ArrayList<>();
        List<ModelColumns> columns = new ArrayList<>();
        ModelColumns col = new ModelColumns();
        col.setTableName("casorgcn");
        col.setColumnName("contrib_institution_display");
        columns.add(col);

        ModelPredicate model = SimilarAlgorithmsFactory.buildCosine(0.6);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.6);
        model.setColumns(columns);
        models.add(model);

//        model = SimilarAlgorithmsFactory.buildJaro(0.9);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildLevenshtein(0.9);
//        model.setColumns(columns);
//        models.add(model);

        model = SimilarAlgorithmsFactory.buildCosine(0.6);
        model.setColumns(columns);
        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildJaccard(0.8);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildJaro(0.8);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildLevenshtein(0.8);
//        model.setColumns(columns);
//        models.add(model);

//        model = MlFactory.buildSentenceBertSingle();
//        model.setColumns(columns);
//        models.add(model);

        ruleRequest.setModelPredicats(models);

//        ErRuleConfig config = new ErRuleConfig();
//        config.setUse(true);
//        config.setLabellingFilePath("hdfs:///tmp/yuxiang/testdata/labeledcn.csv");
//        config.setEidName("cas");
//        ruleRequest.setErRuleConfig(config);

    }

    private static void mockCasorgClean(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo casorgan = getCasorgClean();

        for(ColumnInfo columnInfo : casorgan.getColumnList()) {
            if("row_id".equals(columnInfo.getColumnName())){
                columnInfo.setSkip(true);
            }
        }

        List<TableInfo> listable = new ArrayList<>();
        listable.add(casorgan);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/ruleresult");

        List<ModelPredicate> models = new ArrayList<>();
        List<ModelColumns> columns = new ArrayList<>();
        ModelColumns col = new ModelColumns();
        col.setTableName("casorgan");
        col.setColumnName("name");
        columns.add(col);

        ModelPredicate model = SimilarAlgorithmsFactory.buildCosine(0.9);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.9);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaro(0.9);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildLevenshtein(0.9);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildCosine(0.8);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.8);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaro(0.8);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildLevenshtein(0.8);
        model.setColumns(columns);
        models.add(model);

        ModelColumns col1 = new ModelColumns();
        col1.setTableName("casorgan");
        col1.setColumnName("company");
        columns = new ArrayList<>();
        columns.add(col1);

        model = SimilarAlgorithmsFactory.buildCosine(0.9);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.9);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaro(0.9);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildLevenshtein(0.9);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildCosine(0.8);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.8);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaro(0.8);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildLevenshtein(0.8);
        model.setColumns(columns);
        models.add(model);
//
        ruleRequest.setModelPredicats(models);
//
//        ErRuleConfig config = new ErRuleConfig();
//        config.setUse(true);
//        config.setLabellingFilePath("hdfs:///tmp/yuxiang/testdata/labelednew.csv");
//        config.setEidName("ncas");
//        ruleRequest.setErRuleConfig(config);
    }



    private static TableInfo getCasorgan() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("casorgan");
        table1.setTableDataPath("hdfs:///user/suyanghua/csv/casorg/casorg_10w.csv");

        String header = "contrib_institution_display,contrib_state_bg,contrib_city_bg,contrib_addr_line,contrib_postal_code,uuid,row_id";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getCasorg10W() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("casorg_10w");
        table1.setTableDataPath("hdfs:///user/suyanghua/csv/casorg/casorg_10w.csv");

        String header = "contrib_institution_display,provi,city,addr,postcod,uuid,area,company,number,name";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getCasorgClean() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("casorgan");
//        table1.setTableDataPath("hdfs:///tmp/yuxiang/testdata/casorgcncleaned.csv");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/testdata/newcassimilarcnnlp.csv");

//        String header = "row_id,name,ori_name,alias,country,address,append_address";
//        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";
        String header = "province,city,postcode,uid,contrib_institution_display,address,area,company,number,name,post,prov,cit";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockStar(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo star = getStar();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(star);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }


    private static TableInfo getStar() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("star");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/testdata/star.csv");

        String header = "id,film_zh,production_company,issuing_company,production_area,director,screenwriter,producer,birthplace,constellation,blood_type,occupation,role,star_zh,birthplace1,constellation1,blood_type1,occupation1";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static TableInfo getCasorgcn() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("casorgcn");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/testdata/casorgcn.csv");

        String header = "contrib_institution_display,contrib_country_bg,contrib_state_bg,contrib_city_bg,contrib_addr_line,contrib_postal_code,row_id,display1,display2";//, addr1,addr2,display1,display2,post1,uuid";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";//,varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getCasorganaddr() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("casorgan");
        table1.setTableDataPath("hdfs:///tmp/yuxiang/testdata/casorgaddr.csv");

        String header = "contrib_institution_display,contrib_state_bg,contrib_city_bg,contrib_addr_line,contrib_postal_code,uuid,row_id, addr1,addr2,display1,display2,post1";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockHepatitis(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo hepatitis = getHepatitis();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(hepatitis);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getHepatitis() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("hepatitis");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/hepatitis.csv");

        String header = "attr1,attr2,attr3,attr4,attr5,attr6,attr7,attr8,attr9,attr10,attr11,attr12,attr13,attr14,attr15,attr16,attr17,attr18,attr19,attr20";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockNcvoter(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo chess = getNcvoter();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(chess);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getNcvoter() {
        TableInfo table1 = new TableInfo();
        //table1.setTableName("ncvoter_1001r_19c");
        //table1.setTableDataPath("hdfs:///tmp/zhangjun/ncvoter_1001r_19c.csv");
        table1.setTableName("ncvoter");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/ncvoter.csv");

        // String header = "voter_id,voter_reg_num,name_prefix,first_name,middle_name,last_name,name_suffix,age,gender,race,ethnic,street_address,city,state,zip_code,full_phone_num,birth_place,register_date,download_month";
        // String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        String header = "id,date,election_phase,way_of_voting,voting_intention,party,city_id,city,county_id,county_desc,city_id2,city_id3";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

//        String header = "middle_name,last_name,age,street_address";
//        String type = "varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockNcvoterSampling(RuleDiscoverExecuteRequest ruleRequest, String[] options) {
        TableInfos tables = new TableInfos();
        TableInfo chess = getNcvoterSampling(options);
        List<TableInfo> listable = new ArrayList<>();
        listable.add(chess);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result_" + generateDataMark(options) + ".ree");
    }

    private static TableInfo getNcvoterSampling(String[] options) {
        TableInfo table1 = new TableInfo();
        //table1.setTableName("ncvoter_1001r_19c");
        //table1.setTableDataPath("hdfs:///tmp/zhangjun/ncvoter_1001r_19c.csv");
        table1.setTableName("ncvoter");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/ncvoter__" + generateDataMark(options) + ".csv");

        // String header = "voter_id,voter_reg_num,name_prefix,first_name,middle_name,last_name,name_suffix,age,gender,race,ethnic,street_address,city,state,zip_code,full_phone_num,birth_place,register_date,download_month";
        // String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        String header = "id,date,election_phase,way_of_voting,voting_intention,party,city_id,city,county_id,county_desc,city_id2,city_id3";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

//        String header = "middle_name,last_name,age,street_address";
//        String type = "varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockInspection(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo inspect = getInspection();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(inspect);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("inspection");
//        col.setColumnName("DBA_Name");
//        col.setColumnName("Address");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildJaro(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static TableInfo getInspection() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("inspection");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/inspection.csv");

        String header = "Inspection_ID,DBA_Name,AKA_Name,License,Facility_Type,Risk,Address,City,State,Zip,Inspection_Date,Inspection_Type,Results,Violations,Latitude,Longitude,Location";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockInspection_ml(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo inspect = getInspection_ml();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(inspect);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("inspection");
//        col.setColumnName("DBA_Name");
//        col.setColumnName("Address");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildJaro(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static TableInfo getInspection_ml() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("inspection_ml");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/inspection_ml.csv");

        String header = "Inspection_ID,DBA_Name,AKA_Name,License,Facility_Type,Risk,Address,City,State,Zip,Inspection_Date,Inspection_Type,Results,Violations,Latitude,Longitude,Location,Historical_Wards_2003_2015,Zip_Codes,Community_Areas,Census_Tracts,Wards,Address_ml";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20), varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockInspectionSampling(RuleDiscoverExecuteRequest ruleRequest, String[] options) {
        TableInfos tables = new TableInfos();
        TableInfo inspect = getInspectionSampling(options);
        List<TableInfo> listable = new ArrayList<>();
        listable.add(inspect);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result_" + generateDataMark(options) + ".ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("inspection");
//        col.setColumnName("DBA_Name");
//        col.setColumnName("Address");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildJaro(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static TableInfo getInspectionSampling(String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("inspection");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/inspection__" + generateDataMark(options) + ".csv");

        String header = "Inspection_ID,DBA_Name,AKA_Name,License,Facility_Type,Risk,Address,City,State,Zip,Inspection_Date,Inspection_Type,Results,Violations,Latitude,Longitude,Location,Historical_Wards_2003_2015,Zip_Codes,Community_Areas,Census_Tracts,Wards";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockChess(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo chess = getChess();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(chess);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getChess() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("chess");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/chess.csv");

        String header = "white_king_file,white_king_rank,white_rook_file,white_rook_rank,black_king_file,black_king_rank,optimal_depth_of_win";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockFlight(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo flight = getFlight();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(flight);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getFlight() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("flight");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/flight_1k.csv");

        String header = "Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier,AirlineID,Carrier,TailNum,FlightNum,OriginAirportID,OriginAirportSeqID,OriginCityMarketID,Origin,OriginCityName,OriginState,OriginStateFips,OriginStateName,OriginWac,DestAirportID,DestAirportSeqID,DestCityMarketID,Dest,DestCityName,DestState,DestStateFips,DestStateName,DestWac,CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes,ArrDel15,ArrivalDelayGroups,ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Flights,Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,FirstDepTime,TotalAddGTime,LongestAddGTime,DivAirportLandings,DivReachedDest,DivActualElapsedTime,DivArrDelay,DivDistance,Div1Airport,Div1AirportID,Div1AirportSeqID,Div1WheelsOn,Div1TotalGTime,Div1LongestGTime,Div1WheelsOff,Div1TailNum,Div2Airport,Div2AirportID,Div2AirportSeqID,Div2WheelsOn,Div2TotalGTime,Div2LongestGTime,Div2WheelsOff,Div2TailNum,Div3Airport,Div3AirportID,Div3AirportSeqID,Div3WheelsOn,Div3TotalGTime,Div3LongestGTime,Div3WheelsOff,Div3TailNum,Div4Airport,Div4AirportID,Div4AirportSeqID,Div4WheelsOn,Div4TotalGTime,Div4LongestGTime,Div4WheelsOff,Div4TailNum,Div5Airport,Div5AirportID,Div5AirportSeqID,Div5WheelsOn,Div5TotalGTime,Div5LongestGTime,Div5WheelsOff,Div5TailNum";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockHospital(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getHospital();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("hospital");
//        col.setColumnName("Measure_Name");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static TableInfo getHospital() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("hospital");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/hospital.csv");

        String header = "Provider_Number,Hospital_Name,City,State,ZIP_Code,County_Name,Phone_Number,Hospital_Type,Hospital_Owner,Emergency_Service,Condition,Measure_Code,Measure_Name,Sample,StateAvg";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockHospital_ml(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getHospital_ml();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("hospital");
//        col.setColumnName("Measure_Name");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static TableInfo getHospital_ml() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("hospital_ml");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/hospital_ml.csv");

        String header = "Provider_Number,Hospital_Name,City,State,ZIP_Code,County_Name,Phone_Number,Hospital_Type,Hospital_Owner,Emergency_Service,Condition,Measure_Code,Measure_Name,Sample,StateAvg,Measure_Name_ml";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockHospitalSampling(RuleDiscoverExecuteRequest ruleRequest, String[] options) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getHospitalSampling(options);
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result_" + generateDataMark(options) + ".ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("hospital");
//        col.setColumnName("Measure_Name");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static TableInfo getHospitalSampling(String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("hospital");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/hospital__" + generateDataMark(options) + ".csv");

        String header = "Provider_Number,Hospital_Name,City,State,ZIP_Code,County_Name,Phone_Number,Hospital_Type,Hospital_Owner,Emergency_Service,Condition,Measure_Code,Measure_Name,Sample,StateAvg";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    /*
        flexible Sample data
        ONLY for one .csv file dataset
        dataOption: DATANAME__SampleOption__ROUND
     */

    private static void mockSampleData(RuleDiscoverExecuteRequest ruleRequest, String dataOption) {
        String[] options = dataOption.split("__");
        String dataName = options[0];
        if (dataName.equals("airports")) {
            mockAirportsSampling(ruleRequest, options);
        } else if (dataName.equals("hospital")) {
            mockHospitalSampling(ruleRequest, options);
        } else if (dataName.equals("inspection")) {
            mockInspectionSampling(ruleRequest, options);
        } else if (dataName.equals("ncvoter")) {
            mockNcvoterSampling(ruleRequest, options);
        } else if (dataName.equals("tax200w")) {
            mockTaxSampling(ruleRequest, options, 200);
        } else if (dataName.equals("tax400w")) {
            mockTaxSampling(ruleRequest, options, 400);
        } else if (dataName.equals("tax600w")) {
            mockTaxSampling(ruleRequest, options, 600);
        } else if (dataName.equals("tax800w")) {
            mockTaxSampling(ruleRequest, options, 800);
        } else if (dataName.equals("tax1000w")) {
            mockTaxSampling(ruleRequest, options, 1000);
        } else if (dataName.equals("property")) {
            mockProperty(ruleRequest, options);
        }
    }

    private static String generateDataMark(String[] options) {
        return options[1] + "__" + options[2] + "__" + options[3];
    }

    private static void mockAirports(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getAirports();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("airports");
//        col.setColumnName("name");
//        columns.add(col);

//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.9);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);
    }

    private static TableInfo getAirports() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("airports");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/airports.csv");

        String header = "id,ident,type,name,latitude_deg,longitude_deg,elevation_ft,continent,iso_country,iso_region,municipality,scheduled_service,gps_code,iata_code,local_code,home_link,wikipedia_link,keywords";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),double,double,int,varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockAirports_ml(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getAirports_ml();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("airports");
//        col.setColumnName("name");
//        columns.add(col);

//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.9);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);
    }

    private static TableInfo getAirports_ml() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("airports_ml");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/airports_ml.csv");

        String header = "id,ident,type,name,latitude_deg,longitude_deg,elevation_ft,continent,iso_country,iso_region,municipality,scheduled_service,gps_code,iata_code,local_code,home_link,wikipedia_link,keywords,name_ml";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),double,double,int,varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockAirportsSampling(RuleDiscoverExecuteRequest ruleRequest, String[] options) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getAirportsSampling(options);
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result_" + generateDataMark(options) + ".ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("airports");
//        col.setColumnName("name");
//        columns.add(col);

//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.9);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);
    }

    private static TableInfo getAirportsSampling(String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("airports");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/airports__" + generateDataMark(options) + ".csv");

        String header = "id,ident,type,name,latitude_deg,longitude_deg,elevation_ft,continent,iso_country,iso_region,municipality,scheduled_service,gps_code,iata_code,local_code,home_link,wikipedia_link,keywords";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),double,double,int,varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockAcmTableAB(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo tableA = getTableA();
        TableInfo tableB = getTableB();

        List<TableInfo> listable = new ArrayList<>();
        listable.add(tableA);
        listable.add(tableB);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getTableA() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tableA");
        table1.setTableDataPath("hdfs:///tmp/wanjia/tableA.csv");

        String header = "id,title,authors,venues,year,eid";
        String type = "varchar(20),varchar(100),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getTableB() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tableB");
        table1.setTableDataPath("hdfs:///tmp/wanjia/tableB.csv");

        String header = "id,title,authors,venues,year,eid";
        String type = "varchar(20),varchar(100),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockAcmTableA(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getAcmTableA();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static TableInfo getAcmTableA() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("tableA");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/tableA.csv");

        String header = "id,title,authors,venue,year";
        String type = "varchar(20),varchar(100),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockAdults(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getAdults();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("adult");
//        col.setColumnName("occupation");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static TableInfo getAdults() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("adult");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/adult_data.csv");

        String header = "age,workclass,fnlwgt,education,education_num,marital_status,occupation,relationship,race,sex,capital_gain,capital_loss,hours_per_week,native_country,class";
//        String type = "int,varchar(20),int,varchar(20),int,varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,int,int,varchar(20),varchar(20)";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";
        constructTable(table1, header, type);

        return table1;
    }

    private static void mockAdults_ml(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getAdults_ml();
        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");

        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("adult_ml");
//        col.setColumnName("occupation");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildLevenshtein(0.97);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static TableInfo getAdults_ml() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("adult_ml");
        table1.setTableDataPath("hdfs:///tmp/zhangjun/adult_data_ml.csv");

        String header = "age,workclass,fnlwgt,education,education_num,marital_status,occupation,relationship,race,sex,capital_gain,capital_loss,hours_per_week,native_country,class,occupation_ml";
//        String type = "int,varchar(20),int,varchar(20),int,varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),int,int,int,varchar(20),varchar(20)";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";
        constructTable(table1, header, type);

        return table1;
    }

    private static void mockIMDB(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo nameBasics = getNameBasics(ruleRequest.getDimensionID());
        TableInfo titleAkas = getTitleAkas(ruleRequest.getDimensionID());
        TableInfo titleBasics = getTitleBasics(ruleRequest.getDimensionID());
//        TableInfo titleCrew = getTitleCrew();
        TableInfo titleEpisode = getTitleEpisode(ruleRequest.getDimensionID());
        TableInfo titlePrincipals = getTitlePrincipals(ruleRequest.getDimensionID());
        TableInfo titleRatings = getTitleRatings(ruleRequest.getDimensionID());

        List<TableInfo> listable = new ArrayList<>();
        listable.add(nameBasics);
        listable.add(titleAkas);
        listable.add(titleBasics);
//        listable.add(titleCrew);
        listable.add(titleEpisode);
        listable.add(titlePrincipals);
        listable.add(titleRatings);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }


    private static TableInfo getNameBasics(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("name_basics");
        table1.setTableDataPath(path + "/name_basics.csv");

        String header = "nconst,primaryName,birthYear,deathYear,primaryProfession,knownForTitles";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getTitleAkas(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("title_akas");
        table1.setTableDataPath(path + "/title_akas.csv");

        String header = "titleId,ordering,title,region,language,types,attributes,isOriginalTitle";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        constructTable(table1, header, type);

        return table1;
    }

    private static TableInfo getTitleBasics(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("title_basics");
        table1.setTableDataPath(path + "/title_basics.csv");

        String header = "tconst,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes,genres";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getTitleCrew() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("title_crew");
        table1.setTableDataPath("hdfs:///tmp/imdb/title_crew.csv");
        table1.setDropUselessData(true);

        String header = "tconst,directors,writers";
        String type = "varchar(20),varchar(20),varchar(20)";

        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        if (colNames.length != colTypes.length) {
            throw new RuntimeException();
        }

        List<ColumnInfo> list1 = new ArrayList<>();
        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("tconst")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("titleId");
                fkMsg.setFkTableName("title_akas");
                fkMsgList.add(fkMsg);
                FKMsg fkMsg1 = new FKMsg();
                fkMsg1.setFkColumnName("tconst");
                fkMsg1.setFkTableName("title_basics");
                fkMsgList.add(fkMsg1);
                c1.setFkMsgList(fkMsgList);
            }
            if (c1.getColumnName().equals("directors")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("nconst");
                fkMsg.setFkTableName("name_basics");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);
        return table1;
    }

    private static TableInfo getTitleEpisode(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("title_episode");
        table1.setTableDataPath(path + "/title_episode.csv");
        table1.setDropUselessData(true);

        String header = "tconst,parentTconst,seasonNumber,episodeNumber";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20)";


        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        if (colNames.length != colTypes.length) {
            throw new RuntimeException();
        }

        List<ColumnInfo> list1 = new ArrayList<>();
        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("tconst")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("titleId");
                fkMsg.setFkTableName("title_akas");
                fkMsgList.add(fkMsg);
                FKMsg fkMsg1 = new FKMsg();
                fkMsg1.setFkColumnName("tconst");
                fkMsg1.setFkTableName("title_basics");
                fkMsgList.add(fkMsg1);
                c1.setFkMsgList(fkMsgList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);
        return table1;
    }

    private static TableInfo getTitlePrincipals(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("title_principals");
        table1.setTableDataPath(path + "/title_principals.csv");
        table1.setDropUselessData(true);

        String header = "tconst,ordering,nconst,category,job,characters";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        if (colNames.length != colTypes.length) {
            throw new RuntimeException();
        }

        List<ColumnInfo> list1 = new ArrayList<>();
        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("tconst")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("titleId");
                fkMsg.setFkTableName("title_akas");
                fkMsgList.add(fkMsg);
                FKMsg fkMsg1 = new FKMsg();
                fkMsg1.setFkColumnName("tconst");
                fkMsg1.setFkTableName("title_basics");
                fkMsgList.add(fkMsg1);
                c1.setFkMsgList(fkMsgList);
            }
            if (c1.getColumnName().equals("nconst")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("nconst");
                fkMsg.setFkTableName("name_basics");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);
        return table1;
    }

    private static TableInfo getTitleRatings(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("title_ratings");
        table1.setTableDataPath(path + "/title_ratings.csv");
        table1.setDropUselessData(true);

        String header = "tconst,averageRating,numVotes";
        String type = "varchar(20),varchar(20),varchar(20)";

        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        if (colNames.length != colTypes.length) {
            throw new RuntimeException();
        }

        List<ColumnInfo> list1 = new ArrayList<>();
        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("tconst")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("titleId");
                fkMsg.setFkTableName("title_akas");
                fkMsgList.add(fkMsg);
                FKMsg fkMsg1 = new FKMsg();
                fkMsg1.setFkColumnName("tconst");
                fkMsg1.setFkTableName("title_basics");
                fkMsgList.add(fkMsg1);
                c1.setFkMsgList(fkMsgList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);
        return table1;
    }


    private static void mockTwoTableJoin(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();
        TableInfo product = getProduct();
        TableInfo shop = getShop();

        List<TableInfo> listable = new ArrayList<>();
        listable.add(product);
        listable.add(shop);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/"+ruleRequest.getTaskId() +"/result.ree");
    }

    private static TableInfo getShop() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("shop");
        table1.setTableDataPath("hdfs:///tmp/small3/shop.csv");

        String header = "id,name,date_created,on_sale_product";
        String type = "VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getProduct() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("product");
//        table1.setTableDataPath("hdfs:///tmp/small2/author2paper.csv");
        table1.setTableDataPath("hdfs:///tmp/small3/product.csv");

        String header = "id,seller,description,weight,type,price,online_year";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        List<ColumnInfo> list1 = new ArrayList<>();
        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("seller")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("id");
                fkMsg.setFkTableName("shop");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);

        return table1;
    }


    private static void mock1MillionMutiTable(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();

//        TableInfo table1 = getWanyiT1();
//        TableInfo table2 = getWanyiT2();
//        TableInfo table3 = getWanyiT3();
//        TableInfo table4 = getWanyiT4();
        TableInfo table1 = getTable1();
//        TableInfo table2 = getTable2();
//        TableInfo testPaper = getTestPaper();
//        TableInfo propertyBasic = getPropertyBasic();
//        TableInfo propertyAddress = getPropertyAddress();
//        TableInfo propertyFeatures = getPropertyFeatures();
//        TableInfo picture = getPicture();
//        TableInfo Mapping_SA1 = getMapping_SA1();
//        TableInfo SA1_Satistics = getSA1_Satistics();
//        TableInfo School = getSchool();
//        TableInfo SchoolRanking = getSchoolRanking();
//        TableInfo Mapping_School = getMapping_School();
//        TableInfo Train_Station = getTrain_Station();
//        TableInfo Train_Time = getTrain_Time();
//        TableInfo Mapping_Train_Station = getMapping_Train_Station();

        List<TableInfo> listable = new ArrayList<>();
//        listable.add(testPaper);
        listable.add(table1);
//        listable.add(table2);
//        listable.add(table3);
//        listable.add(table4);

//        listable.add(propertyBasic);
//        listable.add(propertyAddress);
//        listable.add(propertyFeatures);  //都是数字类型，并且都是0和1
//        listable.add(picture);
//        listable.add(Mapping_SA1);  //都是数字类型
//        listable.add(SA1_Satistics);  //都是数字类型
//        listable.add(School);
//        listable.add(SchoolRanking);
//        listable.add(Mapping_School); //都是数字类型
//        listable.add(Train_Station);
//        listable.add(Train_Time);  //都是数字类型
//        listable.add(Mapping_Train_Station); //都是数字类型

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("1399");
        ruleRequest.setResultStorePath("/tmp/rulefind/1399/result.ree");
        ruleRequest.setCr("0.000001");
        ruleRequest.setFtr("0.8");

    }

    private static TableInfo getTestPaper() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("relation");
        table1.setTableDataPath("hdfs:///tmp/small/relation.csv");

        String header = "cc,ac,pn,nm,str,ct,zip";
        String type = "VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getTable2() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("table2");
        table1.setTableDataPath("hdfs:///tmp/small/table2.csv");

        String header = "id,sn,name,city,zcode,addr,tel,birth,gender,product";
        String type = "VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),INTEGER,VARCHAR(20),INTEGER,VARCHAR" +
                "(20),VARCHAR(20),VARCHAR(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getTable1() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("table1");
        table1.setTableDataPath("hdfs:///tmp/small/table1.csv");

        String header = "id,sn,name,city,zcode,addr,tel,birth,gender,product";
        String type = "VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),INTEGER,VARCHAR(20),INTEGER,VARCHAR" +
                "(20),VARCHAR(20),VARCHAR(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getMapping_Train_Station() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Mapping_Train_Station");
        table1.setTableDataPath("hdfs:///tmp/milliondata/Mapping_Train_Station.csv");

        String header = "id,propertyID,stop_id,distance_text1,distance_value1,duration_text1,duration_value1";
        String type = "VARCHAR(20),VARCHAR(20),INT,VARCHAR(20),INT,VARCHAR(20),INT";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getTrain_Time() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Train_Time");
        table1.setTableDataPath("hdfs:///tmp/milliondata/Train_Time.csv");

        String header = "id,stop_ori,stop_des,avg_time,trans_flag";
        String type = "INT,INT,INT,INT,INT";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getTrain_Station() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Train_Station");
        table1.setTableDataPath("hdfs:///tmp/milliondata/Train_Station.csv");

        String header = "stop_id,stop_no,stop_short_name,stop_name,stop_lat,stop_lon";
        String type = "INT,VARCHAR(20),VARCHAR(20),VARCHAR(20),DOUBLE,DOUBLE";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getMapping_School() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Mapping_School");
        table1.setTableDataPath("hdfs:///tmp/milliondata/Mapping_School.csv");

        String header = "pro_ID,sec1,sec2";
        String type = "VARCHAR(20),VARCHAR(20),VARCHAR(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getSchoolRanking() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("School_Ranking");
        table1.setTableDataPath("hdfs:///tmp/milliondata/School_Ranking.csv");

        String header = "school_ID,oriName,Ranking,Locality,IB,Students_Enrolled_in_VCE,Median_VCE_score,ScoresGT40Percent";
        String type = "VARCHAR(20),VARCHAR(20),INT,VARCHAR(20),VARCHAR(20),INT,INT,DOUBLE";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getSchool() {

        TableInfo table1 = new TableInfo();
        table1.setTableName("School");
        table1.setTableDataPath("hdfs:///tmp/milliondata/School.csv");

        String header = "school_ID,name,gender,restrictedZone,type,school_lng,school_lat";
        String type = "VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),DOUBLE,DOUBLE";

        constructTable(table1, header, type);
        return table1;
    }

    private static void constructTable(TableInfo table1, String header, String type) {
        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");
        if (colNames.length != colTypes.length) {
            throw new RuntimeException("colName and colType length are different");
        }

        List<ColumnInfo> list1 = new ArrayList<>();

        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (colNames[i].equals("title")) {
                c1.setModelId(String.valueOf(i));
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);
    }

    private static TableInfo getSA1_Satistics() {

        TableInfo table1 = new TableInfo();
        table1.setTableName("SA1_Satistics");
        table1.setTableDataPath("hdfs:///tmp/milliondata/SA1_Satistics.csv");

        String header = "region_id,sa1_no,residents,Median_age,Median_total_personal_income_weekly,Birthplace_Australia_Persons,Birthplace_Australia_Persons_percentage,Language_spoken_at_home_English_only_Persons,English_Percentage,Australian_citizen_Persons,AU_percentage,Age_of_Persons_attending_an_educational_institution_25_years_and_over_Persons,Highest_year_of_school_completed_Year_12_or_equivalent_Persons,Median_rent_weekly";
        String type = "VARCHAR(20),VARCHAR(20),INT,INT,INT,INT,DOUBLE,INT,DOUBLE,INT,DOUBLE,INT,INT,INT";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getMapping_SA1() {

        TableInfo table1 = new TableInfo();
        table1.setTableName("Mapping_SA1");
        table1.setTableDataPath("hdfs:///tmp/milliondata/Mapping_SA1.csv");

        String header = "proID,sa1_no";
        String type = "VARCHAR(20),VARCHAR(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static TableInfo getPicture() {

        TableInfo table1 = new TableInfo();
        table1.setTableName("Picture");
        table1.setTableDataPath("hdfs:///tmp/milliondata/Picture.csv");

        String header = "proID,picNo,picAddr";
        String type = "VARCHAR(20),INT,VARCHAR(20)";

        constructTable(table1, header, type);
        return table1;
    }

//    private static TableInfo getPropertyFeatures() {
//
//        TableInfo table1 = new TableInfo();
//        table1.setTableName("Property_Feature");
//        table1.setTableDataPath("hdfs:///tmp/milliondata/Property_Features.csv");
//
//        String header = "ID,Air_Conditioning,Alarm,Balconey,BBQ,City_view,Close_to_Shops,Close_to_Transport,Close_to_Schools,courtyard,Dining_room,Dish_Washer,Ducted,Ensuite,Family_Room,Fireplace,Fully_Fenced,gas_heating,Gym,Heating,Intercom,Laundry,Mountain_Views,Park,Swimming_Pool,Renovated,River_Views,Rumpus_Room,Sauna,Study_room,Sun_Room,System_Heating,Tennis_Court,Water_Views,wordrobe,SUM";
//        String type = "VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20)";
//
//        constructTable(table1, header, type);
//        return table1;
//    }
//
//    private static TableInfo getPropertyAddress() {
//
//        TableInfo table1 = new TableInfo();
//        table1.setTableName("Property_Address");
//        table1.setTableDataPath("hdfs:///tmp/small/Property_Address.csv");
//
//        String header = "proID,Lat,Lng,Formated_Address,Locality,State,Postal_Code";
//        String type = "VARCHAR(20),DOUBLE,DOUBLE,VARCHAR(20),VARCHAR(20),VARCHAR(20),VARCHAR(20)";
//
//        constructTable(table1, header, type);
//        return table1;
//    }
//
//
//    private static TableInfo getPropertyBasic() {
//        TableInfo table1 = new TableInfo();
//        table1.setTableName("Propery_Basic");
//        table1.setTableDataPath("hdfs:///tmp/small/Propery_Basic.csv");
//
//        String header = "proID,address,price,bedroom,bathroom,parking,proType,sold_date,agency_name,agency_addr,des_head,des_content,features";
//        String type = "varchar(20),varchar(20),int,int,int,int,varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";
//
//        constructTable(table1, header, type);
//        return table1;
//    }


    private static void mockPaperAuthorSmall(RuleDiscoverExecuteRequest ruleRequest){
        TableInfos tables = new TableInfos();
        TableInfo author = getAuthor2Small();
        TableInfo paper = getPaper2Small();
        TableInfo a2p = getA2PSmall();

        List<TableInfo> listable = new ArrayList<>();
        listable.add(author);
        listable.add(paper);
        listable.add(a2p);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/"+ruleRequest.getTaskId() +"/result.ree");

    }

    private static void mockContact(RuleDiscoverExecuteRequest ruleRequest){
        TableInfos tables = new TableInfos();
//        TableInfo contact = getContact();
//        TableInfo content = getContent();
//        TableInfo action = getAction();
        TableInfo join = getContactJoin();

//        for(ColumnInfo columnInfo : contact.getColumnList()) {
//            if("visitor_id".equals(columnInfo.getColumnName())){
//                List<FKMsg> fkList = new ArrayList<>();
//                FKMsg fkMsg = new FKMsg();
//                fkMsg.setFkColumnName("visitor_id");
//                fkMsg.setFkTableName("action");
//                fkList.add(fkMsg);
//                columnInfo.setFkMsgList(fkList);
//            }
//        }

//        for(ColumnInfo columnInfo : action.getColumnList()) {
//            if("campaign_id".equals(columnInfo.getColumnName())){
//                List<FKMsg> fkList = new ArrayList<>();
//                FKMsg fkMsg = new FKMsg();
//                fkMsg.setFkColumnName("campaign_id");
//                fkMsg.setFkTableName("content");
//                fkList.add(fkMsg);
//                columnInfo.setFkMsgList(fkList);
//            }
//        }


        List<TableInfo> listable = new ArrayList<>();
//        listable.add(contact);
//        listable.add(content);
//        listable.add(action);
        listable.add(join);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/"+ruleRequest.getTaskId() +"/result.ree");
    }

    private static TableInfo getContact() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("contact");
        table1.setTableDataPath("/tmp/yuxiang/testdata/contact.csv");

        String header = "id,visitor_id,contact_id,first_name,last_name,phone,email,industry_lv1_code,industry_lv2_code,position,country,province,city,company,create_time";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getContent() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("content");
        table1.setTableDataPath("/tmp/yuxiang/testdata/content.csv");

        String header = "id,offer_id,offer_name,offer_type,offer_category,sub_offer_category,campaign_id,campaign_name,heat_base_line,start_time,end_time,platform_name,platform_type,probe_type,offer_config,create_user,create_time,update_user,update_time";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);

        return table1;
    }

    private static TableInfo getAction() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("action");
        table1.setTableDataPath("/tmp/yuxiang/testdata/action.csv");

        String header = "id,visitor_id,contact_id,campaign_id,offer_id,action_id,action_name,timestamp,start_time,end_time,duration,platform_type,platform_name,industry_tag,product_tag,solution_tag,campaign_tag,topic_tag,properties,create_user,create_time,update_user,update_time";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getContactJoin() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("contactjoin");
        table1.setTableDataPath("/tmp/yuxiang/testdata/contactjoin.csv");

        String header = "contact_id,contact_visitor_id,contact_contact_id,contact_first_name,contact_last_name,contact_phone,contact_email,contact_industry_lv1_code,contact_industry_lv2_code,contact_position,contact_country,contact_province,contact_city,contact_company,contact_create_time,action_id,action_visitor_id,action_contact_id,action_campaign_id,action_offer_id,action_action_id,action_action_name,action_timestamp,action_start_time,action_end_time,action_duration,action_platform_type,action_platform_name,action_industry_tag,action_product_tag,action_solution_tag,action_campaign_tag,action_topic_tag,action_properties,action_create_user,action_create_time,action_update_user,action_update_time,content_id,content_offer_id,content_offer_name,content_offer_type,content_offer_category,content_sub_offer_category,content_campaign_id,content_campaign_name,content_heat_base_line,content_start_time,content_end_time,content_platform_name,content_platform_type,content_probe_type,content_offer_config,content_create_user,content_create_time,content_update_user,content_update_time";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static void mockProperty(RuleDiscoverExecuteRequest ruleRequest, String[] options){
        TableInfos tables = new TableInfos();
        TableInfo basic = getProperyBasic(ruleRequest.getDimensionID(), options);
        TableInfo address = getPropertyAddress(ruleRequest.getDimensionID(), options);
        TableInfo features = getPropertyFeatures(ruleRequest.getDimensionID(), options);
        TableInfo picture = getPropertyPicture(ruleRequest.getDimensionID(), options);
        TableInfo sa1_satistics = getPropertySA1Satistics(ruleRequest.getDimensionID(), options);
        TableInfo mapping_sa1 = getPropertyMappingSA1(ruleRequest.getDimensionID(), options);
        TableInfo school = getPropertySchool(ruleRequest.getDimensionID(), options);
        TableInfo school_ranking = getPropertySchoolRanking(ruleRequest.getDimensionID(), options);
        TableInfo mapping_school = getPropertyMappingSchool(ruleRequest.getDimensionID(), options);
        TableInfo train_station = getPropertyTrainStation(ruleRequest.getDimensionID(), options);
        TableInfo train_time = getPropertyTrainTime(ruleRequest.getDimensionID(), options);
        TableInfo mapping_train_station = getPropertyMappingTrainStation(ruleRequest.getDimensionID(), options);

        List<TableInfo> listable = new ArrayList<>();
        listable.add(basic);
        listable.add(address);
        listable.add(features);
        listable.add(picture);
        listable.add(sa1_satistics);
        listable.add(mapping_sa1);
        listable.add(school);
        listable.add(school_ranking);
        listable.add(mapping_school);
        listable.add(train_station);
        listable.add(train_time);
        listable.add(mapping_train_station);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/"+ruleRequest.getTaskId() +"/result.ree");
    }

    private static TableInfo getProperyBasic(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Propery_Basic");
        if (options == null) {
            table1.setTableDataPath(path + "/1_Propery_Basic.csv");
        } else {
            table1.setTableDataPath(path + "/1_Propery_Basic__" + generateDataMark(options) + ".csv");
        }

        String header = "proID,address,price,bedroom,bathroom,parking,proType,sold_date,agency_name,agency_addr,des_head,des_content,features";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertyAddress(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Property_Address");
        if (options == null) {
            table1.setTableDataPath(path + "/2_Property_Address.csv");
        } else {
            table1.setTableDataPath(path + "/2_Property_Address__" + generateDataMark(options) + ".csv");
        }

        String header = "proID,Lat,Lng,Formated_Address,Locality,State,Postal_Code";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertyFeatures(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Property_Features");
        if (options == null) {
            table1.setTableDataPath(path + "/3_Property_Features.csv");
        } else {
            table1.setTableDataPath(path + "/3_Property_Features__" + generateDataMark(options) + ".csv");
        }

        String header = "IDxx,Air_Conditioning,Alarm,Balconey,BBQ,City_view,Close_to_Shops,Close_to_Transport,Close_to_Schools,courtyard,Dining_room,Dish_Washer,Ducted,Ensuite,Family_Room,Fireplace,Fully_Fenced,gas_heating,Gym,Heating,Intercom,Laundry,Mountain_Views,Park,Swimming_Pool,Renovated,River_Views,Rumpus_Room,Sauna,Study_room,Sun_Room,System_Heating,Tennis_Court,Water_Views,wordrobe,SUM";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertyPicture(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Picture");
        if (options == null) {
            table1.setTableDataPath(path + "/4_Picture.csv");
        } else {
            table1.setTableDataPath(path + "/4_Picture__" + generateDataMark(options) + ".csv");
        }

        String header = "proID,picNo,picAddr";
        String type = "varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertySA1Satistics(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("SA1_Satistics");
        if (options == null) {
            table1.setTableDataPath(path + "/5_SA1_Satistics.csv");
        } else {
            table1.setTableDataPath(path + "/5_SA1_Satistics__" + generateDataMark(options) + ".csv");
        }

        String header = "region_id,sa1_no,residents,Median_age,Median_total_personal_income_weekly,Birthplace_Australia_Persons,Birthplace_Australia_Persons_percentage,Language_spoken_at_home_English_only_Persons,English_Percentage,Australian_citizen_Persons,AU_percentage,Age_of_Persons_attending_an_educational_institution_25_years_and_over_Persons,Highest_year_of_school_completed_Year_12_or_equivalent_Persons,Median_rent_weekly";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertyMappingSA1(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Mapping_SA1");
        if (options == null) {
            table1.setTableDataPath(path + "/6_Mapping_SA1.csv");
        } else {
            table1.setTableDataPath(path + "/6_Mapping_SA1__" + generateDataMark(options) + ".csv");
        }

        String header = "proID,sa1_no";
        String type = "varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertySchool(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("School");
        if (options == null) {
            table1.setTableDataPath(path + "/7_School.csv");
        } else {
            table1.setTableDataPath(path + "/7_School__" + generateDataMark(options) + ".csv");
        }

        String header = "school_ID,name,gender,restrictedZone,type,school_lng,school_lat";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertySchoolRanking(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("School_ranking");
        if (options == null) {
            table1.setTableDataPath(path + "/8_School_ranking.csv");
        } else {
            table1.setTableDataPath(path + "/8_School_ranking__" + generateDataMark(options) + ".csv");
        }

        String header = "school_ID,oriName,Ranking,Locality,IB,Students_Enrolled_in_VCE,Median_VCE_score,Scores_of_40_percentage";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertyMappingSchool(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Mapping_School");
        if (options == null) {
            table1.setTableDataPath(path + "/9_Mapping_School.csv");
        } else {
            table1.setTableDataPath(path + "/9_Mapping_School__" + generateDataMark(options) + ".csv");
        }

        String header = "pro_ID,sec1,sec2";
        String type = "varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertyTrainStation(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Train_Station");
        if (options == null) {
            table1.setTableDataPath(path + "/10_Train_Station.csv");
        } else {
            table1.setTableDataPath(path + "/10_Train_Station__" + generateDataMark(options) + ".csv");
        }

        String header = "stop_id,stop_no,stop_short_name,stop_name,stop_lat,stop_lon";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertyTrainTime(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Train_Time");
        if (options == null) {
            table1.setTableDataPath(path + "/11_Train_Time.csv");
        } else {
            table1.setTableDataPath(path + "/11_Train_Time__" + generateDataMark(options) + ".csv");
        }

        String header = "id,stop_ori,stop_des,avg_time,trans_flag";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPropertyMappingTrainStation(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("Mapping_Train_Station");
        if (options == null) {
            table1.setTableDataPath(path + "/12_Mapping_Train_Station.csv");
        } else {
            table1.setTableDataPath(path + "/12_Mapping_Train_Station__" + generateDataMark(options) + ".csv");
        }

        String header = "id,propertyID,stop_id,distance_text1,distance_value1,duration_text1,duration_value1";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static void mockPaperAuthor(RuleDiscoverExecuteRequest ruleRequest){
        TableInfos tables = new TableInfos();
        TableInfo author = getAuthor2(ruleRequest.getDimensionID());
        TableInfo paper = getPaper2(ruleRequest.getDimensionID());
        TableInfo a2p = getA2P(ruleRequest.getDimensionID());

        List<TableInfo> listable = new ArrayList<>();
        listable.add(author);
        listable.add(paper);
        listable.add(a2p);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/"+ruleRequest.getTaskId() +"/result.ree");

//        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("AMiner_Author");
//        col.setColumnName("author_affiliations");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildJaro(0.98);
//        model.setColumns(columns);
//        models.add(model);
//
//        ModelColumns col1 = new ModelColumns();
//        col1.setTableName("AMiner_Paper");
//        col1.setColumnName("paper_title");
//        columns = new ArrayList<>();
//        columns.add(col1);
//
//        model = SimilarAlgorithmsFactory.buildLevenshtein(0.98);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static void mockPaperAuthor_ml(RuleDiscoverExecuteRequest ruleRequest){
        TableInfos tables = new TableInfos();
        TableInfo author = getAuthor2_ml(ruleRequest.getDimensionID());
        TableInfo paper = getPaper2_ml(ruleRequest.getDimensionID());
        TableInfo a2p = getA2P_ml(ruleRequest.getDimensionID());

        List<TableInfo> listable = new ArrayList<>();
        listable.add(author);
        listable.add(paper);
        listable.add(a2p);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/"+ruleRequest.getTaskId() +"/result.ree");

//        // set similarity or ML functions
//        List<ModelPredicate> models = new ArrayList<>();
//        List<ModelColumns> columns = new ArrayList<>();
//        ModelColumns col = new ModelColumns();
//        col.setTableName("AMiner_Author");
//        col.setColumnName("author_affiliations");
//        columns.add(col);
//
//        ModelPredicate model = SimilarAlgorithmsFactory.buildJaro(0.98);
//        model.setColumns(columns);
//        models.add(model);
//
//        ModelColumns col1 = new ModelColumns();
//        col1.setTableName("AMiner_Paper");
//        col1.setColumnName("paper_title");
//        columns = new ArrayList<>();
//        columns.add(col1);
//
//        model = SimilarAlgorithmsFactory.buildLevenshtein(0.98);
//        model.setColumns(columns);
//        models.add(model);
//
//        ruleRequest.setModelPredicats(models);

    }

    private static void mockPaperAuthorSampling(RuleDiscoverExecuteRequest ruleRequest, String[] options){
        TableInfos tables = new TableInfos();
        TableInfo author = getAuthor2_sampling(ruleRequest.getDimensionID(), options);
        TableInfo paper = getPaper2_sampling(ruleRequest.getDimensionID(), options);
        TableInfo a2p = getA2P_sampling(ruleRequest.getDimensionID(), options);

        List<TableInfo> listable = new ArrayList<>();
        listable.add(author);
        listable.add(paper);
        listable.add(a2p);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
    }

    private static void mockPaperAuthorSample(RuleDiscoverExecuteRequest ruleRequest, String dataName){
        String[] datanames = dataName.split("_");
        TableInfos tables = new TableInfos();
        TableInfo author = getAuthorSample(datanames[1]);
        TableInfo paper = getPaperSample(datanames[1]);
        TableInfo a2p = getA2PSample(datanames[1]);

        List<TableInfo> listable = new ArrayList<>();
        listable.add(author);
        listable.add(paper);
        listable.add(a2p);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/"+ruleRequest.getTaskId() +"/result.ree");
    }
    private static void mockWanyiMutiTable(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfos tables = new TableInfos();

//        TableInfo table1 = getWanyiT1();
//        TableInfo table2 = getWanyiT2();
//        TableInfo table3 = getWanyiT3();
//        TableInfo table4 = getWanyiT4();
        TableInfo author = getAuthor();
//        TableInfo paper = getPaper();
        TableInfo venue = getVenue();

        List<TableInfo> listable = new ArrayList<>();
        listable.add(author);
//        listable.add(paper);
        listable.add(venue);
//        listable.add(table1);
//        listable.add(table2);
//        listable.add(table3);
//        listable.add(table4);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("131");
        ruleRequest.setResultStorePath("/tmp/rulefind/131/result.ree");
        ruleRequest.setCr("0.001");
        ruleRequest.setFtr("0.9");
    }


    private static TableInfo getA2PSmall() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author2Paper");
//        table1.setTableDataPath("hdfs:///tmp/small2/author2paper.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Author2Paper.csv");
        table1.setTableDataPath("hdfs:///tmp/small5/AMiner_Author2Paper.csv");

        String header = "author2paper_id,author_id,paper_id,author_position";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE";


        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        List<ColumnInfo> list1 = new ArrayList<>();

        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("author_id")) {
                List<FKMsg> fkList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("author_id");
                fkMsg.setFkTableName("AMiner_Author");
                fkList.add(fkMsg);
                c1.setFkMsgList(fkList);
            }
            if (c1.getColumnName().equals("paper_id")) {
                List<FKMsg> fkList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("paper_id");
                fkMsg.setFkTableName("AMiner_Paper");
                fkList.add(fkMsg);
                c1.setFkMsgList(fkList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);

//        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getA2P(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author2Paper");
//        table1.setTableDataPath("hdfs:///tmp/small2/author2paper.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Author2Paper.csv");
        table1.setTableDataPath(path + "/AMiner_Author2Paper.csv");

        String header = "author2paper_id,author_id,paper_id,author_position";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE";


        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        List<ColumnInfo> list1 = new ArrayList<>();

        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("author_id")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("author_id");
                fkMsg.setFkTableName("AMiner_Author");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
            if (c1.getColumnName().equals("paper_id")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("paper_id");
                fkMsg.setFkTableName("AMiner_Paper");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);

//        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getA2P_ml(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author2Paper_ml");
//        table1.setTableDataPath("hdfs:///tmp/small2/author2paper.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Author2Paper.csv");
        table1.setTableDataPath(path + "/AMiner_Author2Paper_ml.csv");

        String header = "author2paper_id,author_id,paper_id,author_position";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE";


        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        List<ColumnInfo> list1 = new ArrayList<>();

        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("author_id")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("author_id");
                fkMsg.setFkTableName("AMiner_Author_ml");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
            if (c1.getColumnName().equals("paper_id")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("paper_id");
                fkMsg.setFkTableName("AMiner_Paper_ml");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);

//        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getA2P_sampling(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author2Paper");
        table1.setTableDataPath(path + "/AMiner_Author2Paper__" + generateDataMark(options) + ".csv");

        String header = "author2paper_id,author_id,paper_id,author_position";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE";

        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        List<ColumnInfo> list1 = new ArrayList<>();

        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("author_id")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("author_id");
                fkMsg.setFkTableName("AMiner_Author");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
            if (c1.getColumnName().equals("paper_id")) {
                List<FKMsg> fkMsgList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("paper_id");
                fkMsg.setFkTableName("AMiner_Paper");
                fkMsgList.add(fkMsg);
                c1.setFkMsgList(fkMsgList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);

//        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getA2PSample(String sample_ratio) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author2Paper");
//        table1.setTableDataPath("hdfs:///tmp/small2/author2paper.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Author2Paper.csv");
        table1.setTableDataPath("hdfs:///tmp/aminer/random_sample/sample_" + sample_ratio + "/AMiner_Author2Paper.csv");

        String header = "author2paper_id,author_id,paper_id,author_position";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE";


        String[] colNames = header.split(",");
        String[] colTypes = type.split(",");

        List<ColumnInfo> list1 = new ArrayList<>();

        for (int i = 0; i < colNames.length; i++) {
            ColumnInfo c1 = new ColumnInfo();
            c1.setColumnName(colNames[i]);
            c1.setColumnType(colTypes[i]);
            if (c1.getColumnName().equals("author_id")) {
                List<FKMsg> fkList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("author_id");
                fkMsg.setFkTableName("AMiner_Author");
                fkList.add(fkMsg);
                c1.setFkMsgList(fkList);
            }
            if (c1.getColumnName().equals("paper_id")) {
                List<FKMsg> fkList = new ArrayList<>();
                FKMsg fkMsg = new FKMsg();
                fkMsg.setFkColumnName("paper_id");
                fkMsg.setFkTableName("AMiner_Paper");
                fkList.add(fkMsg);
                c1.setFkMsgList(fkList);
            }
            list1.add(c1);
        }
        table1.setColumnList(list1);

//        constructTable(table1, header, type);
        return table1;
    }
    private static TableInfo getVenue() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("venue");
        table1.setTableDataPath("hdfs:///tmp/glovedata/venue.csv");

        String header = "venue_id,venue_name";
        String type = "varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static TableInfo getPaper2Small() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Paper");
//        table1.setTableDataPath("hdfs:///tmp/small2/paper.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Paper.csv");
        table1.setTableDataPath("hdfs:///tmp/small5/AMiner_Paper.csv");

        String header = "paper_id,paper_title,author,paper_affiliations,year,venue";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),DOUBLE,varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }


    private static TableInfo getPaper2(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Paper");
//        table1.setTableDataPath("hdfs:///tmp/small2/paper.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Paper.csv");
        table1.setTableDataPath(path + "/AMiner_Paper.csv");

        String header = "paper_id,paper_title,author,paper_affiliations,year,venue";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),DOUBLE,varchar(20)";
//
//        String[] colNames = header.split(",");
//        String[] colTypes = type.split(",");
//
//        List<ColumnInfo> list1 = new ArrayList<>();
//
//        for (int i = 0; i < colNames.length; i++) {
//            ColumnInfo c1 = new ColumnInfo();
//            c1.setColumnName(colNames[i]);
//            c1.setColumnType(colTypes[i]);
//            if (c1.getColumnName().equals("author")) {
//                FKMsg fkMsg = new FKMsg();
//                fkMsg.setFkColumnName("author_name");
//                fkMsg.setFkTableName("author");
//                c1.setFkMsg(fkMsg);
//            }
//            if (c1.getColumnName().equals("venue")) {
//                FKMsg fkMsg = new FKMsg();
//                fkMsg.setFkColumnName("venue_name");
//                fkMsg.setFkTableName("venue");
//                c1.setFkMsg(fkMsg);
//            }
//            list1.add(c1);
//        }
//        table1.setColumnList(list1);
        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPaper2_ml(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Paper_ml");
//        table1.setTableDataPath("hdfs:///tmp/small2/paper.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Paper.csv");
        table1.setTableDataPath(path + "/AMiner_Paper_ml.csv");

        String header = "paper_id,paper_title,author,paper_affiliations,year,venue,paper_title_ml";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),DOUBLE,varchar(20),varchar(20)";
//
//        String[] colNames = header.split(",");
//        String[] colTypes = type.split(",");
//
//        List<ColumnInfo> list1 = new ArrayList<>();
//
//        for (int i = 0; i < colNames.length; i++) {
//            ColumnInfo c1 = new ColumnInfo();
//            c1.setColumnName(colNames[i]);
//            c1.setColumnType(colTypes[i]);
//            if (c1.getColumnName().equals("author")) {
//                FKMsg fkMsg = new FKMsg();
//                fkMsg.setFkColumnName("author_name");
//                fkMsg.setFkTableName("author");
//                c1.setFkMsg(fkMsg);
//            }
//            if (c1.getColumnName().equals("venue")) {
//                FKMsg fkMsg = new FKMsg();
//                fkMsg.setFkColumnName("venue_name");
//                fkMsg.setFkTableName("venue");
//                c1.setFkMsg(fkMsg);
//            }
//            list1.add(c1);
//        }
//        table1.setColumnList(list1);
        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPaper2_sampling(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Paper");
        table1.setTableDataPath(path + "/AMiner_Paper__" + generateDataMark(options) + ".csv");

        String header = "paper_id,paper_title,author,paper_affiliations,year,venue";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),DOUBLE,varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getPaperSample(String sample_ratio) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Paper");
//        table1.setTableDataPath("hdfs:///tmp/small2/paper.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Paper.csv");
        table1.setTableDataPath("hdfs:///tmp/aminer/random_sample/sample_" + sample_ratio + "/AMiner_Paper.csv");

        String header = "paper_id,paper_title,author,paper_affiliations,year,venue";
        String type = "varchar(20),varchar(20),varchar(20),varchar(20),DOUBLE,varchar(20)";
//
//        String[] colNames = header.split(",");
//        String[] colTypes = type.split(",");
//
//        List<ColumnInfo> list1 = new ArrayList<>();
//
//        for (int i = 0; i < colNames.length; i++) {
//            ColumnInfo c1 = new ColumnInfo();
//            c1.setColumnName(colNames[i]);
//            c1.setColumnType(colTypes[i]);
//            if (c1.getColumnName().equals("author")) {
//                FKMsg fkMsg = new FKMsg();
//                fkMsg.setFkColumnName("author_name");
//                fkMsg.setFkTableName("author");
//                c1.setFkMsg(fkMsg);
//            }
//            if (c1.getColumnName().equals("venue")) {
//                FKMsg fkMsg = new FKMsg();
//                fkMsg.setFkColumnName("venue_name");
//                fkMsg.setFkTableName("venue");
//                c1.setFkMsg(fkMsg);
//            }
//            list1.add(c1);
//        }
//        table1.setColumnList(list1);
        constructTable(table1, header, type);
        return table1;
    }
    private static TableInfo getAuthor2Small() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author");
//        table1.setTableDataPath("hdfs:///tmp/small2/author.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Author.csv");
        table1.setTableDataPath("hdfs:///tmp/small5/AMiner_Author.csv");

        String header = "author_id,author_name,author_affiliations,published_papers,citations,h_index,p_index,p_index_with_unequal_a_index,research_interests";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,varchar(20)";
        String confidence = "1,1,1,1,1,1,1,1,1";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getAuthor2(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author");
//        table1.setTableDataPath("hdfs:///tmp/small2/author.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Author.csv");
        table1.setTableDataPath(path + "/AMiner_Author.csv");

        String header = "author_id,author_name,author_affiliations,published_papers,citations,h_index,p_index,p_index_with_unequal_a_index,research_interests";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getAuthor2_ml(String path) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author_ml");
//        table1.setTableDataPath("hdfs:///tmp/small2/author.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Author.csv");
        table1.setTableDataPath(path + "/AMiner_Author_ml.csv");

        String header = "author_id,author_name,author_affiliations,published_papers,citations,h_index,p_index,p_index_with_unequal_a_index,research_interests,author_affiliations_ml";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getAuthor2_sampling(String path, String[] options) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author");
        table1.setTableDataPath(path + "/AMiner_Author__" + generateDataMark(options) + ".csv");

        String header = "author_id,author_name,author_affiliations,published_papers,citations,h_index,p_index,p_index_with_unequal_a_index,research_interests";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getAuthorSample(String sample_ratio) {
        TableInfo table1 = new TableInfo();
        table1.setTableName("AMiner_Author");
//        table1.setTableDataPath("hdfs:///tmp/small2/author.csv");
//        table1.setTableDataPath("hdfs:///tmp/aminer/csv/AMiner_Author.csv");
        table1.setTableDataPath("hdfs:///tmp/aminer/random_sample/sample" + sample_ratio + "/AMiner_Author.csv");

        String header = "author_id,author_name,author_affiliations,published_papers,citations,h_index,p_index,p_index_with_unequal_a_index,research_interests";
        String type = "varchar(20),varchar(20),varchar(20),DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }
    private static TableInfo getAuthor() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("author");
        table1.setTableDataPath("hdfs:///tmp/glovedata/author.csv");

        String header = "author_name,author_org,author_fk";
        String type = "varchar(20),varchar(20),varchar(20)";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getWanyiT3() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("t1w");
//        table1.setTableDataPath("hdfs:///tmp/test2/data/bbb.csv");
        table1.setTableDataPath("hdfs:///tmp/wanyi/t26w.csv");

        String header = "row_id,address,picno,state,postalcode,price,protype,agency_name,proid,formated_address,des_head,sold_date,locality,picaddr,agency_addr,bedroom,parking,bathroom";
        String type = "varchar(20),varchar(20),INT,varchar(20),varchar(20),INT,varchar(20),varchar(20),varchar(20)," +
                "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),INT,INT,INT";

        constructTable(table1, header, type);
        return table1;
    }

    private static TableInfo getWanyiT4() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("t2w");
//        table1.setTableDataPath("hdfs:///tmp/test2/data/bbb.csv");
        table1.setTableDataPath("hdfs:///tmp/wanyi/t26w.csv");

        String header = "row_id,address,picno,state,postalcode,price,protype,agency_name,proid,formated_address,des_head,sold_date,locality,picaddr,agency_addr,bedroom,parking,bathroom";
        String type = "varchar(20),varchar(20),INT,varchar(20),varchar(20),INT,varchar(20),varchar(20),varchar(20)," +
                "varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),varchar(20),INT,INT,INT";

        constructTable(table1, header, type);
        return table1;
    }

    @NotNull
    private static TableInfo getWanyiT2() {
        TableInfo table1 = new TableInfo();
        table1.setTableName("table2");
//        table1.setTableDataPath("hdfs:///tmp/test2/data/bbb.csv");
        table1.setTableDataPath("hdfs:///tmp/wanyi/t2/rd20200813192811451738759.orc");

        ColumnInfo c1 = new ColumnInfo();
        c1.setColumnName("row_id");
        c1.setColumnType("varchar(50)");
        c1.setSkip(true);

        ColumnInfo c2 = new ColumnInfo();
        c2.setColumnName("projectdescription");
        c2.setColumnType("varchar(50)");

        ColumnInfo c3 = new ColumnInfo();
        c3.setColumnName("citycompany");
        c3.setColumnType("varchar(50)");

        ColumnInfo c4 = new ColumnInfo();
        c4.setColumnName("externalname");
        c4.setColumnType("varchar(50)");

        ColumnInfo c5 = new ColumnInfo();
        c5.setColumnName("oldname");
        c5.setColumnType("varchar(50)");

        ColumnInfo c6 = new ColumnInfo();
        c6.setColumnName("projectcompanyname");
        c6.setColumnType("varchar(50)");

        ColumnInfo c7 = new ColumnInfo();
        c7.setColumnName("region");
        c7.setColumnType("varchar(50)");

        ColumnInfo c8 = new ColumnInfo();
        c8.setColumnName("icpprojectcode");
        c8.setColumnType("varchar(50)");

        ColumnInfo c9 = new ColumnInfo();
        c9.setColumnName("projectname");
        c9.setColumnType("varchar(50)");

        ColumnInfo c10 = new ColumnInfo();
        c10.setColumnName("projectdetailname");
        c10.setColumnType("varchar(50)");

        ColumnInfo c11 = new ColumnInfo();
        c11.setColumnName("projectcode");
        c11.setColumnType("varchar(50)");

        List<ColumnInfo> list1 = new ArrayList<>();
        list1.add(c1);
        list1.add(c2);
        list1.add(c3);
        list1.add(c4);
        list1.add(c5);
        list1.add(c6);
        list1.add(c7);
        list1.add(c8);
        list1.add(c9);
        list1.add(c10);
        list1.add(c11);

        table1.setColumnList(list1);
        return table1;
    }

    @NotNull
    private static TableInfo getWanyiT1() {
        TableInfo table = new TableInfo();
        table.setTableName("table1");
//        table.setTableDataPath("hdfs:///tmp/test2/data/aaa.csv");
        table.setTableDataPath("hdfs:///tmp/wanyi/t1/rd20200813192811123738755.orc");

        ColumnInfo c1 = new ColumnInfo();
        c1.setColumnName("row_id");
        c1.setColumnType("varchar(50)");
        c1.setSkip(true);

        ColumnInfo c2 = new ColumnInfo();
        c2.setColumnName("icpprojectcode");
        c2.setColumnType("varchar(50)");
        List<FKMsg> fkMsgList = new ArrayList<>();
        FKMsg fkMsg = new FKMsg();
        fkMsg.setFkColumnName("icpprojectcode");
        fkMsg.setFkTableName("table2");
        fkMsgList.add(fkMsg);
        c2.setFkMsgList(fkMsgList);

        ColumnInfo c3 = new ColumnInfo();
        c3.setColumnName("projectcode");
        c3.setColumnType("varchar(50)");

        ColumnInfo c4 = new ColumnInfo();
        c4.setColumnName("projectcompanyname");
        c4.setColumnType("varchar(50)");

        ColumnInfo c5 = new ColumnInfo();
        c5.setColumnName("projectdetailname");
        c5.setColumnType("varchar(50)");

        ColumnInfo c6 = new ColumnInfo();
        c6.setColumnName("projectname");
        c6.setColumnType("varchar(50)");

        ColumnInfo c7 = new ColumnInfo();
        c7.setColumnName("projectdescription");
        c7.setColumnType("varchar(50)");

        ColumnInfo c8 = new ColumnInfo();
        c8.setColumnName("citycompany");
        c8.setColumnType("varchar(50)");

        ColumnInfo c9 = new ColumnInfo();
        c9.setColumnName("externalname");
        c9.setColumnType("varchar(50)");

        List<ColumnInfo> list = new ArrayList<>();
        list.add(c1);
        list.add(c2);
        list.add(c3);
        list.add(c4);
        list.add(c5);
        list.add(c6);
        list.add(c7);
        list.add(c8);
        list.add(c9);

        table.setColumnList(list);
        return table;
    }

    private static void mockMutiTable(RuleDiscoverExecuteRequest ruleRequest) {

        TableInfos tables = new TableInfos();

        TableInfo table = new TableInfo();
        table.setTableName("aaa");
//        table.setTableDataPath("hdfs:///tmp/test2/data/aaa.csv");
        table.setTableDataPath("hdfs:///tmp/testOrc/data/aaa");

        ColumnInfo colID = new ColumnInfo();
        colID.setColumnName("id");
        colID.setModelStorePath("hdfs:///data/model/");

        ColumnInfo colSn = new ColumnInfo();
        colSn.setColumnName("name");
        colSn.setModelStorePath("hdfs:///data/model/");

        ColumnInfo colName = new ColumnInfo();
        colName.setColumnName("tel");
        colName.setModelStorePath("hdfs:///data/model/");

        ColumnInfo colCity = new ColumnInfo();
        colCity.setColumnName("address_id");
        colCity.setModelStorePath("hdfs:///data/model/");
        List<FKMsg> fkMsgList = new ArrayList<>();
        FKMsg fkMsg = new FKMsg();
        fkMsg.setFkColumnName("id");
        fkMsg.setFkTableName("bbb");
        fkMsgList.add(fkMsg);
        colCity.setFkMsgList(fkMsgList);


        List<ColumnInfo> list = new ArrayList<>();
        list.add(colID);
        list.add(colSn);
        list.add(colName);
        list.add(colCity);

        table.setColumnList(list);

        TableInfo table1 = new TableInfo();
        table1.setTableName("bbb");
//        table1.setTableDataPath("hdfs:///tmp/test2/data/bbb.csv");
        table1.setTableDataPath("hdfs:///tmp/testOrc/data/bbb");

        ColumnInfo colID1 = new ColumnInfo();
        colID1.setColumnName("id");
        colID1.setModelStorePath("hdfs:///data/model/");

        ColumnInfo colSn1 = new ColumnInfo();
        colSn1.setColumnName("address_name");
        colSn1.setModelStorePath("hdfs:///data/model/");

        ColumnInfo colName1 = new ColumnInfo();
        colName1.setColumnName("address_code");
        colName1.setModelStorePath("hdfs:///data/model/");

        List<ColumnInfo> list1 = new ArrayList<>();
        list1.add(colID1);
        list1.add(colSn1);
        list1.add(colName1);

        table1.setColumnList(list1);

        List<TableInfo> listable = new ArrayList<>();
        listable.add(table);
        listable.add(table1);

        tables.setTableInfoList(listable);

        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("137");
        ruleRequest.setDimensionID("15");
        ruleRequest.setResultStorePath("/tmp/rule");
        ruleRequest.setCr("0.0005");
        ruleRequest.setFtr("0.8");
    }

    private static void mockTbl1WTable(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfo table = new TableInfo();
        table.setTableName("tbl_1w");
        table.setTableDataPath("/tmp/test/data/tbl_1w.csv");

        ColumnInfo c1 = new ColumnInfo();
        c1.setColumnName("row_id");
        c1.setColumnType("varchar(50)");
        ColumnInfo c2 = new ColumnInfo();
        c2.setColumnName("address");
        c2.setColumnType("varchar(50)");
        ColumnInfo c3 = new ColumnInfo();
        c3.setColumnName("picno");
        c3.setColumnType("varchar(50)");
        ColumnInfo c4 = new ColumnInfo();
        c4.setColumnName("state");
        c4.setColumnType("varchar(50)");
        ColumnInfo c5 = new ColumnInfo();
        c5.setColumnName("postalcode");
        c5.setColumnType("varchar(50)");
        ColumnInfo c6 = new ColumnInfo();
        c6.setColumnName("price");
        c6.setColumnType("varchar(50)");
        ColumnInfo c7 = new ColumnInfo();
        c7.setColumnName("protype");
        c7.setColumnType("varchar(50)");
        ColumnInfo c8 = new ColumnInfo();
        c8.setColumnName("agency_name");
        c8.setColumnType("varchar(50)");
        ColumnInfo c10 = new ColumnInfo();
        c10.setColumnName("proid");
        c10.setColumnType("varchar(50)");
        ColumnInfo c11 = new ColumnInfo();
        c11.setColumnName("formated_address");
        c11.setColumnType("varchar(50)");
        ColumnInfo c12 = new ColumnInfo();
        c12.setColumnName("des_head");
        c12.setColumnType("varchar(50)");
        ColumnInfo c13 = new ColumnInfo();
        c13.setColumnName("sold_date");
        c13.setColumnType("varchar(50)");
        ColumnInfo c14 = new ColumnInfo();
        c14.setColumnName("locality");
        c14.setColumnType("varchar(50)");
        ColumnInfo c15 = new ColumnInfo();
        c15.setColumnName("picaddr");
        c15.setColumnType("varchar(50)");
        ColumnInfo c16 = new ColumnInfo();
        c16.setColumnName("agency_addr");
        c16.setColumnType("varchar(50)");
        ColumnInfo c17 = new ColumnInfo();
        c17.setColumnName("bedroom");
        c17.setColumnType("varchar(50)");
        ColumnInfo c18 = new ColumnInfo();
        c18.setColumnName("parking");
        c18.setColumnType("varchar(50)");
        ColumnInfo c19 = new ColumnInfo();
        c19.setColumnName("bathroom");
        c19.setColumnType("varchar(50)");


        List<ColumnInfo> list = new ArrayList<>();
        list.add(c1);
        list.add(c2);
        list.add(c3);
        list.add(c4);
        list.add(c5);
        list.add(c6);
        list.add(c7);
        list.add(c8);
        list.add(c10);
        list.add(c11);
        list.add(c12);
        list.add(c13);
        list.add(c14);
        list.add(c15);
        list.add(c16);
        list.add(c17);
        list.add(c18);
        list.add(c19);

        table.setColumnList(list);

        List<TableInfo> tableInfoList = new ArrayList<>();
        tableInfoList.add(table);

        TableInfos tables = new TableInfos();
        tables.setTableInfoList(tableInfoList);
        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("12");
        ruleRequest.setDimensionID("15");
        ruleRequest.setResultStorePath("/tmp/test1");
        ruleRequest.setCr("0.0005");
        ruleRequest.setFtr("0.8");
    }


    private static void mockTable(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfo table = new TableInfo();
        table.setTableName("tbl_1w");
        table.setTableDataPath("/tmp/test/data/tbl_1w.csv");

        ColumnInfo c1 = new ColumnInfo();
        c1.setColumnName("row_id");
        c1.setColumnType("varchar(50)");
        ColumnInfo c2 = new ColumnInfo();
        c2.setColumnName("address");
        c2.setColumnType("varchar(50)");
        ColumnInfo c3 = new ColumnInfo();
        c3.setColumnName("picno");
        c3.setColumnType("varchar(50)");
        ColumnInfo c4 = new ColumnInfo();
        c4.setColumnName("state");
        c4.setColumnType("varchar(50)");
        ColumnInfo c5 = new ColumnInfo();
        c5.setColumnName("postalcode");
        c5.setColumnType("varchar(50)");
        ColumnInfo c6 = new ColumnInfo();
        c6.setColumnName("price");
        c6.setColumnType("varchar(50)");
        ColumnInfo c7 = new ColumnInfo();
        c7.setColumnName("protype");
        c7.setColumnType("varchar(50)");
        ColumnInfo c8 = new ColumnInfo();
        c8.setColumnName("agency_name");
        c8.setColumnType("varchar(50)");
        ColumnInfo c10 = new ColumnInfo();
        c10.setColumnName("proid");
        c10.setColumnType("varchar(50)");
        ColumnInfo c11 = new ColumnInfo();
        c11.setColumnName("formated_address");
        c11.setColumnType("varchar(50)");
        ColumnInfo c12 = new ColumnInfo();
        c12.setColumnName("des_head");
        c12.setColumnType("varchar(50)");
        ColumnInfo c13 = new ColumnInfo();
        c13.setColumnName("sold_date");
        c13.setColumnType("varchar(50)");
        ColumnInfo c14 = new ColumnInfo();
        c14.setColumnName("locality");
        c14.setColumnType("varchar(50)");
        ColumnInfo c15 = new ColumnInfo();
        c15.setColumnName("picaddr");
        c15.setColumnType("varchar(50)");
        ColumnInfo c16 = new ColumnInfo();
        c16.setColumnName("agency_addr");
        c16.setColumnType("varchar(50)");
        ColumnInfo c17 = new ColumnInfo();
        c17.setColumnName("bedroom");
        c17.setColumnType("varchar(50)");
        ColumnInfo c18 = new ColumnInfo();
        c18.setColumnName("parking");
        c18.setColumnType("varchar(50)");
        ColumnInfo c19 = new ColumnInfo();
        c19.setColumnName("bathroom");
        c19.setColumnType("varchar(50)");


        List<ColumnInfo> list = new ArrayList<>();
        list.add(c1);
        list.add(c2);
        list.add(c3);
        list.add(c4);
        list.add(c5);
        list.add(c6);
        list.add(c7);
        list.add(c8);
        list.add(c10);
        list.add(c11);
        list.add(c12);
        list.add(c13);
        list.add(c14);
        list.add(c15);
        list.add(c16);
        list.add(c17);
        list.add(c18);
        list.add(c19);

        table.setColumnList(list);

        List<TableInfo> tableInfoList = new ArrayList<>();
        tableInfoList.add(table);

        TableInfos tables = new TableInfos();
        tables.setTableInfoList(tableInfoList);
        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("12");
        ruleRequest.setDimensionID("15");
        ruleRequest.setResultStorePath("/tmp/test1");
        ruleRequest.setCr("0.0005");
        ruleRequest.setFtr("0.8");
    }

    private static void mockUserInfo(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfo table = new TableInfo();
        table.setTableName("user_info");
        table.setTableDataPath("/user/suyanghua/csv/user_info/user_info.csv");

        ColumnInfo colID = new ColumnInfo();
        colID.setColumnName("id");
        colID.setColumnType("varchar(50)");

        ColumnInfo colSn = new ColumnInfo();
        colSn.setColumnName("sn");
        colSn.setColumnType("varchar(50)");

        ColumnInfo colName = new ColumnInfo();
        colName.setColumnName("name");
        colName.setColumnType("varchar(50)");

        ColumnInfo colCity = new ColumnInfo();
        colCity.setColumnName("city");
        colCity.setColumnType("varchar(50)");

        ColumnInfo zcode = new ColumnInfo();
        zcode.setColumnName("zcode");
        zcode.setColumnType("int8");

        ColumnInfo addr = new ColumnInfo();
        addr.setColumnName("addr");
        addr.setColumnType("varchar(50)");

        ColumnInfo tel = new ColumnInfo();
        tel.setColumnName("tel");
        tel.setColumnType("varchar(50)");

        ColumnInfo birth = new ColumnInfo();
        birth.setColumnName("birth");
        birth.setColumnType("varchar(50)");

        ColumnInfo gender = new ColumnInfo();
        gender.setColumnName("gender");
        gender.setColumnType("varchar(50)");

        ColumnInfo product = new ColumnInfo();
        product.setColumnName("product");
        product.setColumnType("varchar(50)");

        List<ColumnInfo> list = new ArrayList<>();
        list.add(colID);
        list.add(colSn);
        list.add(colName);
        list.add(colCity);
        list.add(zcode);
        list.add(addr);
        list.add(tel);
        list.add(birth);
        list.add(gender);
        list.add(product);

        table.setColumnList(list);

        List<TableInfo> tableInfoList = new ArrayList<>();
        tableInfoList.add(table);

        TableInfos tables = new TableInfos();
        tables.setTableInfoList(tableInfoList);
        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("121");
        ruleRequest.setDimensionID("15");
        ruleRequest.setResultStorePath("/tmp/test1");
        ruleRequest.setCr("0.0005");
        ruleRequest.setFtr("0.8");

        List<ModelPredicate> models = new ArrayList<>();
        List<ModelColumns> columns = new ArrayList<>();
        ModelColumns col = new ModelColumns();
        col.setTableName("user_info");
        col.setColumnName("addr");
        columns.add(col);

        ModelPredicate model = SimilarAlgorithmsFactory.buildCosine(0.9);
        model.setColumns(columns);
        models.add(model);

        model = SimilarAlgorithmsFactory.buildJaccard(0.9);
        model.setColumns(columns);
        models.add(model);

//        model = SimilarAlgorithmsFactory.buildJaro(0.9);
//        model.setColumns(columns);
//        models.add(model);
//
//        model = SimilarAlgorithmsFactory.buildLevenshtein(0.9);
//        model.setColumns(columns);
//        models.add(model);

//        model = MlFactory.buildSelfSingle();
//        model.setColumns(columns);
//        models.add(model);


//        List<ModelColumns> columns1 = new ArrayList<>();
//        ModelColumns col1 = new ModelColumns();
//        col1.setTableName("user_info");
//        col1.setColumnName("product");
//        columns1.add(col1);

        ruleRequest.setModelPredicats(models);
    }

    private static void mockAcmTable(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfo tableA = new TableInfo();
        tableA.setTableName("tableA");
        tableA.setTableDataPath("/user/suyanghua/csv/acm/tableA.csv");

        ColumnInfo colID = new ColumnInfo();
        colID.setColumnName("id");
        colID.setColumnType("varchar(50)");

        ColumnInfo colTitle = new ColumnInfo();
        colTitle.setColumnName("title");
        colTitle.setColumnType("varchar(50)");

        ColumnInfo colAuthors = new ColumnInfo();
        colAuthors.setColumnName("authors");
        colAuthors.setColumnType("varchar(50)");

        ColumnInfo colVenue = new ColumnInfo();
        colVenue.setColumnName("venue");
        colVenue.setColumnType("varchar(50)");

        ColumnInfo colYear = new ColumnInfo();
        colYear.setColumnName("year");
        colYear.setColumnType("varchar(50)");

        ColumnInfo colEid = new ColumnInfo();
        colEid.setColumnName("eid");
        colEid.setColumnType("varchar(50)");

        List<ColumnInfo> list = new ArrayList<>();
        list.add(colID);
        list.add(colTitle);
        list.add(colAuthors);
        list.add(colVenue);
        list.add(colYear);
        list.add(colEid);

        tableA.setColumnList(list);


        TableInfo tableB = new TableInfo();
        tableB.setTableName("tableB");
        tableB.setTableDataPath("/user/suyanghua/csv/acm/tableB.csv");
        tableB.setColumnList(list);

        List<TableInfo> tableInfoList = new ArrayList<>();
        tableInfoList.add(tableA);
        tableInfoList.add(tableB);

        TableInfos tables = new TableInfos();
        tables.setTableInfoList(tableInfoList);
        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("1001");
        ruleRequest.setDimensionID("15");
        ruleRequest.setResultStorePath("/tmp/test1");
        ruleRequest.setCr("0.00001");
        ruleRequest.setFtr("0.8");
    }

    private static void mockChessTable(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfo table = new TableInfo();
        table.setTableName("chess");
        table.setTableDataPath("/tmp/small7/chess.csv");

        ColumnInfo colID = new ColumnInfo();
        colID.setColumnName("white_king_file");
        colID.setColumnType("varchar(50)");

        ColumnInfo colSn = new ColumnInfo();
        colSn.setColumnName("white_king_rank");
        colSn.setColumnType("varchar(50)");

        ColumnInfo colName = new ColumnInfo();
        colName.setColumnName("white_rook_file");
        colName.setColumnType("varchar(50)");

        ColumnInfo colCity = new ColumnInfo();
        colCity.setColumnName("white_rook_rank");
        colCity.setColumnType("varchar(50)");

        ColumnInfo addr = new ColumnInfo();
        addr.setColumnName("black_king_file");
        addr.setColumnType("varchar(50)");

        ColumnInfo tel = new ColumnInfo();
        tel.setColumnName("black_king_rank");
        tel.setColumnType("varchar(50)");

        ColumnInfo birth = new ColumnInfo();
        birth.setColumnName("optimal_depth_of_win");
        birth.setColumnType("varchar(50)");

        List<ColumnInfo> list = new ArrayList<>();
        list.add(colID);
        list.add(colSn);
        list.add(colName);
        list.add(colCity);
        list.add(addr);
        list.add(tel);
        list.add(birth);

        table.setColumnList(list);

        List<TableInfo> tableInfoList = new ArrayList<>();
        tableInfoList.add(table);

        TableInfos tables = new TableInfos();
        tables.setTableInfoList(tableInfoList);
        ruleRequest.setTableInfos(tables);
        ruleRequest.setResultStorePath("/tmp/rulefind/"+ruleRequest.getTaskId() +"/result.ree");
    }

    private static void mockWbcTable(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfo table = new TableInfo();
        table.setTableName("wbc");
        table.setTableDataPath("/user/suyanghua/csv/wbc/wbc.csv");

        ColumnInfo colID = new ColumnInfo();
        colID.setColumnName("sample_code_number");
        colID.setColumnType("varchar(50)");

        ColumnInfo colSn = new ColumnInfo();
        colSn.setColumnName("clump_thickness");
        colSn.setColumnType("varchar(50)");

        ColumnInfo colName = new ColumnInfo();
        colName.setColumnName("uniformity_of_cell_size");
        colName.setColumnType("varchar(50)");

        ColumnInfo colCity = new ColumnInfo();
        colCity.setColumnName("uniformity_of_cell_shape");
        colCity.setColumnType("varchar(50)");

        ColumnInfo addr = new ColumnInfo();
        addr.setColumnName("marginal_adhesion");
        addr.setColumnType("varchar(50)");

        ColumnInfo tel = new ColumnInfo();
        tel.setColumnName("single_epithelial_cell_size");
        tel.setColumnType("varchar(50)");

        ColumnInfo birth = new ColumnInfo();
        birth.setColumnName("bare_nuclei");
        birth.setColumnType("varchar(50)");


        ColumnInfo bland_chromatin = new ColumnInfo();
        bland_chromatin.setColumnName("bland_chromatin");
        bland_chromatin.setColumnType("varchar(50)");

        ColumnInfo normal_nucleoli = new ColumnInfo();
        normal_nucleoli.setColumnName("normal_nucleoli");
        normal_nucleoli.setColumnType("varchar(50)");

        ColumnInfo mitoses = new ColumnInfo();
        mitoses.setColumnName("mitoses");
        mitoses.setColumnType("varchar(50)");

        ColumnInfo classColumn = new ColumnInfo();
        classColumn.setColumnName("class");
        classColumn.setColumnType("varchar(50)");

        List<ColumnInfo> list = new ArrayList<>();
        list.add(colID);
        list.add(colSn);
        list.add(colName);
        list.add(colCity);
        list.add(addr);
        list.add(tel);
        list.add(birth);
        list.add(bland_chromatin);
        list.add(normal_nucleoli);
        list.add(mitoses);
        list.add(classColumn);

        table.setColumnList(list);

        List<TableInfo> tableInfoList = new ArrayList<>();
        tableInfoList.add(table);

        TableInfos tables = new TableInfos();
        tables.setTableInfoList(tableInfoList);
        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("11001");
        ruleRequest.setDimensionID("15");
        ruleRequest.setResultStorePath("/tmp/test1");
        ruleRequest.setCr("0.0005");
        ruleRequest.setFtr("0.8");
    }

    public static Map<String, PredicateExpressionModel> mockGeneratePredicate() {
        Map<String, PredicateExpressionModel> predicate = new HashMap<>();

        PredicateExpressionModel p1 = new PredicateExpressionModel();
        p1.setExplicitPredicateExpression("user_info.t0.id == user_info.t1.id");
        p1.setCalcPredicateExpression("if(t0.id == t1.id, 1, 0)");
        predicate.put("pre1", p1);

        PredicateExpressionModel p2 = new PredicateExpressionModel();
        p2.setExplicitPredicateExpression("user_info.t0.id <> user_info.t1.id");
        p2.setCalcPredicateExpression("if(t0.id != t1.id, 1, 0)");
        predicate.put("pre2", p2);

        PredicateExpressionModel p3 = new PredicateExpressionModel();
        p3.setExplicitPredicateExpression("user_info.t0.name == user_info.t1.name");
        p3.setCalcPredicateExpression("if(t0.name == t1.name, 1, 0)");
        predicate.put("pre3", p3);

        PredicateExpressionModel p4 = new PredicateExpressionModel();
        p4.setExplicitPredicateExpression("user_info.t0.name <> user_info.t1.name");
        p4.setCalcPredicateExpression("if(t0.name != t1.name, 1, 0)");
        predicate.put("pre4", p4);

        return  predicate;
    }

    private static void mockFdReducedTable(RuleDiscoverExecuteRequest ruleRequest) {
        TableInfo table = new TableInfo();
        table.setTableName("fd-reduced-30");
        table.setTableDataPath("/tmp/zhangjun/fd-reduced-30.csv");

        String[] columnName = {"000","001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018"
                ,"019","020","021","022","023","024","025","026","027","028","029"};
        List<ColumnInfo> list = new ArrayList<>();

        for (int i = 0; i < columnName.length; i++) {
            ColumnInfo currentColInfo = new ColumnInfo();
            currentColInfo.setColumnName(columnName[i]);
            currentColInfo.setColumnType("varchar(50)");
            list.add(currentColInfo);
        }

        table.setColumnList(list);

        List<TableInfo> tableInfoList = new ArrayList<>();
        tableInfoList.add(table);

        TableInfos tables = new TableInfos();
        tables.setTableInfoList(tableInfoList);
        ruleRequest.setTableInfos(tables);
        ruleRequest.setTaskId("12306");
        ruleRequest.setDimensionID("15");
        ruleRequest.setResultStorePath("/tmp/rulefind/" + ruleRequest.getTaskId() + "/result.ree");
        ruleRequest.setCr("0.0000000001");
        ruleRequest.setFtr("0.8");
    }
}
