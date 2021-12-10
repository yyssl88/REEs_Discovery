package sics.seiois.mlsserver.biz.mock;

import com.alibaba.fastjson.JSONObject;
import com.sics.seiois.client.model.mls.TableInfos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sics.seiois.mlsserver.model.SparkContextConfig;

public class TableInfoTest {

    private static final Logger logger = LoggerFactory.getLogger(TableInfoTest.class);


    public static void main(String[] args) throws Exception {
        String tableJson = RuleFindRequestMock.mockRuleFindReqest("").toString();
        testTable(tableJson);
    }

    private static void testTable(String tableJson) throws Exception {

        String reqJson = tableJson;
//        File tableJsonFile = new File(tableJson);
//        reqJson = FileUtils.readFileToString(tableJsonFile, "utf-8");

        JSONObject jsonObject = JSONObject.parseObject(reqJson);
        String taskId = jsonObject.getString("taskId");
        JSONObject tableInfosJson = jsonObject.getJSONObject("tableInfos");
        TableInfos tableInfos = tableInfosJson.toJavaObject(TableInfos.class);

        SparkContextConfig sparkContextConfig = new SparkContextConfig();
    }
}
