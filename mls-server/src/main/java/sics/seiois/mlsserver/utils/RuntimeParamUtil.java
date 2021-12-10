package sics.seiois.mlsserver.utils;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.service.impl.EvidenceGenerateMain;

import java.util.HashMap;
import java.util.Map;

public class RuntimeParamUtil {
    private static final Logger logger = LoggerFactory.getLogger(RuntimeParamUtil.class);

    public static String getRuntimeParam(String otherParam, String param) {
        if(otherParam == null || "".equalsIgnoreCase(otherParam)) return null;
        Map<String, String> name2valueMap = convertString2Map(otherParam);
//        logger.info("####run time param: {}", name2valueMap);
        return name2valueMap.get(param);
    }

    public static boolean canGet(String otherParam, String param) {
        Map<String, String> name2valueMap = convertString2Map(otherParam);
        return (name2valueMap.get(param) == null || "null".equals(name2valueMap.get(param))) ? false : true;
    }

    @NotNull
    private static Map<String, String> convertString2Map(String otherParam) {
        Map<String, String> name2valueMap = new HashMap<>();
        String[] name2values = otherParam.split(";");
        for (String name2value : name2values) {
            String[] name2valueArray = name2value.split("=");
            name2valueMap.put(name2valueArray[0], name2valueArray[1]);
        }
        return name2valueMap;
    }
}
