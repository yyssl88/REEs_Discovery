package sics.seiois.mlsserver.utils.properties;

import com.alibaba.fastjson.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import sics.seiois.mlsserver.dao.MlsPropertiesMapper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Resource;

public class MlsPropertiesUtils {

    private static final Log log = LogFactory.getLog(MlsPropertiesUtils.class);

    public static Map<String, String> getMap() {
        ApplicationContext ac = MlsApplicationContextUtil.getApplicationContext();
        MlsPropertiesMapper mlsPropertiesMapper = ac.getBean(MlsPropertiesMapper.class);
        List<MlsPropertiesPojo> mlsPropertiesDtos =mlsPropertiesMapper.selectAll();

        Map<String, String> propertityMap = new ConcurrentHashMap<>();
        if (mlsPropertiesDtos != null && mlsPropertiesDtos.size() > 0) {
            for (MlsPropertiesPojo mlsPropertiesDto : mlsPropertiesDtos) {
                propertityMap.put(mlsPropertiesDto.getKey(), mlsPropertiesDto.getValue());
            }
        }
        log.info("mlsPropertiesUtils.MAP:" + JSON.toJSONString(propertityMap));

        return propertityMap;
    }

}
