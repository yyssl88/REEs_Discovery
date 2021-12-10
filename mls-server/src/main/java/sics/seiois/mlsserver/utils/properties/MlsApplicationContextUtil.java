package sics.seiois.mlsserver.utils.properties;

import org.springframework.context.ApplicationContext;

public class MlsApplicationContextUtil {

    private static ApplicationContext applicationContext;

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public static void setApplicationContext(ApplicationContext applicationContext) {
        MlsApplicationContextUtil.applicationContext = applicationContext;
    }

}
