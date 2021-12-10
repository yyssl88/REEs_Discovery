package sics.seiois.mlsserver.service.impl;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import sics.seiois.mlsserver.utils.properties.MlsApplicationContextUtil;

@Component
public class MlsApplicationContext implements ApplicationContextAware {

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        MlsApplicationContextUtil.setApplicationContext(applicationContext);
    }
}
