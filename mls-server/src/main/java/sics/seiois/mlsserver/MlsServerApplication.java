package sics.seiois.mlsserver;

import com.sics.seiois.common.DefaultServerApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.web.client.RestTemplate;


@SpringBootApplication(scanBasePackages = {"sics.seiois.mlsserver", "com.sics.seiois.common"})
@EnableEurekaClient
@EnableAspectJAutoProxy
public class MlsServerApplication extends DefaultServerApplication {

    public MlsServerApplication() {
        super(MlsServerApplication.class);
    }

    public static void main(String[] args) {
//        String fPath =  UdfJarManager.getLocalPath();
//        String hdfsPath = UdfJarManager.getHdfsPath();
//        new UdfJarManager().prepareUdfJars(hdfsPath, fPath);

        MlsServerApplication application = new MlsServerApplication();
        application.run(args);
    }

    @Bean
    @LoadBalanced
    public RestTemplate restTemplate(){
        return new RestTemplate();
    }
}
