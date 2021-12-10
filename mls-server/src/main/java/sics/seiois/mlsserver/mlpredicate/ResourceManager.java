package sics.seiois.mlsserver.mlpredicate;

import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.dao.ModelDaoMapper;

import javax.annotation.Resource;
import java.net.URI;
import java.util.Enumeration;
import java.util.Properties;

@Getter
public class ResourceManager {

    private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);

    @Resource
    private static ModelDaoMapper modelDaoMapper;
    public static boolean UseYarn = true;
    // static String BasePath = "file:///E://tmp/";
    static String BasePath = "E://temp2/";
    static String HdfsPath = "hdfs:///data/models/";
    static String ResourcePath;

    public static void setUseYarn(boolean useYarn) {
        UseYarn = useYarn;
    }

    public static FileSystem getFileSystem() throws Exception{
        //默认加载 core-site.xml core-defult.xml hdfs-site.xml hdfs-defult.xml
        Configuration configuration = new Configuration();
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }

    /*public static void readFile(String filePath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); // 复制到标准输出流
        } finally {
            IOUtils.closeStream(in);
        }
    }*/

    public static FSDataInputStream readFile(String fileName) throws Exception {
        FileSystem fileSystem = getFileSystem();
        Path readPath = new Path(fileName);
        //文件流
        FSDataInputStream inputStream =  fileSystem.open(readPath);
        /*try{
            //读流
            IOUtils.copyBytes(inputStream,System.out,4096,false);
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            //close steam 关闭资源
            IOUtils.closeStream(inputStream);
        }*/
        return inputStream;
    }

    public static String getModelFilePath(String modelid) throws Exception {
        String modelPath = null;

        URI uri = getFileSystem().getUri();

        String configFilePath = uri + "/data/models/" + modelid + "/model.conf";

        Properties properties = new Properties();

        // mls配置项从文件读取
        try {
            properties.load(readFile(configFilePath));
            //properties.load(configFilePath);
        } catch (Exception e) {
            logger.error("properties load error", e);
        }

        Enumeration<?> enum1 = properties.propertyNames();

        while (enum1.hasMoreElements()) {
            String key = (String) enum1.nextElement();
            String value = properties.getProperty(key);
            switch(key){
                case "resource_name":
                    ResourcePath = value;
                    break;
                default:
                    logger.info("parseConfig(): unknown config option: key=" + key + "value=" + value);
                    break;
            }
        }

        if(UseYarn) {
            // modelPath = "./resources.zip/resources/" + modelid + "/bert_pb";
            //modelPath = "./" + modelid + "./" + ".zip/"+ modelid + "/bert_pb";
            modelPath = "./" + ResourcePath + ".zip/bert_pb";
        } else {
             modelPath = "/temp2/resources/" + ResourcePath + "/bert_pb";
        }
        return modelPath;
    }

    public static String getArchivesFilePath() {
        // String path = "hdfs:///data/models/resources.zip";
        String path = "";
        try {
            // Configuration conf = new Configuration(); // 加载配置文件，将配置文件放在src下
            // FileSystem fs = FileSystem.get(conf); // 获取文件系统实例
            Configuration conf = new Configuration(); // 加载配置文件，将配置文件放在src下
            FileSystem fs = FileSystem.get(URI.create(HdfsPath), conf);
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(HdfsPath), true);
            // 递归列出该目录下所有文件，不包括文件夹，后面的布尔值为是否递归
            while (listFiles.hasNext()) { // 如果listfiles里还有东西
                LocatedFileStatus next = listFiles.next(); // 得到下一个并pop出listFiles
                String fileName = next.getPath().getName();
                // System.out.println(fileName); // 输出
                if (fileName.endsWith(".zip")) {
                    if (path.length() > 0) {
                        path = path + ",";
                    }
                    path = path + next.getPath().toString();
                }
            }
        } catch (Exception e) {
            logger.error("getArchivesFilePath error", e);
        }
        // System.out.println("**********path:" + path);
        return path;
    }

    public static String getVocabFilePath(String modelid) throws Exception {
        String path = null;

        URI uri = getFileSystem().getUri();

        String configFilePath = uri + "/data/models/" + modelid + "/model.conf";

        Properties properties = new Properties();

        // mls配置项从文件读取
        try {
            properties.load(readFile(configFilePath));
        } catch (Exception e) {
            logger.error("getVocabFilePath error", e);
        }

        Enumeration<?> enum1 = properties.propertyNames();

        while (enum1.hasMoreElements()) {
            String key = (String) enum1.nextElement();
            String value = properties.getProperty(key);
            switch(key){
                case "resource_name":
                    ResourcePath = value;
                    break;
                default:
                    logger.info("parseConfig(): unknown config option: key=" + key + "value=" + value);
                    break;
            }
        }

        if(UseYarn) {
            // path = "./resources.zip/resources/" + modelid + "/bert.vocab";
            path = "./" + ResourcePath + ".zip/"+ ResourcePath + "/bert.vocab";
            path = "./" + ResourcePath + ".zip/bert.vocab";
        } else {
            path = "/temp2/resources/" + ResourcePath + "/bert.vocab";
        }
        return path;
    }

    public static void addTfJars(SparkSession spark){
        if(UseYarn) {
            // NULL
            // 在doRuleDiscoveryJava中addjar
        }
        else {
            spark.sparkContext().addFile(BasePath, true);
            // spark.sparkContext().addJar("file:\\\\\\" + SparkFiles.getRootDirectory()  + "/lib/libtensorflow-1.15.0.jar");
            // spark.sparkContext().addJar("file:\\\\\\" + SparkFiles.getRootDirectory()  + "/lib/tensorflow_jni-1.15.0.jar");
        }

    }

}
