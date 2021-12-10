1、python文件的管理方式
python代码文件，由java代码从http服务器下载。
需把python代码文件（把mls-server整个项目的源码压缩成mlsserver_py.tar.gz）上传到：
http://package.rml.net.cn/anaconda/mlsserver_py.tar.gz
见：
src\main\java\sics\seiois\mlsserver\mlpredicate\pyutils\Env.java

2、数据表文件、机器学习模型的管理方式
数据表文件、机器学习模型，有python代码从hdfs服务器下载。
见：
src\main\resources\python\mlpredicate\ditto\rml\rml_api.py