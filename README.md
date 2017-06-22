#搭建Hadoop本地测试环境
   注：如果不配置HDFS，会使用本地文件系统代替HDFS
1. 下载Hadoop win版本，然后配置环境变量HADOOP_HOME
2. 添加依赖包。这里以3.0.0-alpha2为例子
            
            <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-common</artifactId>
                <version>3.0.0-alpha2</version>
            </dependency>
    
            <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-hdfs</artifactId>
                <version>3.0.0-alpha2</version>
            </dependency>
    
            <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-client</artifactId>
                <version>3.0.0-alpha2</version>
            </dependency>

3. 编写mapreduce程序，例如wordcount

4. 添加日志文件 log4j.properties 
    
    `
    log4j.appender.console=org.apache.log4j.ConsoleAppender
    log4j.appender.console.Target=System.out
    log4j.appender.console.layout=org.apache.log4j.PatternLayout
    log4j.appender.console.layout.ConversionPattern=%d{ABSOLUTE} %5p %c{1}:%L - %m%n
    log4j.rootLogger=INFO, console
    `
    
5. 运行wordcount程序。这时会抛出异常，提示在$HADOOP_HOME/bin目录下，缺少 winutils.exe 这个程序。
    解决方案：下载：http://download.csdn.net/detail/u010435203/9606355 放到$HADOOP_HOME/bin下面
    
6. 再次运行，会继续报错：java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Ljava/lang/String;I)Z
    解决方案:直接下载：http://download.csdn.net/detail/u010435203/9606129.为NativeIO的java类，放到路径为org.apache.hadoop.io.nativeio的包下即可
    
7. 执行测试用例。  本地测试，配置configuration   例如：wordcount 输入参数的program arguments为 testfile/input/wordcount testfile/output/wordcount
     集群或者伪集群：   hadoop jar hadoop-example.jar map.WordCount  /input /output