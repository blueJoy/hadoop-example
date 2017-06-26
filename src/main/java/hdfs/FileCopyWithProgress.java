package hdfs;

import com.sun.xml.internal.messaging.saaj.packaging.mime.util.OutputUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;

/**
 * Created by baixiangzhu on 2017/6/26.
 */
public class FileCopyWithProgress {

    /*

    遇到的问题：org.apache.hadoop.security.AccessControlException: org.apache.hadoop.security .AccessControlException: Permission denied:
                user=Administrator, access=WRITE, inode="hadoop": hadoop:supergroup:rwxr-xr-x

    1、在系统的环境变量或java JVM变量里面添加HADOOP_USER_NAME，这个值具体等于多少看自己的情况，
        以后会运行HADOOP上的Linux的用户名。（修改完重启eclipse，不然可能不生效）
    2、将当前系统的帐号修改为hadoop
    3、使用HDFS的命令行接口修改相应目录的权限，hadoop fs -chmod 777 /user,后面的/user是要上传文件的路径，不同的情况可能不一样，
        比如要上传的文件路径为hdfs://namenode/user/xxx.doc，则这样的修改可以，如果要上传的文件路径为hdfs://namenode/java/xxx.doc，
        则要修改的为hadoop fs -chmod 777 /java或者hadoop fs -chmod 777 /，java的那个需要先在HDFS里面建立Java目录，后面的这个是为根目录调整权限。
     */
    public static void main(String[] args) throws IOException {

        String localSrc = "E://menu_privileges.json";

        String dst = "hdfs://192.168.16.133:9000/test/menu.json";

        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst),conf);

        OutputStream out = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.println("*");
            }
        });

        IOUtils.copyBytes(in,out,4096,true);


    }

}
