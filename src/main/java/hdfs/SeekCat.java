package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Created by baixiangzhu on 2017/6/26.
 */
public class SeekCat {

    public static void main(String[] args) throws IOException {

        String  uri = "hdfs://192.168.16.133:9000/test/hello.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);

        FSDataInputStream fsIn = null;

        try{
            fsIn = fs.open(new Path(uri));
            IOUtils.copyBytes(fsIn,System.out,4096,false);

            //从指定位置读取
            fsIn.seek(3);

            IOUtils.copyBytes(fsIn,System.out,4096,false);
        }finally {
            IOUtils.closeStream(fsIn);
        }

    }

}
