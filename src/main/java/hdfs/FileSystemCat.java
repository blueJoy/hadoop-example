package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by baixiangzhu on 2017/6/26.
 */
public class FileSystemCat {

    public static void main(String[] args) throws IOException {

        //可以通过args传入
        String uri = "hdfs://192.168.16.133:9000/test/README.txt";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);

        InputStream in = null;

        try{
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }


    }

}
