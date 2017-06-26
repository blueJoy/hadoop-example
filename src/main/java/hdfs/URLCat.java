package hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 *
 * Created by baixiangzhu on 2017/6/26.
 */
public class URLCat {

    static{

        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }


    public static void main(String[] args) {

        InputStream in = null;

        String path = "hdfs://192.168.16.133:9000/test/README.txt";

        try {
            in = new URL(path).openStream();

            IOUtils.copyBytes(in,System.out,4096,false);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }


    }

}
