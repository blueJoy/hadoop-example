package map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * 多表连接
 * Created by baixiangzhu on 2017/6/22.
 */
public class MTJoin {

    private static int time = 0;

    /*
    在map中先去分输入行属于左表还是右表，然后对两列值进行分割
    保存连接列在key值，剩余列和左右表标志在value中，最后输出
     */
    private static class MTMapper extends Mapper<Object,Text,Text,Text>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            //0-9数字的前一个位置
            int i = 0;
            //首行不处理
            if(line.contains("factoryname") || line.contains("addressID"))
                return;

            //找出数据中的分割点
            while (line.charAt(i) >= '9'|| line.charAt(i) <= '0'){
                i++;
            }

            //左表  factory表.第一个位为字符
            if(line.charAt(0) >= '9'|| line.charAt(0) <= '0'){
                int j = i-1;
                while(line.charAt(j) != ' ') j--;

                //例：Tencent 3
                String[] values = {line.substring(0,j),line.substring(i)};

                //例： 3 - 1+Tencent
                context.write(new Text(values[1]),new Text("1+"+values[0]));
            }else {

                //右表   address 表. 第一位为0 -9 的数字
                int j = i +1;  //1
                while(line.charAt(j) != ' ') j++;
                //例： 1-北京
                String[] values = {line.substring(0,i+1),line.substring(j)};
                //例：1- 2+北京
                context.write(new Text(values[0]),new Text("2+"+values[1]));
            }
        }
    }


    /*
     reduce 解析map输出，将value中数据按照左右表分别保存，
     然后笛卡尔积输出
     */
    public static class MTReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            if(time == 0){
                context.write(new Text("factoryname"),new Text("addressname"));
                time ++;
            }

            int factoryNum = 0;
            String [] factory = new String[10];
            int addressNum = 0;
            String [] address = new String[10];

            Iterator<Text> iterator = values.iterator();

            //values   3- [1+Tenct,2+Beijing]
            while (iterator.hasNext()){

                String record = iterator.next().toString();
                System.out.println("####"+record);
                int len = record.length();
                int i =2;
                char type = record.charAt(0);
                String factoryname = "";
                String addressname = "";
                if(type == '1'){ //左表
                    factory[factoryNum] = record.substring(i);
                    factoryNum++;
                }else {  //右表
                    address[addressNum] = record.substring(i);
                    addressNum ++;
                }

            }

            if(factoryNum !=0 && addressNum != 0){
                for (int m =0; m < factoryNum ; m++){
                    for (int n=0; n < addressNum; n++){
                        context.write(new Text(factory[m]),new Text(address[n]));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();


        Job job = new Job(conf,"multiple table join");
        job.setJarByClass(MTJoin.class);
        job.setMapperClass(MTJoin.MTMapper.class);
        //job.setCombinerClass(STReducer.class);
        job.setReducerClass(MTJoin.MTReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
