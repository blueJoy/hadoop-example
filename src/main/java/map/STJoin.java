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
 * 单表连接
 * Created by baixiangzhu on 2017/6/22.
 */
public class STJoin {

    public static int time =0;

    /*
        map将输入分割成child 和 parent ,然后正序输出一次作为右表，
        反序输出一次作为左表，需要注意的是在输出的value中必须加上左右表 区别标志
     */
    public static class STMapper extends Mapper<Object,Text,Text,Text>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String childName;
            String parentName;
            //左右表区分标志
            String relationType;

            String line = value.toString();
            int i = 0;

            while (line.charAt(i) != ' '){
                i ++;
            }

            String [] values = {line.substring(0,i),line.substring(i+1)};
            if(values[0].compareTo("child") != 0){

                childName = values[0];
                parentName = values[1];

                //左表
                relationType = "1";
                context.write(new Text(values[1]),new Text(relationType+"+"+childName+"+"+parentName));

                //右表
                relationType = "2";
                context.write(new Text(values[0]),new Text(relationType+"+"+childName+"+"+parentName));
            }
        }
    }

    public static class STReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //输出表头
            if(time == 0){

                context.write(new Text("grandchild"),new Text("grandparent"));

                time ++;
            }

            int grandChildNum = 0;
            String [] grandChild = new String[10];

            int grandParentNum = 0;
            String [] grandParent = new String[10];

            Iterator<Text> iterator = values.iterator();

            while (iterator.hasNext()){

                String record = iterator.next().toString();
                System.out.println("########################"+record);
                int len = record.length();
                int i = 2;
                if(len == 0) continue;

                char relationType = record.charAt(0);
                String childName = "";
                String parentName = "";

                //获取value-list 中的value 的child
                while(record.charAt(i) != '+'){
                    childName = childName+record.charAt(i);
                    i++;
                }

                i = i+1;

                //获取value-list 中 value的parent
                while(i < len){
                    parentName = parentName + record.charAt(i);
                    i++;
                }


                //从左表去除child放入grandChild
                if(relationType == '1'){
                    grandChild[grandChildNum] = childName;
                    grandChildNum ++;
                }
                //从右表去除parent放入grandParent
                else{
                    grandParent[grandParentNum] = parentName;
                    grandParentNum ++;
                }

            }

            //grandChild 和 grandParent 数组求笛卡尔积
            if(grandChildNum != 0 && grandParentNum != 0){
                for (int m = 0 ; m < grandChildNum ; m ++){
                    for (int n = 0; n < grandParentNum; n ++){
                        //输出结果
                        context.write(new Text(grandChild[m]),new Text(grandParent[n]));
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();


        Job job = new Job(conf,"single table join");
        job.setJarByClass(STJoin.class);
        job.setMapperClass(STMapper.class);
        //job.setCombinerClass(STReducer.class);
        job.setReducerClass(STReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
