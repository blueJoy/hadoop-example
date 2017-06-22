package map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 *
 *      name - scoure
 * Created by baixiangzhu on 2017/6/19.
 */
public class AverageScore {

    public static class ScoreMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            System.out.println(line);


            StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");

            while (tokenizerArticle.hasMoreTokens()){

                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                //姓名
                String strName = tokenizerLine.nextToken();
                //分数
                String strScore = tokenizerLine.nextToken();

                Text name = new Text(strName);
                IntWritable score = new IntWritable(Integer.parseInt(strScore));

                context.write(name,score);
            }
        }
    }


    public static class ScoreReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            int count = 0;

            Iterator<IntWritable> iterator = values.iterator();

            while (iterator.hasNext()){
                sum += iterator.next().get();
                count ++;
            }

            int average = sum / count;
            context.write(key,new IntWritable(average));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf,"scoreProcess");
        job.setJarByClass(AverageScore.class);
        job.setMapperClass(AverageScore.ScoreMapper.class);
        job.setCombinerClass(AverageScore.ScoreReducer.class);
        job.setReducerClass(AverageScore.ScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
