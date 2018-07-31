package Day7_30;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;


//统计单词出现的次数 ,指定输出格式
public class WordCount2 {

    public static class myMapper2 extends Mapper<LongWritable,Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //把获得每行到转换为字符串
            String line = value.toString();
            //按照规则切割
            String[] words = line.split(",");

            //遍历数组，获得key
            for (String word : words) {

                //设置值，直接完成写入
                context.write(new Text(word),new LongWritable(1));
            }
        }
    }

    public static class myReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            //定义统计变量
            long sum  =0l;

            //遍历mapper中获得v2s
            for (LongWritable value : values) {
                sum += value.get();
            }
            //输出k3,v3
            context.write(key,new LongWritable(sum));

        }
    }

    //创建主函数去整合map 和reducer
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建配置
        Configuration conf = new Configuration();

        //创建job
        Job job = Job.getInstance(conf, WordCount2.class.getSimpleName());

        //指定主类
        job.setJarByClass(WordCount2.class);

        //指定mapper类
        job.setMapperClass(myMapper2.class);

        //指定mapper类输出key,value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定reducer类
        job.setReducerClass(myReducer.class);

        //指定输出key，value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //指定输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //指定输出文档格式
        job.setOutputFormatClass(TextOutputFormat.class);

        //添加到job队列
        job.waitForCompletion(true);
    }





}
