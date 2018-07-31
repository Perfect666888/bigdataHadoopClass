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

public class wordCount {

    public static class myMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
                                                //输入的key  value输出的key value
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //把值转换为字符串，方便切割
            //一次是一行
            String line = value.toString();
            //按照指定规则切割
            String[] words = line.split(",");
            for (String word : words) {
                //写入到输出，出现一次，就是标记 1
                context.write(new Text(word),new LongWritable(1));
            }


        }
    }


    public static class myReduce extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            //设置统计变量
            Long sum =0l;
            //遍历迭代器，去获得标记
            for (LongWritable value : values) {
                sum+=value.get();
            }
            //输出结果，key 是word  value 为sum
            context.write(key,new LongWritable(sum));

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置信息
        Configuration conf = new Configuration();
        //建立job对象
        //设置jobName,一般直接是该类的类名
        String jobName=wordCount.class.getSimpleName();
        Job job = Job.getInstance(conf, jobName);

        //指定主类
        job.setJarByClass(wordCount.class);

        //指定map类
        job.setMapperClass(myMapper.class);
        //指定map的输出类型
        //key
        job.setMapOutputKeyClass(Text.class);
        //value
        job.setMapOutputValueClass(LongWritable.class);

        //指定reducer类
        job.setReducerClass(myReduce.class);

        //指定输出类型(和reducer类的输出一样)
        //key
        job.setOutputKeyClass(Text.class);
        //value
        job.setOutputValueClass(LongWritable.class);

        //添加输入路径，直接使用args[0] 类似scanner
        FileInputFormat.addInputPath(job,new Path(args[0]));

        //指定输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //指定输出文件格式[可不写]
        job.setOutputFormatClass(TextOutputFormat.class);

        //加入到job进程
        job.waitForCompletion(true);
    }



}
