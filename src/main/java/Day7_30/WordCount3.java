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


//统计单词出现的次数，加入到commbine，指定间隔符

public class WordCount3 {

    public static class myMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //读取到每行数据转换为字符串
            String line = value.toString();
            //按照指定 规则切割
            String[] words = line.split(",");

            //遍历数组,作为key
            for (String word : words) {

                //出现一次，就做一个标记为1，方便统计
                context.write(new Text(word), new LongWritable(1));
            }

        }
    }
        public static class myReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
            @Override
            protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
                //设定统计变量
                long sum =0;
                //遍历迭代及，开始统计出现次数
                for (LongWritable value : values) {
                    sum+=value.get();
                }

                //统计完毕,输出
                context.write(key,new LongWritable(sum));
            }
        }

        //创建主函数，整合类
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            //创建配置
            Configuration conf = new Configuration();
            //设置输出格式
            conf.set("mapred.textoutputformat.separator",",");

            //创建job对象
            Job job = Job.getInstance(conf, WordCount3.class.getSimpleName());

            //指定类
            job.setJarByClass(WordCount3.class);

            //指定mapper
            job.setMapperClass(myMapper.class);
            //指定mapper输出的key，value类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            //指定reducer类
            job.setReducerClass(myReducer.class);

            //把reducer添加到combie
            job.setCombinerClass(myReducer.class);

            //添加输入路径
            FileInputFormat.addInputPath(job,new Path(args[0]));
            //指定输出路径
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            //指定输出文件格式
            job.setOutputFormatClass(TextOutputFormat.class);

            //把job添加到进程
            job.waitForCompletion(true);

        }




}
