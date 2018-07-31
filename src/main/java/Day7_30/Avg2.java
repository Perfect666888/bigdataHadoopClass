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

public class Avg2 {

    //创建自己的mapper
    public static class myMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //读取到的数据转为字符串
            String line = value.toString();

            //按照指定条件切割
            String[] splits = line.split("\t");

            //数据清洗
            if (splits.length==8&&splits[4]!=null){

                //再次判断停留时间的是不是数字，还是\\N
                //是\\N需要重新赋值
                if(splits[4].equals("\\N")){
                    splits[4]="0";
                }
                //输出
                //key 为id   value 为停留时间
                context.write(new Text(splits[0]),new LongWritable(Long.parseLong(splits[4])));
            }

        }
    }

    //创建自己的reducer
    public static class myReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //创建总和变量
            long sum =0l;
            //创建统计变量，values中有多少个数
            long size =0l;

            //遍历迭代器去获得总和
            for (LongWritable value : values) {
                sum +=(value.get());
                //个数+1
                size++;
            }

            //防止除数为0
            if(size!=0){
                //获得平均时间
                long avgTime=(sum/size);
                //输出
                //key id value 平均时间
                context.write(key,new LongWritable(avgTime));
            }
        }
    }

    //创建主类整合类,调用
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置
        Configuration conf = new Configuration();

        //创建job对象
        Job job = Job.getInstance(conf, Avg2.class.getSimpleName());

        //设置jar类
        job.setJarByClass(Avg2.class);

        //设置mapper类
        job.setMapperClass(myMapper.class);
        //设置mapper的输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reducer
        job.setReducerClass(myReducer.class);

        //设置输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //设置输出文件格式
        job.setOutputFormatClass(TextOutputFormat.class);

        //添加job到队列
        job.waitForCompletion(true);

    }




}
