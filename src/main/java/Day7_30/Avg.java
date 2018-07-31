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

import java.io.IOException;


//求每个人在城市的平均逗留时间
public class Avg {

    //创建自己的mapper
    public static class myMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //转为字符串
            String line = value.toString();
            //按照指定格式切割
            String[] splits = line.split("\t");
            //获得指定字符串，作为key 和value
            //先完成数据清洗
            if(splits.length==8&&splits[4]!=null){

                //再次判断，如果时间这列数据为/n时，需要设置为0
                if(splits[4].equals("\\N")){
                    splits[4]="0";
                }
                //输出  key  id    value 停留时间
                context.write(new Text(splits[0]),new LongWritable(Long.parseLong(splits[4])));
            }
        }
    }

    //创建自己的reducer
    public static class myReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //设置统计变量
            long sum =0l;
            //统计迭代器里有多少数
            long length =0l;

            //遍历迭代器
            for (LongWritable value : values) {
                sum+=(value.get());
                length++;
            }

            //求出value，即avg
            //再次清洗
            if(length!=0) {
                long avgTime = (sum/length);
                //输出
                context.write(key,new LongWritable(avgTime));
            }

        }
    }


    //创建主方法整合类
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //建立配置
        Configuration conf = new Configuration();

        //建立job对象
        Job job = Job.getInstance(conf, Avg.class.getSimpleName());

        //指定jarClass
        job.setJarByClass(Avg.class);

        //指定mapper
        job.setMapperClass(myMapper.class);

        //指定mapper输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定reducer
        job.setReducerClass(myReducer.class);

        //指定输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //指定输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //添加job到任务
        job.waitForCompletion(true);


    }



}
