package Day7_30;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

public class Join2 {

    //两张表完成关联
    //创建自己的mapper
    public static class myMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获得输入的全路径
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String pathName = fileSplit.getPath().getName();

            //把获得到数据转换为字符串，方便切割
            String line = value.toString();

            //判断是那个文件
            //用户数据文件
            if(pathName.contains("dianxin_data")) {
                String[] splits = line.split("\t");
                //数据清洗
                if(splits.length==8&&splits[2]!=null){
                    //获得key  城市id 和 value 用户id 停留时间
                    String mapKey1 =splits[2];
                    //value加标记
                    String mapValue2="dianxin_data"+splits[0]+","+splits[4];

                    //输出
                    context.write(new Text(mapKey1),new Text(mapValue2));
                }


            }

            //城市数据文件
            if (pathName.contains("city_id")){
                String[] splits = line.split(",");
                //获得key  城市id 和 value  城市名称
                String mapKey2=splits[2];
                //value加标记
                String mapValue2="city_id"+splits[3];
                //输出
                context.write(new Text(mapKey2),new Text(mapValue2));
            }
        }
    }


    //创建自己的reducer
    public static class myReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //创建2个集合存放来自不同文件的数据
            Vector<String> dataArr = new Vector<String>();
            Vector<String> cityArr = new Vector<String>();

            //遍历迭代器
            for (Text value : values) {
                //转换为字符串，方便截取和判断
                String line = value.toString();

                //判断标识,来区分
                //区分后，去掉标识
                if(line.startsWith("dianxin_data")){
                    dataArr.add(line.substring(12));
                }else if (line.startsWith("city_id")){
                    cityArr.add(line.substring(7));
                }
            }


            //遍历集合，完成笛卡儿积
            for (int i = 0; i < dataArr.size(); i++) {
                for (int j = 0; j < cityArr.size(); j++) {
                    //拼接value
                    String result =dataArr.get(i)+","+cityArr.get(j);
                    //输出
                    context.write(key,new Text(result));
                }
            }
        }
    }

    //创建主方法，整合类
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置
        Configuration conf = new Configuration();

        //创建job对象
        Job job = Job.getInstance(conf, Join2.class.getSimpleName());

        //指定jar类
        job.setJarByClass(Join2.class);

        //指定mapper类
        job.setMapperClass(myMapper.class);
        //指定mapper的输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定reducer
        job.setReducerClass(myReducer.class);

        //指定输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //添加输入文件路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));

        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        //指定输出文件类型
        job.setOutputFormatClass(TextOutputFormat.class);

        //添加job到队列
        job.waitForCompletion(true);

    }


}
