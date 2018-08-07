package Day8_01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

//错误
//join了之和去除重复，相同的就求总和
public class JoinQC {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置
        Configuration conf = new Configuration();

        //创建job
        Job job = Job.getInstance(conf, JoinQC.class.getSimpleName());

        //指定jar类
        job.setJarByClass(JoinQC.class);
        //指定mapper类
        job.setMapperClass(myMapper.class);
        //指定mapper数据输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //指定reducer类
        job.setReducerClass(myReducer.class);

        //添加输入路径
        FileInputFormat.addInputPath(job, new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Data\\dianxin_data"));
        FileInputFormat.addInputPath(job, new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Data\\city_id.txt"));

        //指定输出路径
        Path outPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\OutJoinQC\\JoinOut");
        //为了方便多次执行，和防止文件夹已经存在的错误,先判断文件是否存在
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        //指定输入文件格式
        job.setInputFormatClass(TextInputFormat.class);

        //添加任务
        job.waitForCompletion(true);

    }

    //创建自定义map类
    public static class myMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获得文件名
            FileSplit fs = (FileSplit) context.getInputSplit();
            String name = fs.getPath().getName();
            //转换为字符串，方便切割
            String line = value.toString();

            //判断，区分文件,数据
            if (name.contains("dianxin_data")) {
                String[] split = line.split("\t");
                //获得id，城市编号，和停留时间
                //需要完成数据清洗,清理脏数据
                if (split.length == 8 && !split[2].equals("\\N")) {
                    //长度相符，城市编号这列不为空
                    //key 城市编号 其他的为value
                    //再次清洗，确定停留 时间这个列为数字
                    if (split[4].equals("\\N")) {
                        split[4] = "0";
                    }
                    //输出
                    //拼接出value，加入标记
                    String result = "dianxin" + split[0] + "," + split[4];
                    context.write(new Text(split[2]), new Text(result));
                }


            } else if (name.contains("city_id")) {
                String[] split = line.split(",");
                //数据清洗
                if (split.length == 10 && split[2] != null) {
                    //再次清洗出名字不为null的
                    if (split[3] != null) {
                        //key 城市编号  value 为城市名称
                        //输出
                        context.write(new Text(split[2]), new Text("city" + split[3]));
                    }
                }

            }


        }
    }

    //创建自定义reducer类
    public static class myReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //创建2个集合存储，2个文件的数据
            //使用map去除重复,
            //数据表   key 城市id+，+用户id   值，停留时间的累加
            HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
            //city表    key  城市id 值 城市名字
            HashMap<String, String> cityMap = new HashMap<String, String>();

            //整合数据
            for (Text value : values) {
                //转换为字符串，方便判断
                String line = value.toString();
                if (line.startsWith("dianxin")) {
                    //去除标记
                    String[] datas = line.substring(7).split(",");
                    //用户id，停留时间
                    //拼接出key
                    String dmKey = key + "," + datas[0];
                    Integer dmValue = Integer.valueOf(datas[1]);
                    //传入到map中
                    if (dataMap.get(dmKey) == null) {
                        dataMap.put(dmKey, dmValue);
                    } else {
                        //存在就完成累加
                        dmValue += dataMap.get(dmKey);
                        //覆盖
                        dataMap.put(dmKey,dmValue);
                    }
                } else if (line.startsWith("city")) {
                    //去除标记
                    String substring = line.substring(4);
                    cityMap.put(key.toString(),substring);
                }
            }




            //遍历datamap
            Set<String> dataKeys = dataMap.keySet();
            for (String dataKey : dataKeys) {
                //截取出城市id
                String[] cityAndId = dataKey.split(",");
                //获得时间总和
                Integer times = dataMap.get(dataKey);
                String cityname = cityMap.get(cityAndId[0]);
                //拼接出数据
                String result = cityname+","+cityAndId[1]+","+times;
                context.write(key,new Text(result));
            }

        }
    }


}
