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

import java.io.IOException;
import java.util.Vector;

//两张数据表进行关联
//要求把，停留时间和什么城市拼接起来
public class MapReduersJoin {

    //创建自定义map对象
    public static class myMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获得文件的全路径转换为字符串
            //转换为split
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            //获得全路径
            String path = fileSplit.getPath().toString();

            //获得的记录转换为字符串
            String line = value.toString();

            //获得到2个文件，所以要区分开
            if (path.contains("dianxin_data")){
                //进行切割
                String[] strs = line.split("\t");
                //数据清洗
                if(strs.length==8 && strs[2]!=null) {
                    //指定key 城市id
                    Text mapKey= new Text(strs[2]);
                    //指定value
                    //拼接需要的字符串类型  标记  用户id，停留时间
                    String result = "data"+strs[0]+","+strs[4];

                    //输出
                    context.write(mapKey,new Text(result));
                }
            }

            if(path.contains("city_id")){
                //切割
                String[] strs2 = line.split(",");
                //指定key
                Text mapKey2= new Text(strs2[2]);
                //拼接value
                String result2= "city"+strs2[3];

                //输出
                context.write(mapKey2,new Text(result2));
            }


        }
    }

    //创建reducer类，完成拼接
    public static class myReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //创建集合存放内容
            Vector<String> dataArr = new Vector<String>();
            Vector<String> cityArr = new Vector<String>();

            //遍历迭代器,切开2个表的内容
            for (Text value : values) {
                //转换为string
                String line = value.toString();
                //判断，分割
                if (line.startsWith("data")){
                    //去掉标记,添加到对应的集合中
                    dataArr.add(line.substring(4));
                }else if(line.startsWith("city")){
                    cityArr.add(line.substring(4));
                }
            }
            
            //完成切割后开始拼接,笛卡儿积
            for (int i = 0; i < dataArr.size(); i++) {
                for (int i1 = 0; i1 < cityArr.size(); i1++) {
                    //拼接value
                    String result =dataArr.get(i)+","+cityArr.get(i1);
                    //输出
                    context.write(key,new Text(result));
                }

            }
        }
    }

    //创建main 整合类
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, MapReduersJoin.class.getSimpleName());

        //指定主类
        job.setJarByClass(MapReduersJoin.class);

        //指定mapper类
        job.setMapperClass(myMapper.class);
        //指定mapper类输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定reducer类
        job.setReducerClass(myReducer.class);

        //指定combie
        job.setCombinerClass(myReducer.class);

        //指定输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定输出文件格式
        job.setOutputFormatClass(TextOutputFormat.class);

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //2个文件添加2次
        FileInputFormat.addInputPath(job,new Path(args[1]));

        //指定输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        //把job添加到任务中
        job.waitForCompletion(true);

    }


}
