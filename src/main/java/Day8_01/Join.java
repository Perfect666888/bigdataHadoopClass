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
import java.util.Vector;

public class Join {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置
        Configuration conf = new Configuration();

        //创建job
        Job job = Job.getInstance(conf, Join.class.getSimpleName());

        //指定jar类
        job.setJarByClass(Join.class);
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
        Path outPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\OutJoin\\JoinOut");
        //为了方便多次执行，和防止文件夹已经存在的错误,先判断文件是否存在
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

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
                if (split.length == 8 && split[2] != null) {
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
                if(split.length==10 && split[2]!=null){
                    //再次清洗出名字不为null的
                    if(split[3]!=null){
                        //key 城市编号  value 为城市名称
                        //输出
                        context.write(new Text(split[2]),new Text("city"+split[3]));
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
            Vector<String> dataArr = new Vector<String>();
            Vector<String> cityArr = new Vector<String>();


            //整合数据
            for (Text value : values) {
                //转换为字符串，方便判断
                String line = value.toString();
                if(line.startsWith("dianxin")){
                    //去除标记
                    dataArr.add(line.substring(7));
                }else if(line.startsWith("city")){
                    cityArr.add(line.substring(4));
                }
            }

            //开始拼接数据
            //key是城市名称，value为信息
            for (int i = 0; i < dataArr.size(); i++) {

                for (int j = 0; j < cityArr.size(); j++) {

                    context.write(new Text(key+","+cityArr.get(j)+","),new Text(dataArr.get(i)));
                }
            }

        }
    }


}
