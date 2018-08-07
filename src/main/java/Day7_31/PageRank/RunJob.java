package Day7_31.PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class RunJob {

    public static enum myEnum {
        my;
    }

    public static void main(String[] args) {
        //创建配置
        Configuration conf = new Configuration();

        //设定收敛值
        double baseline = 0.1;
        //统计运行次数变量
        int count = 0;

        //不知道需要运行几次，所以使用死循环
        while (true) {
            try {
                //统计变量++
                count++;

                //获得文件列表
                FileSystem fs = FileSystem.get(conf);

                //创建Job
                Job job = Job.getInstance(conf, "pr" + count);

                //设置jarclass
                job.setJarByClass(RunJob.class);

                //设置mapper
                job.setMapperClass(myMapper.class);
                //设置mapper输出数据类型
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                //设置reducer
                job.setReducerClass(myReducer.class);

                //设置输出数据类型
                job.setOutputKeyClass(Text.class);
                job.setOutputKeyClass(Text.class);

                //设置输入文件路径
                //第一次的输入路径
                Path inPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Data\\pageRankData.txt");
               // Path inPath = new Path("/mapReducerData/pageRankData.txt");
                //上一次的输出路径，是下二次的输入路径
                //进行判断
                if (count > 1) {
                    inPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Out\\PROut" + (count - 1));
                    //inPath = new Path("/mapReducerOut/PROut" + (count - 1));

                }
                //设置路径
                FileInputFormat.addInputPath(job, inPath);
                //不清楚要运行几次，所以每次的输出路径都不一样
                Path outPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Out\\PROut" + count);
                //Path outPath = new Path("/mapReducerOut/PROut" + count);
                //路径有可能存在，要先判断删除
                if (fs.exists(outPath)) {
                    fs.delete(outPath);
                }
                FileOutputFormat.setOutputPath(job, outPath);

                //设置输入文本类型
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                //添加job
                boolean flag = job.waitForCompletion(true);

                //判断什么时候退出
                if (flag) {
                    System.out.println(count + "次运行成功");

                    //获得差值和
                    long czSum = job.getCounters().findCounter(myEnum.my).getValue();

                    //求出平均值
                    //4代表key的数量
                    double czAvg = czSum / 4000.0;

                    //判断是否小于底线
                    if (czAvg < baseline) {
                        //小于跳出循环，结束
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


    //定义自定义mapper类
    public static class myMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            //转换为字符串，方便切割
            String line = value.toString();
            //创建对象去存放数据
            Node node = Node.setNode(line);
            //结果
            //值   信息

            //输出
            context.write(key, new Text(node.toString()));

            //信息不为null时，开始计算值
            if (node.pdInformations()) {
                //求出值
                double avgScore = node.getRankScore() / node.getInformations().length;
                for (int i = 0; i < node.getInformations().length; i++) {
                    //求出key
                    String key2 = node.getInformations()[i];
                    //输出
                    context.write(new Text(key2), new Text(String.valueOf(avgScore)));
                }
            }

        }
    }

    //定义自定义reducer类
    public static class myReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //设置总和变量
            double sum = 0;
            //创建node对象存储原数据
            Node sourceNode = new Node();

            //遍历迭代器
            for (Text value : values) {
                //定义一个临时的Node对象，去获取数据
                Node node = Node.setNode(value.toString());

                //当informations不为null是，就说明是原数据
                if (node.pdInformations()) {
                    sourceNode = node;
                } else {
                    //对值进行累加，非原数据
                    sum += node.getRankScore();
                }
            }

            //获得新的pr值
            double newPr = ((1 - 0.85) / 4.0) + 0.85 * sum;

            //获取差值
            double oldPr = sourceNode.getRankScore();
            double prC = newPr - oldPr;
            //转换为int，为了防止数据丢失精度，进行*1000
            int cz = (int) (prC * 1000.0);
            //可能为负数,需要取绝对值
            cz = Math.abs(cz);

            //不同key的差值，进行累加，方便取平均
            context.getCounter(myEnum.my).increment(cz);

            //附新的RankScore给原数据
            sourceNode.setRankScore(newPr);

            //输出结果
            context.write(key, new Text(sourceNode.toString()));

        }
    }
}
