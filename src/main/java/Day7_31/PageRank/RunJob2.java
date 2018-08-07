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

public class RunJob2 {
    public static enum myEnum {
        my;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建配置
        Configuration conf = new Configuration();
        //创建收敛底线
        double baseLine = 0.1;

        //创建统计循环次数变量
        int count = 0;

        //不清楚循环几次，所以设置死循环
        while (true) {
            //进入一次就对次数+1
            count++;

            //创建job
            Job job = Job.getInstance(conf, "PR" + count);

            //设置jarclass
            job.setJarByClass(RunJob2.class);
            //设置mapper类
            job.setMapperClass(myMapper.class);
            //设置mapper输出数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //设置reducer类
            job.setReducerClass(myReduce.class);
            //设置输出数据类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //设置输入文本类型 ,会自动把每行的第一个\t前的字符作为key
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            //设置输入文本路径
            Path inPath = null;
            //第一是原数据
            if (count == 1) {
                inPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Data\\pageRankData.txt");
            } else {
                //count不会是0，所以不用担心出现负数
                //第二次的输入路径是第一次循环后输出的路径
                inPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Out2\\PROut" + (count - 1));
            }
            FileInputFormat.addInputPath(job, inPath);
            //设置输出路径,每次输出路径都不一样
            Path outPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Out2\\PROut" + count);
            //要先判断路径是否存在，存在的话，需要先删除，否则会报错
            //获得目录
            FileSystem fs = FileSystem.get(conf);
            //判断
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            //设置输出路径
            FileOutputFormat.setOutputPath(job, outPath);

            //添加job到进程
            boolean flag = job.waitForCompletion(true);

            //到这里一次mapReduce已经结束，已经有结果
            //需要判断是否达到收敛底线
            if (flag) {

                //获得key的差值和
                long prSum = job.getCounters().findCounter(myEnum.my).getValue();

                //求出平均差值
                double prAvg = (prSum / 4000.0);

                //是否到达收敛底线
                if (prAvg < baseLine) {
                    //到达就退出
                    System.out.println("在运行了" + count + "次后,完成收敛");
                    break;
                }
            }


        }


    }

    //自定义mapper类
    public static class myMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //创建Node2对象去存储数据
            //1.减少主体的代码数量
            //2.方便使用
            Node2 node2 = Node2.setNode(value.toString());

            //先把原数据输出
            context.write(key, new Text(node2.toString()));

            //当informations不为null时，就说明该page有指向其他的page，所以需要平均PR给其他page
            if (node2.pdInformations()) {
                //平均pr值
                Double prAvg = node2.getRankScore() / node2.getInformations().length;
                //遍历informations，获得指向的page
                for (int i = 0; i < node2.getInformations().length; i++) {
                    //输出
                    context.write(new Text(node2.getInformations()[i]), new Text(String.valueOf(prAvg)));
                }
            }
        }
    }


    public static class myReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //需要统计每个page的pr总和
            //设置统计变量
            double prSum = 0.0;
            //创建变量存储原数据，也就是原本数据的每行内容
            //方便计算差值
            Node2 sourceNode2 = new Node2();

            //遍历迭代器的内容
            for (Text value : values) {
                //创建临时node2存储数据
                Node2 tmpNode2 = Node2.setNode(value.toString());

                //判断是否为原数据，即informations不为null
                if (tmpNode2.pdInformations()) {
                    //储存源数据
                    sourceNode2 = tmpNode2;
                }else {
                    //对pr值进行累加
                    prSum += tmpNode2.getRankScore();
                }
            }

            //根据计算公式，计算出新的pr值
            double newPR = ((1 - 0.85) / 4.0) + 0.85 * prSum;

            //计算差值
            // 第一个d代表数据类型,后3位代表名
            double dDPR = newPR - sourceNode2.getRankScore();
            //转换为int，累加时需要long类型，为了防止丢失精度，*1000
            //方便累加
            int iDPR = (int) (dDPR * 1000);
            //可能为负数，需要取绝对值
            iDPR= Math.abs(iDPR);


            //全部key的差值累加
           context.getCounter(myEnum.my).increment(iDPR);


            //把newPR赋值给原数据
            sourceNode2.setRankScore(newPR);
            //输出
            context.write(key, new Text(sourceNode2.toString()));


        }
    }


}
