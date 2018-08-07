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

public class RunJob3 {
    public static enum myEnum {
        my;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置
        Configuration conf = new Configuration();

        //创建运行次数统计变量
        int count = 0;

        //设置收敛底线
        double baseline = 0.1;

        while (true) {
            //统计++
            count++;
            conf.setInt("tab", count);

            //创建job
            Job job = Job.getInstance(conf, "pr" + count);

            //指定jar类
            job.setJarByClass(RunJob3.class);
            //指定mapper类
            job.setMapperClass(myMapper.class);
            //指定mapper输出数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //指定reducer类
            job.setReducerClass(myReducer.class);
            //指定数据输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //指定输入路径，每次都不一样,所以进行判断
            Path inPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Data\\pagerankData3.txt");
            if (count > 1) {
                //运行多次，输出变输入
                inPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Out3\\PROut" + (count - 1));
            }
            FileInputFormat.addInputPath(job, inPath);
            //设置输出路径
            //为了避免报错需要先判断路径是否存在
            FileSystem fs = FileSystem.get(conf);
            Path outPath = new Path("F:\\BigData\\IDEA\\bigdataHadoopClass\\Out3\\PROut" + count);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            //设置输入文本格式类型
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            //添加任务
            boolean flag = job.waitForCompletion(true);

            //求平均差值和判断
            if (flag) {

                //获得差值和
                long prSum = job.getCounters().findCounter(myEnum.my).getValue();

                //求出平均差值
                double prAvg = (double) (prSum / 4000.0);

                //判断
                if (prAvg<baseline){
                    System.out.println("在第"+count+"次运行后，完成收敛");
                    break;
                }

            }

        }
    }


    //自定义mapper类
    public static class myMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //创建对象去存储值
            Node3 node3 = null;

            //第几次运行
            int tab = context.getConfiguration().getInt("tab", 1);
            if (tab == 1) {
                //第一次，运行需要添加pr值
                node3=Node3.setNode3("1.0"+"\t"+ value.toString());
            }else {
                node3=Node3.setNode3(value.toString());
            }

            //输出
            context.write(key, new Text(node3.toString()));

            //有指向需要平均分配pr值
            if (node3.pdInformations()) {
                //平均pr值
                double prAvg = node3.getPageRank() / node3.getInformations().length;
                //获得每个指向的信息
                for (int i = 0; i < node3.getInformations().length; i++) {
                    context.write(new Text(node3.getInformations()[i]), new Text(String.valueOf(prAvg)));
                }
            }
        }
    }


    //自定义reducer类
    public static class myReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //创建pr总和变量
            double prSum = 0.0;
            //创建变量存储有指向的数据
            Node3 sourceNode3 = null;

            //遍历迭代器
            for (Text value : values) {
                //临时node3变量存储数据
                Node3 tmpNode3 = Node3.setNode3(value.toString());

                //判断是否有指向信息，有就存储，并且不累加pr值
                if (tmpNode3.pdInformations()) {
                    sourceNode3 = tmpNode3;
                } else {
                    prSum += tmpNode3.getPageRank();
                }
            }

            //求出newpr值，有阻尼值
            double newPR = (1 - 0.85) / 4.0 + 0.85 * prSum;

            //求出差值
            double dDPR = newPR - sourceNode3.getPageRank();
            //转换为int方便累加,累加时需要long类型，防止丢失精度在*1000
            int iDPR = (int) (dDPR * 1000.0);
            //可能为负数，取绝对值
            iDPR = Math.abs(iDPR);

            //差值累加
            context.getCounter(myEnum.my).increment(iDPR);

            //把newpr赋值到源数据
            sourceNode3.setPageRank(newPR);
            //输出
            context.write(key,new Text(sourceNode3.toString()));


        }
    }


}
