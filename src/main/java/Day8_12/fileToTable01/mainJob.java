package Day8_12.fileToTable01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/12 10:38
 */
public class mainJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置
        Configuration conf = new Configuration();
        //创建job
        Job job = Job.getInstance(conf,"fileToTable");

        //配置jar类
        job.setJarByClass(mainJob.class);

        //设置map类
        job.setMapperClass(myMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducer类
        TableMapReduceUtil.initTableReducerJob("wctb2",myReducer.class,job,null,null,null,null,false);


        //设置输入路径
        Path path = new Path("/hbaseTest/dataIn/");
        FileInputFormat.addInputPath(job,path);

        //添加job到进程
        boolean flag = job.waitForCompletion(true);
        if(flag){
            System.out.println("======================");
            System.out.println("success");
            System.out.println("======================");
        }


    }
}
