package Day8_11.wc01;

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
 * @date 2018/8/11 15:17
 */
public class wcJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置jar类
        job.setJarByClass(wcJob.class);
        job.setJobName("word count");

        //设置map类
        job.setMapperClass(wcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置hbase输出reduce类
        //前提表存在
        TableMapReduceUtil.initTableReducerJob("wctb1",wcReducer.class,job,null,null,null,null,false);

        //设置输入路径
        Path inPath = new Path("/upData/wordcount.txt");
        FileInputFormat.addInputPath(job,inPath);

        boolean flag = job.waitForCompletion(true);
        if(flag){
            System.out.println("wc run success!!");
        }


    }
}
