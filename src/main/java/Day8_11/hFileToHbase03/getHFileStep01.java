package Day8_11.hFileToHbase03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/11 20:03
 *
 * 生成hfile文件
 */
public class getHFileStep01 {


    public static class myMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,KeyValue>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //读取到的每行数据切割
            String[] line = value.toString().split("\t");

            //创建rowkey,也就是word作为rowkey
            byte[] rowkey = line[0].getBytes();
            //转换为输出数据格式
            ImmutableBytesWritable k = new ImmutableBytesWritable(rowkey);

            //创建一列数据的格式
            KeyValue keyValue = new KeyValue(rowkey, "cf1".getBytes(), "count".getBytes(), line[1].getBytes());

            //输出
            context.write(k,keyValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "getHFile");

        //设置map类
        job.setMapperClass(myMapper.class);
        //设置reducer类
        //会根据rowkey进行排序
        job.setReducerClass(KeyValueSortReducer.class);

        //设置输出数据类型
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        //设置分区
        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

        //需要设置hfile的具体格式，所以需要引入表
        HTable table = new HTable(conf, "hfileTb");
        HFileOutputFormat2.configureIncrementalLoad(job,table);

        //设置输入路径
        //read的输出文件路径
        FileInputFormat.addInputPath(job,new Path("/mapReducerOut/hbaseout/wc/read"));

        //设置输出路径
        //hFile的存放路径
        FileOutputFormat.setOutputPath(job,new Path("/mapReducerOut/hbaseout/wc/HFile"));

        job.waitForCompletion(true);
    }
}
