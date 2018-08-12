package Day8_12.fileToHfile03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/12 14:37
 * <p>
 * 把file包装为hfile
 */
public class getHFile01 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建配置
        Configuration conf = HBaseConfiguration.create();
        //创建job
        Job job = Job.getInstance(conf, "fileToHFile");

        //设置map类
        job.setMapperClass(myMapper.class);
        //设置reducer类
        //不需要自己去写
        job.setReducerClass(KeyValueSortReducer.class);

        //设置输出类型
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        //设置分区
        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

        //引入表(指定hfiel的格式)
        //表需要先存在
        HTable table = new HTable(conf, "hfileTb2");
        HFileOutputFormat2.configureIncrementalLoad(job,table);


        //设置输入路径
        FileInputFormat.addInputPath(job,new Path("/hbaseTest/readOut"));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("/hbaseTest/hFileOut"));


        //添加到进程
        boolean flag = job.waitForCompletion(true);

        if (flag){
            System.out.println("=====================");
            System.out.println("success");
            System.out.println("=====================");
        }


    }


    //自定义map类
    public static class myMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //读取到的数据按照规律切割
            String[] line = value.toString().split("\t");

            //获得rowkey
            byte[] rowKey = line[0].getBytes();

            //获得值
            byte[] columValue = line[1].getBytes();

            context.write(new ImmutableBytesWritable(rowKey),
                    new KeyValue(rowKey,"cf1".getBytes(),
                            "count".getBytes(), columValue));


        }
    }

}
