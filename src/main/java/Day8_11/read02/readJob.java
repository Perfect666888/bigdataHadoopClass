package Day8_11.read02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/11 15:52
 */
public class readJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建配置
        Configuration conf = HBaseConfiguration.create();
        //设置zookeeper
        conf.set("hbase.zookeeper.quorum","192.168.41.200,192.168.41.201,192.168.41.202");
        //设置端口
        conf.set("hbase.zookeeper.property.clientPort","2181");

        //设置读取那张表
        conf.set("hbase.mapreduce.inputtable","wctb1");
        //设置超时时间，避免超时自动退出影响job
        conf.set("dfs.socket.timeout","18000");

        //创建扫描器
        Scan scan = new Scan();

        //设置缓存
        scan.setCaching(1024);
        scan.setCacheBlocks(false);
        //读取的是全表，所以不使用范围查找，即设置startrowkey和endrowkey

        //创建job，并直接设置jobname
        Job job = new Job(conf, "ScanHbaseJob");

        //设置mapreducer读取的表名和指定map类
        TableMapReduceUtil.initTableMapperJob("wctb1".getBytes(),scan,
                readMap.class,NullWritable.class,Text.class,job);

        //输出路径
        String outPath ="/mapReducerOut/hbaseout/wc/read";
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(outPath));

        boolean flag = job.waitForCompletion(true);
        if(flag){
            System.out.println("read job success");
        }
    }
}
