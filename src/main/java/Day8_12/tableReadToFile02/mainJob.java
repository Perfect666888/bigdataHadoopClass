package Day8_12.tableReadToFile02;

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
 * @date 2018/8/12 10:59
 *
 * 从表中读取文件，输出到mapper结果
 *
 */
public class mainJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建hbaseconf配置文件
        Configuration hBaseConf = HBaseConfiguration.create();

        //指定zookeeper
        hBaseConf.set("hbase.zookeeper.quorum",
                "192.168.41.200,192.168.41.201,192.168.41.202");
        //指定端口
        hBaseConf.set("hbase.zookeeper.property.clientPort","2181");

        //设置读取的表名
        hBaseConf.set("hbase.mapreduce.inputtable","wctb2");
        //设置超时时间
        hBaseConf.set("dfs.socker.timout","18000");

        //创建扫描器
        Scan scan = new Scan();

        //设置缓存
        scan.setCaching(1024);
        scan.setCacheBlocks(false);

        //创建job任务
        Job job = new Job(hBaseConf, "readJob");

        //指定mapreducer表名和map类
        TableMapReduceUtil.initTableMapperJob("wctb2".getBytes(),scan,
                myMapper.class,NullWritable.class,Text.class,job);

        //设置输出路径,会自动创建
        String outPath="/hbaseTest/readOut";
        FileOutputFormat.setOutputPath(job,new Path(outPath));


        //添加进程
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("========================");
            System.out.println("success");
            System.out.println("========================");
        }


    }

}
