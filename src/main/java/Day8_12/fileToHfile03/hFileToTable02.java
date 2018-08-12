package Day8_12.fileToHfile03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/12 15:04
 */
public class hFileToTable02 {
    public static void main(String[] args) throws Exception {
        //创建配置
        Configuration conf = HBaseConfiguration.create();
        String[] dfsArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //连接表
        HTable table = new HTable(conf, "hfileTb2");

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

        loader.doBulkLoad(new Path("/hbaseTest/hFileOut"),table);


    }

}
