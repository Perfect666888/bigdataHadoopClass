package Day8_11.hFileToHbase03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author Perfect
 * @date 2018/8/11 20:41
 */
public class hFileToHbase02 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] dfsArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


        HTable table = new HTable(conf, "hfileTb");

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

        loader.doBulkLoad(new Path("/mapReducerOut/hbaseout/wc/HFile"),table);


    }

}
