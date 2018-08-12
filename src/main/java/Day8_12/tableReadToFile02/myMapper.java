package Day8_12.tableReadToFile02;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/12 11:00
 */
public class myMapper extends TableMapper<NullWritable,Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //可能不止一列,获得每个单元格的集合并遍历
        for (Cell cell : value.listCells()) {
            //获得rowkey
            String rowkey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");

            //获得columvalue
            String columValue = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), "UTF-8");

            //拼接出输出的value
            String line =rowkey+"\t"+columValue;

            //输出到文件
            context.write(NullWritable.get(),new Text(line));

        }

    }
}
