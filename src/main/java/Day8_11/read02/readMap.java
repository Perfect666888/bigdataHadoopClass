package Day8_11.read02;



import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/11 15:52
 */
public class readMap extends TableMapper<NullWritable,Text> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
       //遍历每一行的没列
        for (Cell cell : value.listCells()) {
            //获得rowkey，并且指定文件格式,如果源文件不是utf-8格式，就会乱码
            String rowkey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");

            //获得列族,虽然没用
            String cf = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), "UTF-8");

            //获得列名
            String columName = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), "UTF-8");

            //获得值
            String columValue = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), "UTF-8");

            //拼接出需要的格式
            String line =rowkey+"\t"+columValue;

            //输出
            context.write(NullWritable.get(),new Text(line));

        }

    }
}
