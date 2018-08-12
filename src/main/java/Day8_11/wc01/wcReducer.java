package Day8_11.wc01;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;


/**
 * @author Perfect
 * @date 2018/8/11 15:21
 */
public class wcReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //设置统计变量
        int sum =0;
        for (IntWritable value : values) {
            sum+= value.get();
        }


        //创建put对象，
        Put put = new Put(key.getBytes());

        //创建数据,并添加数据
        put.add("cf1".getBytes(),"count".getBytes(),String.valueOf(sum).getBytes());

        //插入数据到表
        context.write(null,put);


    }
}
