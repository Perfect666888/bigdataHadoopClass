package Day8_12.fileToTable01;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;


import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/12 10:41
 */
public class myReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //统计变量
        int sum =0;
        for (IntWritable value : values) {

            sum+=value.get();
        }

        //创建put对象
        //key是word，也就是rowkey
        Put put = new Put(key.getBytes());

        //添加数据
        put.add("cf1".getBytes(),"count".getBytes(),
                String.valueOf(sum).getBytes());


        //写出数据
        context.write(null,put);

    }
}

