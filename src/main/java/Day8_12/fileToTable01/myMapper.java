package Day8_12.fileToTable01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/12 10:41
 */
public class myMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split(",");
        for (String str : strs) {
            context.write(new Text(str),new IntWritable(1));
        }

    }
}
