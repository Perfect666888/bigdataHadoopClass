package Day8_11.wc01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/11 15:20
 */
public class wcMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split(",");

        for (String word : strs) {

            context.write(new Text(word),new IntWritable(1));
        }


    }
}
