package Day7_30;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UpData {

    public static void main(String[] args) throws URISyntaxException, IOException {

        //创建连接
        URI uri = new URI("hdfs://master:9000");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf);

        //上传文件，
        //创建文件接收
        FSDataOutputStream out = fs.create(new Path("/dianxin_data"));
        FileInputStream in = new FileInputStream("F:\\BigData\\Hadoop_note\\7_30\\dianxin_data");
        IOUtils.copyBytes(in,out,1024,true);


        FSDataOutputStream out2 = fs.create(new Path("/city_id.txt"));
        FileInputStream in2 = new FileInputStream("F:\\BigData\\Hadoop_note\\7_30\\city_id.txt");
        IOUtils.copyBytes(in2,out2,1024,true);


    }
}
