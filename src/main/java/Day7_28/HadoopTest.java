package Day7_28;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopTest {
    public static void main(String[] args) throws URISyntaxException, IOException {

        //获得连接
        URI uri = new URI("hdfs://master:9000");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(uri, conf);

        //创建文件
        //fs.create(new Path("/3.txt"));

        //创建目录
        //fs.mkdirs(new Path("/idea"));

        //删除文件或目录
//        fs.delete(new Path("/3.txt"),true);
//        fs.delete(new Path("/idea"),true);

        //=========================================================
        //查看文件
//        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
//        for (FileStatus fileStatus : fileStatuses) {
//            System.out.println(fileStatus);
//        }

        //-=============================================
        //读取文件
//        FSDataInputStream open = fs.open(new Path("/22.txt"));
//        BufferedReader br = new BufferedReader(new InputStreamReader(open,"UTF-8"));
//        String line =null;
//        while((line=br.readLine())!=null){
//            System.out.println(line);
//        }
//        br.close();
//        open.close();

        //---------=======================
        //上传文件
        //创建文件接收
//        FSDataOutputStream fos = fs.create(new Path("/ZooKeeper.txt"));
//        //读取文件
//        FileInputStream fis = new FileInputStream("F:\\bigdata笔记\\ZooKeeper.txt");
//        //上传
//        IOUtils.copyBytes(fis,fos,conf,true);

        //==================================
        //下载文件
        //打开文件
        FSDataInputStream open = fs.open(new Path("/ZooKeeper.txt"));
        //创建文件接收
        FileOutputStream fos = new FileOutputStream("F:\\bigdata笔记\\ZooKeeper2.txt");
        IOUtils.copyBytes(open,fos,1024,true);


    }
}
