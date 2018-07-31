package Day7_28;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class Test01 {
    public static void main(String[] args) throws URISyntaxException, IOException {
        //建立连接
        URI uri = new URI("hdfs://192.168.41.200:9000");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf);


        //创建文件
        //fs.create(new Path("/a.txt"));

        //删除文件
       // fs.delete(new Path("/a.txt"),true);

        //读取目录
//        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
//        for (FileStatus fileStatus : fileStatuses) {
//            System.out.println(fileStatus.getPath());
//        }

        //读取文件
//        FSDataInputStream open = fs.open(new Path("/ZooKeeper.txt"));
//        BufferedReader br = new BufferedReader(new InputStreamReader(open,"UTF-8"));
//        String line =null;
//        while((line=br.readLine())!=null){
//            System.out.println(line);
//        }
//        open.close();
//        br.close();

        //上传文件
//        FSDataOutputStream out = fs.create(new Path("/mvc架构.png"));
//        FileInputStream in = new FileInputStream("F:\\bigdata笔记\\mvc架构.png");
//        IOUtils.copyBytes(in,out,conf,true);


        //下载文件
        FSDataInputStream in = fs.open(new Path("/mvc架构.png"));
        FileOutputStream out = new FileOutputStream("F:\\bigdata笔记\\2.png");
        IOUtils.copyBytes(in,out,1024,true);

    }
}
