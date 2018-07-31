package Day7_28;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class Demo01 {
    public static void main(String[] args) throws URISyntaxException, IOException {
        //建立连接
        URI uri = new URI("hdfs://192.168.41.200:9000");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf);

        //显示文件存储位置
//        FileStatus fileStatus = fs.getFileStatus(new Path("/ZooKeeper.txt"));
//        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
//        for (BlockLocation fileBlockLocation : fileBlockLocations) {
//            System.out.println(fileBlockLocation);
//            String[] hosts = fileBlockLocation.getHosts();
//            System.out.println(hosts[0]);
//            System.out.println(hosts[1]);
//
//        }

        //读取目录
//        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
//        for (FileStatus fileStatus : fileStatuses) {
//            System.out.println(fileStatus);
//        }


        //删除文件
        System.out.println("===========删除文件=====");
        if (fs.exists(new Path("/test01"))) {
            boolean delete = fs.delete(new Path("/test01"));
            System.out.println(delete);
        }

        //创建文件
        System.out.println("===========创建文件==================");
        if(!fs.exists(new Path("1.txt"))){
            fs.create(new Path("1.txt"));
        }

        //读取文件
        System.out.println("==================读取文件=====================");
        FSDataInputStream open = fs.open(new Path("/ZooKeeper.txt"));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open, "UTF-8"));
        String str =null;
        while((str=bufferedReader.readLine())!=null){
            System.out.println(str);
        }
        //下载文件
        System.out.println("=================下载文件======================");
        FileOutputStream fileOutputStream = new FileOutputStream("F:\\bigdata笔记\\2.txt");
        IOUtils.copyBytes(open,fileOutputStream,1024,true);
        open.close();
        bufferedReader.close();

        //上传文件
        System.out.println("======================上传文件=======================");
        FSDataOutputStream out = fs.create(new Path("/视频.mkv"));
        FileInputStream in = new FileInputStream("I:\\法证先锋1\\法證先鋒.2006.EP01.双语字幕.TVRip.X264.mkv");
        IOUtils.copyBytes(in,out,1024,true);
        out.close();
        in.close();


    }
}
