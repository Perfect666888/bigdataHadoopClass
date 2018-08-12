package Day8_10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;

/**
 * @author Perfect
 * @date 2018/8/10 18:59
 */
public class HBaseDemo01 {

    //设置表名
    String tbName = "javaHbase";
    //创建hbase管理类
    HBaseAdmin hBaseAdmin;
    //创建hbase表类
    HTable hTable;

    @Before
    public void begin() throws IOException {
        //创建配置信息
        Configuration conf = new Configuration();
        //配置ZooKeeper地址列表
        conf.set("hbase.zookeeper.quorum", "192.168.41.200,192.168.41.201,192.168.41.202");
        //初始化hbase admin类
        hBaseAdmin = new HBaseAdmin(conf);
        //初始化habse table类
        hTable = new HTable(conf, tbName);
    }

    @After
    public void end() throws IOException {
        if (hBaseAdmin != null) {
            hBaseAdmin.close();
        }
        if (hTable != null) {
            hTable.close();
        }
    }

    /**
     * 创建表
     */

    @Test
    public void createTable() throws IOException {

        //先判断这个张表是存在
        if (hBaseAdmin.tableExists(tbName)) {
            //存在就删除
            //断开连接
            hBaseAdmin.disableTable(tbName);
            //删除
            hBaseAdmin.deleteTable(tbName);
        }

        ///创建表描述
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tbName));

        //创建列族描述
        HColumnDescriptor cfdesc = new HColumnDescriptor("cf1");
        //设置缓存
        cfdesc.setInMemory(true);
        //设置最大版本
        cfdesc.setMaxVersions(2);
        //添加列族描述
        tableDesc.addFamily(cfdesc);

        //创建表
        hBaseAdmin.createTable(tableDesc);
    }

    /**
     * 插入数据
     */

    @Test
    public void insertTable() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        //创建rowkey
        String rowkey = "1";
        //创建put类
        Put line = new Put(rowkey.getBytes());
        //存储列1,name
        line.add("cf1".getBytes(), "name".getBytes(), "A01".getBytes());
        //储存列2,age
        line.add("cf1".getBytes(), "age".getBytes(), "10".getBytes());

        //添加数据到表
        hTable.put(line);
    }


    /**
     * 查询数据
     */
    @Test
    public void get() throws Exception {
        //创建get类，查询数据，rowkey作为参数
        Get get = new Get("1".getBytes());
        //查询指定列
        get.addColumn("cf1".getBytes(), "name".getBytes());
        //获得查询结果
        Result line = hTable.get(get);
        //获得一个单元格
        Cell cell = line.getColumnLatestCell("cf1".getBytes(), "name".getBytes());

        //转换为字符串，并且输出
        System.out.println("=======================================");
        System.out.println(new String(CellUtil.cloneValue(cell)));
        System.out.println("========================================");
    }


    /**
     * 批量插入数据
     */
    @Test
    public void insertTables() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        //创建集合存储put对象，一次性输出
        ArrayList<Put> puts = new ArrayList<Put>();

        for (int i = 2; i < 10; i++) {
            //创建rowkey
            String rowkey = String.valueOf(i);
            //创建put对象
            Put put = new Put(rowkey.getBytes());
            //创建name列的值
            String nameValue = "A0" + i;
            //创建age列的值
            String ageValue = "1" + i;
            //添加数据
            put.add("cf1".getBytes(), "name".getBytes(), nameValue.getBytes());
            put.add("cf1".getBytes(), "age".getBytes(), ageValue.getBytes());

            //添加到集合中
            puts.add(put);
        }

        hTable.put(puts);
    }


    /**
     * 范围查找
     */

    @Test
    public void scanTbs() throws IOException {
        //创建扫描器
        Scan scan = new Scan();
        //设置查找范围
        String startRowkey = "6";
        String endRowkey = "9";

        //设置开始key
        scan.setStartRow(startRowkey.getBytes());
        //设置结束key
        scan.setStopRow(endRowkey.getBytes());

        //执行扫描,获得结果
        ResultScanner rss = hTable.getScanner(scan);
        //遍历集合
        for (Result result : rss) {

            Cell cell1 = result.getColumnLatestCell("cf1".getBytes(), "name".getBytes());
            Cell cell2 = result.getColumnLatestCell("cf1".getBytes(), "age".getBytes());
            System.out.println(new String(CellUtil.cloneValue(cell1)) +"====="+ new String(CellUtil.cloneValue(cell2)));

        }
    }




}
