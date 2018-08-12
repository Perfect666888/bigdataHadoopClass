package Day8_10;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * @author Perfect
 * @date 2018/8/10 20:33
 */
public class HBaseDemo02 {
    //创建表名
    String tName = "mobile";
    //创建管理类
    HBaseAdmin hBaseAdmin;
    //创建table类
    HTable hTable;

    //创建random类
    Random r = new Random();

    /**
     * 随机生成手机号
     */

    public String getPhone(String prefix) {
        return prefix + String.format("%08d", r.nextInt(999999));
    }

    /**
     * 随机生成日期
     */
    public String getDate(String year) {
        return year + String.format("%02d%02d%02d%02d%02d",
                r.nextInt(12) + 1, r.nextInt(29) + 1,
                r.nextInt(24), r.nextInt(60), r.nextInt(60));
    }

    /**
     * 随机生成日期
     */
    public String getDayDate(String pref) {
        return pref + String.format("%02d%02d%02d", r.nextInt(24), r.nextInt(60), r.nextInt(60));

    }

    /**
     * 初始化配置
     */

    @Before
    public void begin() throws IOException {
        //创建配置信息类
        Configuration conf = new Configuration();
        //配置zk地址
        conf.set("hbase.zookeeper.quorum", "192.168.41.200,192.168.41.201,192.168.41.202");

        //初始化hbase管理类
        hBaseAdmin = new HBaseAdmin(conf);
        //初始化table类
        hTable = new HTable(conf, tName);
    }

    /**
     * 最后需要释放资源
     */
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
        //判断表存在不存在
        if (hBaseAdmin.tableExists(tName)) {
            //存在就让表失效
            hBaseAdmin.disableTable(tName);
            //删除表
            hBaseAdmin.deleteTable(tName);
        }

        //创建表描述
        HTableDescriptor htDesc = new HTableDescriptor(TableName.valueOf(tName));

        //创建列族描述
        HColumnDescriptor cfDesc = new HColumnDescriptor("cf1");
        //设置缓存
        cfDesc.setInMemory(true);
        //设置版本号
        cfDesc.setMaxVersions(2);

        //增加列族描述
        htDesc.addFamily(cfDesc);

        //创建表
        hBaseAdmin.createTable(htDesc);
        System.out.println("创建表完成");
    }

    /**
     * 插入数据
     */

    @Test
    public void insertDB() throws Exception {

        //设置rowkey
        //格式
        //手机号_时间
        String phoneNum = getPhone("138");
        String time = getDate("2018");
        String rowkey = phoneNum + "_" + time;

        //创建put类，用于存储数据,
        Put put = new Put(rowkey.getBytes());

        //存储手机号
        put.add("cf1".getBytes(), "phoneNum".getBytes(), phoneNum.getBytes());
        //存储时间
        put.add("cf1".getBytes(), "time".getBytes(), time.getBytes());
        //存对端手机号
        put.add("cf1".getBytes(), "dpNum".getBytes(), getPhone("136").getBytes());
        //存储类型
        put.add("cf1".getBytes(), "type".getBytes(), Bytes.toBytes(r.nextInt(2)));

        //存储数据
        hTable.put(put);
        System.out.println("插入数据完成");

    }

    /**
     * 查询数据，指定rowkey
     */

    @Test
    public void get() throws IOException {
        //创建get对象
        Get get = new Get("13800636928_20180502222433".getBytes());

        //查询指定列
        get.addColumn("cf1".getBytes(), "time".getBytes());
        //执行查询
        Result rs = hTable.get(get);
        //获得对应的单元格数据
        Cell cell = rs.getColumnLatestCell("cf1".getBytes(), "time".getBytes());

        //转换为字符串输出
        System.out.println("=================================");
        System.out.println(new String(CellUtil.cloneValue(cell)));
        System.out.println("===================================");
    }

    /**
     * 批量插入数据
     * 10个用户
     * 每个用户10条数据
     */

    @Test
    public void insertDBs() throws Exception {
        //创建集合存储put对象
        ArrayList<Put> puts = new ArrayList<Put>();

        //利用循环，生成数据
        for (int i = 0; i < 10; i++) {
            //创建10个用户
            String phoneNum = getPhone("13" + i);

            //生成数据
            for (int j = 0; j < 10; j++) {
                //获得时间
                String time = getDate("2018");

                //设置时间格式
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

                //创建rowkey,按照时间做降序
                String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(time).getTime());

                //创建put类对象，用于存储数据,
                Put put = new Put(rowkey.getBytes());

                //存储手机号
                put.add("cf1".getBytes(), "phoneNum".getBytes(), phoneNum.getBytes());
                //存储时间
                put.add("cf1".getBytes(), "time".getBytes(), time.getBytes());
                //存对端手机号
                put.add("cf1".getBytes(), "dpNum".getBytes(), getPhone("13" + j).getBytes());
                //存储类型
                put.add("cf1".getBytes(), "type".getBytes(), (r.nextInt(2) + "").getBytes());

                //添加put对象到集合
                puts.add(put);
            }
        }
        //插入数据
        hTable.put(puts);
    }


    /**
     * 范围查找
     * 查找  13900231570 2018年全年的通话记录
     */
    @Test
    public void scanDBs() throws Exception {
        //创建扫描器
        Scan scan = new Scan();

        //设置时间格式
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

        //范围查找
        //开始rowkey
        String startRowkey = "13900231570" + "_" + (Long.MAX_VALUE - sdf.parse("20190101000000").getTime());
        //结束rowkey
        String endRowkey = "13900231570" + "_" + (Long.MAX_VALUE - sdf.parse("20180101000000").getTime());



        //设定开始key
        scan.setStartRow(startRowkey.getBytes());
        //设定结束key
        scan.setStopRow(endRowkey.getBytes());

        //查不到数据
        //开始扫描,获得结果
        ResultScanner rss = hTable.getScanner(scan);

        System.out.println("=================================范围查找======================================");
        for (Result rs : rss) {

            Cell cellPhoneNum = rs.getColumnLatestCell("cf1".getBytes(), "phoneNum".getBytes());
            Cell cellTime = rs.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
            Cell cellDpNum = rs.getColumnLatestCell("cf1".getBytes(), "dpNum".getBytes());
            Cell cellType = rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes());

            System.out.println(new String(CellUtil.cloneValue(cellPhoneNum)) + "--"
                    + new String(CellUtil.cloneValue(cellTime)) +
                    "--" + new String(CellUtil.cloneValue(cellDpNum)) + "--"
                    + new String(CellUtil.cloneValue(cellType)));
        }
    }


    /**
     * 范围查找
     * 查找  13900231570 主叫类型的通话记录
     */

    @Test
    public void scanDBs2() throws IOException {
        //创建扫描器
        Scan scan = new Scan();
        //创建过滤器集合
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        PrefixFilter filter1 = new PrefixFilter("13900231570".getBytes());
        //增加列过滤条件
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter("cf1".getBytes(), "type".getBytes(),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1"));

        //添加到过滤集合中
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);

        //将过滤器添加到扫描器中
        scan.setFilter(filterList);

        System.out.println("=============================过滤查找=======================================");
        //执行扫描
        ResultScanner rss = hTable.getScanner(scan);
        for (Result rs : rss) {
            System.out.println(new String(CellUtil.cloneValue(rs
                    .getColumnLatestCell("cf1".getBytes(), "phoneNum".getBytes())))
                    + "  "
                    + new String(CellUtil.cloneValue(rs.getColumnLatestCell(
                    "cf1".getBytes(), "time".getBytes())))
                    + "  "
                    + new String(CellUtil.cloneValue(rs.getColumnLatestCell(
                    "cf1".getBytes(), "dpNum".getBytes())))
                    + "  "
                    + new String(CellUtil.cloneValue(rs.getColumnLatestCell(
                    "cf1".getBytes(), "type".getBytes()))));

            //一加type列就报空指针异常
        }

    }

}
