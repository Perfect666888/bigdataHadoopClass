package Day8_10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @author Perfect
 * @date 2018/8/12 9:36
 */
public class HBaseDemo03Imp {

    //创建连接对接对象
    HConnection hTablePool=null;
    //创建配置文件,加入静态修饰，只允许修改一次
    static Configuration conf =null;

    //创建无参构造，随着对象的创建而创建
    //完成全局变量的初始化
    public HBaseDemo03Imp() {
        //初始化对象
        conf =new Configuration();
        //zookeeper地址
        String zk_list= "192.168.41.200,192.168.41.201,192.168.41.202";
        //配置配置文件
        conf.set("hbase.zookeeper.quorum",zk_list);
        //初始化连接
        try {
            hTablePool=HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //存储方法,数据到表
    public void save(Put put,String tableName){
        //创建表接口
        HTableInterface table =null;
        try {
            //初始化对象
            table=hTablePool.getTable(tableName);
            //上传文件
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            //释放资源
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 插入一个cell
     * 需要参数
     * 表名，rowkey columFamily colum value
     */

    public void insertCell(String tableName,String rowKey,String columFamily,
                       String columName,String value){
        //创建表接口
        HTableInterface table =null;
        try {
            //获得表
            table= hTablePool.getTable(tableName);

            Put put = new Put(rowKey.getBytes());
            //导入数据
            put.add(columFamily.getBytes(),columName.getBytes(),value.getBytes());
            //添加到表
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            //释放资源
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    /**
     * 插多个cell
     * 需要参数
     * 表名，rowkey columFamily
     * colum[] value[]
     * 注:需要一一对应,长度相同
     * 改进，也可以使用map
     * key       value
     *colum      value
     *
     */

    public void insertCells(String tableName,String rowKey,String columFamily,
                       String[] columNameS,String[] valueS){
        //创建表接口
        HTableInterface table =null;
        try {
            //获得表
            table= hTablePool.getTable(tableName);

            Put put = new Put(rowKey.getBytes());
            //导入数据
            for (int i = 0; i < columNameS.length; i++) {
                put.add(columFamily.getBytes(),columNameS[i].getBytes(),
                        valueS[i].getBytes());
            }
            //添加到表
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            //释放资源
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 指定rowkey获得数据
     */
    public Result getByRowKey(String tableName,String rowKey){

        HTableInterface table =null;
        //获得接收结果
        Result rs=null;
        try {
            table=hTablePool.getTable(tableName);
            Get get = new Get(rowKey.getBytes());
            rs =table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //返回结果
        return rs;
    }

    /**
     * 查询一行数据
     * 参数
     * tableName
     * rowKey
     * colums
     */




}
