package Day8_07_02;


import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;


/**
 * @author Perfect
 * @date 2018/8/7 20:58
 */
public class zkDemo {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        String constr = "192.168.41.200:2181,192.168.41.201:2181,192.168.41.202:2181";
        ZooKeeper zk = new ZooKeeper(constr, 10000, null);
        byte[] data = zk.getData("/testjava", true, new Stat());
        System.out.println(new String(data));

    }
}
