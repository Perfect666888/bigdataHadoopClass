package Day8_07_02;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Perfect
 * @date 2018/8/7 19:39
 */
public class zkDemo02 {
    public static void main(String[] args) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        String constr = "192.168.41.200:2181,192.168.41.201:2181,192.168.41.202:2181";

        ZooKeeper zk = null;

        try {
            zk = new ZooKeeper(constr, 10000, null);
            //同步绑定事件监听器
            zk.getData("/testjava", new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("事件被触发" + event.getType());
                }
            }, new Stat());

            //阻塞
            countDownLatch.await();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {

            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }
}
