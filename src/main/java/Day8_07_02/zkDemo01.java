package Day8_07_02;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Perfect
 * @date 2018/8/7 19:14
 */
public class zkDemo01 {
    public static void main(String[] args) {

        //阻塞变量
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = null;

        String constr = "192.168.41.200:2181,192.168.41.201:2181,192.168.41.202:2181";

        try {
            zooKeeper = new ZooKeeper(constr, 10000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {

                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        //关闭阻塞
                        countDownLatch.countDown();
                    }
                }
            });
            //阻塞
            countDownLatch.await();

            System.out.println("开始");
            //创建节点
            zooKeeper.create("/testjava","abcd".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            System.out.println("写完");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {
            if (zooKeeper != null) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
