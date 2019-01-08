package zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import java.util.List;

public class zookeerperWatch {
    ZooKeeper zk=null;

    public zookeerperWatch ()throws Exception{
        /*
         * 构造一个zookeeper客户端对象
         *sessionTimeout会话超
         * watcher   监控通知的处理逻辑
         * */
        zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000,  new Watcher() {
            @Override
            public void process(WatchedEvent event) {//event 收到事件的通知


                /*
                *
                * 避免事件构造的时候，收到一次事件
                * Event.KeeperState.SyncConnected  连接成功
                * Event.EventType.NodeDataChanged   数据改变的时候，调用逻辑
                *
                */
                if(event.getState() == Event.KeeperState.SyncConnected && event.getType()==Event.EventType.NodeDataChanged){//节点数据改变
                    System.out.println(event.getPath());//事件发生的节点路劲
                    System.out.println(event.getType()); //收到事件的类型
                    System.out.println("收到通知了，正在处理................"); //收到事件后，我们的处理逻辑

                    try {
                        zk.getData("/test",true, null);
                    } catch (KeeperException|InterruptedException e) {
                        e.printStackTrace();
                    }
                }else if(event.getState() == Event.KeeperState.SyncConnected && event.getType()==Event.EventType.NodeChildrenChanged){//子节点改变
                    System.out.println("子节点发生改变·............");
                }
            }
        });

    }

    @Test
    public void watch() throws Exception{

        /*
        *
        * 参数1：监听路径
        * 参数2：收到坚挺路劲后的处理逻辑
        *
        * */
        byte[] data = zk.getData("/test",true, null);   //监听节点数据变化
        List<String> children = zk.getChildren("/test", true); //监听节点的子节点变化事件


        System.out.println(new String(data,"UTF-8"));

            Thread.sleep(Long.MAX_VALUE);
    }

}
