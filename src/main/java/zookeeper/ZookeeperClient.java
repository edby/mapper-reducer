package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.util.List;


public class ZookeeperClient {

    ZooKeeper zk=null;

    public ZookeeperClient ()throws Exception{
        /*
         * 构造一个zookeeper客户端对象
         *sessionTimeout会话超
         * watcher   监控通知的处理逻辑
         * */
        zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, null);

    }

    @Test
    public  void testCreate() throws Exception{

        /*
        * 创建
        * 参数1：要创建的节点路径
        * 参数2：数据
        * 参数3：访问权限  ZooDefs.Ids.OPEN_ACL_UNSAFE  指定权限
        * 参数4：节点类型  CreateMode.PERSISTENT   指定节点类型，持久化节点
        * */
        String create = zk.create("/test", "hello".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println(create);
        zk.close();

    }


    @Test
    public void testUpdate() throws Exception{

        /*
        * 修改
        * 参数1 : 路径
        * 参数2： 数据
        * 参数3： 要修改的版本，-1代表任何版本
        * */
        zk.setData("/test", "我爱你".getBytes("UTF-8"),-1);
        zk.close();


    }

      @Test
    public void getData() throws Exception{

        /*
         * 获取
         * 参数一 : 路径
         * 参数2：是否监听
         * 参数3：要获取数据的版本，null是最新版本
         * */
        byte[] data = zk.getData("/test", false, null);

        System.out.println(new String(data,"UTF-8"));

        zk.close();


    }

    @Test
    public void testListChildren() throws Exception{

        /*
         * 获取
         * 参数1 : 节点路径
         * 参数2：是否监听
         *
         * 注意返回的节点中，只有子节点名字，不带全路径
         * */
        List<String> children = zk.getChildren("/aa", false);
        for (String child : children) {

            System.out.println(child);
        }

        zk.close();

    }



    @Test
    public void rm() throws Exception{

        /*
         * 删除
         * 参数1 : 节点路径
         * 参数2：是否监听
         *
         * 注意返回的节点中，只有子节点名字，不带全路径
         * */
        zk.delete("/test",-1);
        zk.close();

    }


}
