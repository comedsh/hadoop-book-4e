package znode;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import common.ZNodeDeletion;
import common.ZooKeeperGenerator;

public class TestZNodeDeletion {
	
	/**
	 * 这里我选择采用 /dubbo 作为根节点来做删除测试，测试之前，要先创建好 /dubbo 的相关路径；创建的方法如下
	 * 
	 * skeleton-dubbo-spring-rpc/provider/provider-service/StartApplication.java|StartApplication2.java 启动服务以后，既可以注册 dubbo 的 zookeeper 服务
	 * 
	 * 不过，最好是在加上以前 TCC 的 dubbo zookeeper 服务，这样的话，/dubbo 根目录下可以有多级目录，测试用例就更为完善了.. 
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	@Test
	public void testDeleteNode() throws IOException, InterruptedException, KeeperException{
		
		ZooKeeper keeper = ZooKeeperGenerator.generate( "localhost", 5000 );
		
		ZNodeDeletion.delete( null, "/dubbo", keeper );
		
	}
	
	
}
