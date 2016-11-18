package znode;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import common.ZooKeeperGenerator;

public class TestBasicZooKeeperBehaviors {

	ZooKeeper keeper;
	
	@Before
	public void before() throws IOException, InterruptedException, KeeperException{
		
		keeper = ZooKeeperGenerator.generate("localhost", 5000);
		
		if( keeper.exists("/test", false) != null ){
			
			keeper.delete("/test", -1); // 每次测试方法启动前，清理数据。
			
		}
		
	}
	
	@Test
	public void testTempraryNode() throws IOException, InterruptedException{
		
		try {
			
			keeper.create("/test/sample", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			
		} catch (KeeperException e) {

			e.printStackTrace();
			
			Assert.assertTrue("临时节点创建失败，子节点 /sample 必须创建在永久节点 /test 上", true);
			
		}
		
	}
	
	@Test
	public void testPermanentNode() throws IOException, InterruptedException{
		
		try {
			
			keeper.create("/test", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
			
		} catch (KeeperException e) {
			
			e.printStackTrace();
			
			Assert.assertFalse("no exception there, must create success", true);
			
		}
		
			
		try {
			// 临时节点必须创建在永久节点之下
			keeper.create("/test/sample", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL );
			
		} catch (KeeperException e) {
			
			e.printStackTrace();
			
			Assert.assertFalse("no exception there, must create success", true );
			
		}
		
		// 一次创建两级临时节点
		try {
			
			keeper.create("/test/test1/test2", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL );
			
		} catch (KeeperException e) {
			
			e.printStackTrace();
			
			Assert.assertTrue(" 一次不能创建多级临时节点 ~~~~", true);
		}
		
	}
	
}
