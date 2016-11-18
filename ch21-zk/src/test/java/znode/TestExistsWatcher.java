package znode;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import common.ZooKeeperGenerator;

public class TestExistsWatcher {

	ZooKeeper keeper;
	
	@Before
	public void before() throws IOException, InterruptedException, KeeperException{
		
		keeper = ZooKeeperGenerator.generate( "localhost", 5000 );
		
		if( keeper.exists("/test", false) != null ){
			
			keeper.delete("/test", -1);
			
		}
	}
	
	/**
	 * 测试 Exists Watcher 的处理逻辑，一旦成功捕获想要的事件，便会立即终止监听... 如果想要继续监听，则必须重新注册.. 可以参考用例 ConfigWatcher#process() 方法。
	 * 
	 * 如何测试，
	 * 
	 * Positive Case,
	 * 1. 先执行 testExistsWatcher()
	 * 2. 在执行 createRootPath()，创建 /test 根节点，这个时候，从 #1 的控制台可以立即看到 NodeCreated 事件返回，表明 /test 节点存在了..
	 * 
	 * Negative Case,
	 * 3. 在执行 deleteRootPath()
	 * 4. 在执行 createRootPath()，这时候，#1 的控制台不再打印任何的信息，表示 Watcher 已经不再工作 -> 印证了只要第一次监听到自己想要的事件，后续便不会再继续监听了.. 
	 * 
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void testExitsWatcher() throws KeeperException, InterruptedException{
		
		/**
		 * 一直监听，直到 /test 节点创建为止
		 */
		keeper.exists("/test", new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				
				System.out.println( event.getType() );
				
				/** 一旦成功监听到想要的事件，监听便会终止 **/
				if( event.getType() == EventType.NodeCreated ){
					
					System.out.println("/test get created");
					
				};
				
			}
			
		});
		
		Thread.sleep( Long.MAX_VALUE );
		
	}
	
	@Test
	public void createRootPath() throws KeeperException, InterruptedException{
		
		keeper.create("/test", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
		
	}
	
	@Test
	public void deleteRootPath() throws KeeperException, InterruptedException{
		
		keeper.delete("/test", -1);
		
	}
	
}
