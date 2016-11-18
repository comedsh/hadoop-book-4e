package common;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * 
 * 创建 ZooKeeper 实例
 * 
 * @author 商洋
 *
 * @createTime：Nov 18, 2016 11:01:13 AM
 * 
 */
public class ZooKeeperGenerator {

	public static ZooKeeper generate( String address, final int sessionTimeout) throws IOException, InterruptedException{
		
		final CountDownLatch latch = new CountDownLatch(1);
		
		ZooKeeper keeper = new ZooKeeper( address, sessionTimeout, new Watcher(){
			
			@Override
			public void process( WatchedEvent event ) {
				
				if ( event.getState() == KeeperState.SyncConnected ) {
					
					latch.countDown();
					
				}					
				
			}
			
		});
		
		latch.await();
		
		return keeper;
		
	}
	
}
