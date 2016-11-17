package znode;


//cc ConnectionWatcher A helper class that waits for the connection to ZooKeeper to be established
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

// vv ConnectionWatcher
/**
 * 将连接 zookeeper 的部分抽象出来了。
 * 
 * 之所以命名为 Watcher，是因为连接 zookeeper 是异步的，所以需要注入一个 Watcher 对象来进行回调。
 * 
 * @author mac
 *
 */
public abstract class AbstractConnectionWatcher implements Watcher {

	private static final int SESSION_TIMEOUT = 5000;

	protected ZooKeeper zk;
	
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException {
		
		zk = new ZooKeeper( hosts, SESSION_TIMEOUT, this );
		
		connectedSignal.await();
		
	}

	/**
	 * 当连接成功以后，进行回调。
	 */
	@Override
	public void process(WatchedEvent event) {
		
		if ( event.getState() == KeeperState.SyncConnected ) {
			
			connectedSignal.countDown();
			
		}
		
	}

	public void close() throws InterruptedException {
		
		zk.close();
		
	}
}
// ^^ ConnectionWatcher