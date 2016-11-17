package znode;

//cc CreateGroup A program to create a znode representing a group in ZooKeeper

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

// vv CreateGroup
public class CreateGroup implements Watcher {

	private static final int SESSION_TIMEOUT = 5000;

	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException {
		
		/** @param this -> Watcher 回调，当 ZooKeeper 异步创建成功以后回调该接口的 Watcher#process() 方法**/
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);

		connectedSignal.await();
	}

	/**
	 * 当 ZooKeeper 创建成功以后，会回调该 Watcher 接口的 process 方法
	 */
	@Override
	public void process(WatchedEvent event) { // Watcher interface
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}

	/**
	 * 创建 group
	 * @param groupName
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void create(String groupName) throws KeeperException, InterruptedException {
		
		String path = "/" + groupName;
		
		/** CreateMode.PERSISTENT 创建一个永久的节点  **/
		String createdPath = zk.create(path, null/* data */, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		System.out.println("Created " + createdPath);
		
	}

	public void close() throws InterruptedException {
		zk.close();
	}

	public static void main(String[] args) throws Exception {

		CreateGroup createGroup = new CreateGroup();
		
		String zookeeperAddr = "localhost";
		
		String groupName = "zoo";
		
		createGroup.connect( zookeeperAddr ); // 默认会使用 zookeeper 2181 端口
		
		createGroup.create( groupName );
		
		createGroup.close();
	}
}
// ^^ CreateGroup
