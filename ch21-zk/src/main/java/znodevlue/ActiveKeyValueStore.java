package znodevlue;

//== ActiveKeyValueStore
//== ActiveKeyValueStore-Read
//== ActiveKeyValueStore-Write

import java.nio.charset.Charset;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import znode.AbstractConnectionWatcher;

// vv ActiveKeyValueStore
/**
 * 
 * 该测试用例主要是用来演示，zookeeper 的节点是如何保存和读取数据的；结合 ConfigUpdater 以及 ConfigWatcher 用于读写。
 * 
 * @author mac
 *
 */
public class ActiveKeyValueStore extends AbstractConnectionWatcher {

	private static final Charset CHARSET = Charset.forName( "UTF-8" );

	// vv ActiveKeyValueStore-Write
	public void write(String path, String value) throws InterruptedException, KeeperException {
		
		/** 这里是同步调用 **/
		Stat stat = zk.exists( path, false );
		
		// 如果节点不存在，则返回 null
		if (stat == null) {
			
			zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
		} else {
			
			zk.setData(path, value.getBytes(CHARSET), -1);
			
		}
		
	}

	// ^^ ActiveKeyValueStore-Write
	// ^^ ActiveKeyValueStore
	// vv ActiveKeyValueStore-Read
	
	public String read(String path, Watcher watcher) throws InterruptedException, KeeperException {
		
		/** 将 ConfigWatcher 作为 watcher 注入，Stat 目的是获取元数据，这里不需要，所以设置为 null **/
		/** 注意，这里会一直监听，直到有变化为止 **/
		byte[] data = zk.getData( path, watcher, null/* stat */ );
		
		return new String(data, CHARSET);
		
	}
	
	// ^^ ActiveKeyValueStore-Read
	// vv ActiveKeyValueStore
}
// ^^ ActiveKeyValueStore
