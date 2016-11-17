package znodevlue.retry;

//== ResilientActiveKeyValueStore-Write

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import znode.AbstractConnectionWatcher;

public class ResilientActiveKeyValueStore extends AbstractConnectionWatcher {

	private static final Charset CHARSET = Charset.forName("UTF-8");
	private static final int MAX_RETRIES = 5;
	private static final int RETRY_PERIOD_SECONDS = 10;

	// vv ResilientActiveKeyValueStore-Write
	/**
	 * 这里增加了 retry 的逻辑，当发生 KeeperException 后，连接异常，会进行 retry 一定的次数.. 然后抛出异常，并终止！
	 * 
	 * 这里的 retry 的逻辑里面，其实隐含了一个非常重要的概念，就是“幂等性”！
	 * 
	 * @param path
	 * @param value
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void write(String path, String value) throws InterruptedException, KeeperException {
		
		int retries = 0;
		
		while (true) {
			
			try {
				
				Stat stat = zk.exists(path, false);
				
				if (stat == null) {
					
					zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					
				} else {
					
					zk.setData(path, value.getBytes(CHARSET), stat.getVersion());
					
				}
				
				return;
				
			} catch (KeeperException.SessionExpiredException e) {
				
				throw e;
				
			} catch (KeeperException e) {
				
				/** 当 retry 了指定次数以后，抛出异常，终止当前进程 **/
				if (retries++ == MAX_RETRIES) {
					
					throw e;
					
				}
				
				// sleep then retry
				TimeUnit.SECONDS.sleep(RETRY_PERIOD_SECONDS);
			}
		}
	}

	// ^^ ResilientActiveKeyValueStore-Write
	public String read(String path, Watcher watcher) throws InterruptedException, KeeperException {
		
		byte[] data = zk.getData(path, watcher, null/* stat */);
		
		return new String(data, CHARSET);
		
	}
}
