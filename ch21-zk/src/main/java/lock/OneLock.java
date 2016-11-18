package lock;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.recipes.lock.ProtocolSupport;
import org.apache.zookeeper.recipes.lock.ZooKeeperOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 为了解决 OneLock0 遗留下来的问题，分别创建永久的 root path 和 临时的 leaf path
 * 
 * @author 商洋
 *
 * @createTime：Nov 18, 2016 12:23:26 PM
 */
public class OneLock extends ProtocolSupport{
	
	private static final Logger logger = LoggerFactory.getLogger(OneLock.class);
	
	String rootPath;
	
	String leafPath = "lock"; // default name, could be modified by the setter method.
	
	List<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
	
	public OneLock( ZooKeeper keeper, String path ){
		
		super( keeper );
		
		this.rootPath = path;
		
	}
	
	public void setLeafPath( String path ){
		
		this.leafPath = path;
		
	}
	
	public String getPath(){
		
		return rootPath + "/" + leafPath;
		
	}
	
	/**
	 * 创建临时的节点；采用 retry 机制。
	 * @throws KeeperException 
	 * @throws InterruptedException 
	 */
	public boolean lock() throws InterruptedException{
		// 1. 创建永久的根节点
		RootNodeOperation rop = new RootNodeOperation();
		
		try {
			
			retryOperation( rop );
			
		} catch (KeeperException e) {

			logger.info( e.getMessage() );
			
		}
		
		// 2. 创建永久的子节点 -> 锁
		OneLockOperation oop = new OneLockOperation();

		try {
			
			// 分布式环境中，需要 retry 的机制，因为，分布式环境中，我们编程的前提是，认为网络是不可靠的....
			retryOperation( oop );
			
		} catch (KeeperException e) {
			
			// 为什么是 info? 因为 KeeperException 是期望发生的；因为并发情况下，只允许一个线程能够成功创建子节点，其它的节点会报错，返回；
			logger.info( e.getMessage() );
			
		}	
		
		return oop.isLocked();
	
	}
	
	public void unlock() throws KeeperException, InterruptedException {
		
		retryOperation( new ZooKeeperOperation(){

			@Override
			public boolean execute() throws KeeperException, InterruptedException {

				zookeeper.delete( getPath(), -1 ); // 删除节点，既是释放锁
				
				return true; // 执行成功，终止 retry.
			}
			
		});

		
	}
	
	/**
	 * 创建永久根节点；在分布式并发的环境下，保证有且仅有一个 Client 成功创建了 root 节点即可。
	 * 
	 * @author 商洋
	 *
	 * @createTime：Nov 18, 2016 2:09:29 PM
	 */
	class RootNodeOperation implements ZooKeeperOperation{

		@Override
		public boolean execute() throws KeeperException, InterruptedException {
			
			Stat stat = zookeeper.exists(rootPath, false);
			
			if( stat == null ){
				
				// 并发创建的情况下，如果 rootpath 对应的节点已经存在，则会抛出一个 Keeper Exception
				zookeeper.create( rootPath, null, acls, CreateMode.PERSISTENT );
				
			}
			
			return true;
				
		}
		
	}
	
	class OneLockOperation implements ZooKeeperOperation{
		
		boolean locked;
		
		@Override
		public boolean execute() throws KeeperException, InterruptedException {
			
			String path = rootPath + "/" + leafPath;
			
			logger.debug(" start to retrive the lock; path:"+path+"; zookeeper:" + zookeeper.toString() );			
			
	        Stat stat = zookeeper.exists( getPath(), false );
	        
	        System.out.println( "Stat:" + ( stat == null ? "null" : stat.toString() ) );
	        
	        if( stat == null ){
	        	
	        	// 如果 path 对应的 znode 对象已经存在，则会抛出一个 KeeperException..
	        	zookeeper.create( path, null, acls, CreateMode.EPHEMERAL );
	        	
	        	logger.info(" the znode "+path+" created successful" );
	        	
	        	locked = true;
	        }
	        
	        /**
        	 * 哈哈，又云里雾里了哈，怎么都返回 true？true 表示当前的 execution 成功并终止，不需要 retry了，而 retry 的前提是，有异常发生。
        	 */
	        return true;
			
		}
		
		boolean isLocked(){
			
			return locked;
			
		}
		
	}
	
}

