package lock;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.recipes.lock.ProtocolSupport;
import org.apache.zookeeper.recipes.lock.ZooKeeperOperation;

/**
 * 区别于 WriteLock，OneLock 同一时间只允许一个 Client 节点获得锁，其它节点试图获得锁，立马失败；WriteLock 是一种排队机制，轮换的来获得对资源的锁。
 * 
 * 应用场景，Scheduler，比如，每晚定期对 Order 进行超时自动收货动作，当在一个分布式应用场景下，多个节点都有相同的 Scheduler 实例，那么这个时候，需要保证只有一个 Scheduler 执行一次即可， 其它 Scheduler 试图执行，执行失败返回。 
 * 
 * @author 商洋
 *
 */
public class OneLock extends ProtocolSupport{
	
	String path;
	
	public OneLock( ZooKeeper keeper, String path ){
		
		super( keeper );
		
		this.path = path;
		
	}
	
	/**
	 * 创建临时的节点；采用 retry 机制。
	 * @throws KeeperException 
	 * @throws InterruptedException 
	 */
	public boolean lock() throws KeeperException, InterruptedException{

		OneLockOperation operation = new OneLockOperation( super.getAcl() );

		try {
			
			// 分布式环境中，需要 retry 的机制，因为，分布式环境中，我们编程的前提是，认为网络是不可靠的....
			retryOperation( operation );
			
			return operation.getLocked();
			
		} catch (KeeperException e) {
			
			throw e;
			
		} catch (InterruptedException e) {
			
			throw e;
			
		}		
	
	}
	
	public void unlock(){
		
	}
	
	public void close(){
		
	}
	
	class OneLockOperation implements ZooKeeperOperation{
		
		boolean locked;
		
		List<ACL> acls;
		
		public OneLockOperation( List<ACL> acls ){
			
			this.acls = acls;
		}
		
		@Override
		public boolean execute() throws KeeperException, InterruptedException {
			
	        Stat stat = zookeeper.exists( path, false );
	        
	        if( stat == null ){
	        
	        	zookeeper.create( path, null, acls, CreateMode.EPHEMERAL );
	        	
	        	locked = true;
	        }
	        
	        /**
        	 * 哈哈，又云里雾里了哈，怎么都返回 true？true 表示当前的 execution 成功并终止，不需要 retry了，而 retry 的前提是，有异常发生。
        	 */
	        return true;
			
		}
		
		boolean getLocked(){
			
			return locked;
			
		}
		
	}
	
}
