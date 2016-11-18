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
 * 有个问题需要注意，如果某个 Client A 在锁定过程中，执行的是一个较长的事务，zookeeper 因为网络原因断开了；然后超时，EPHEMERAL 的锁也释放了；这个时候，如果 Client B 成功获得锁，开始对临界资源进行处理；可能的情况有，
 * 
 * 1）Client A 虽然丢失了锁，但对临界资源仍然在处理；导致的结果是，该临界资源同时被 Client A 和 Client B 处理 -> 互斥不满足了
 *    解决办法，
 *    a) 一旦 Client A 检测到网络丢失并且确认在这种情况下，锁已经丢失，那么当前正在处理的一切事务都必须回滚；代价非常的大...
 *    b) 人工干预？
 *    
 * 2）OneLock 的大部分场景是，在某一天的某一个时间点共同竞争锁，其它的失败了以后，当天不再竞争锁，必须等待下一个触发时间.. 所以没有影响；
 * 
 * 针对 #1 出现的情况，我比较倾向于 a) 的解决方案，回滚，但前提是，每时每刻都在竞争这个锁资源 -> 这个场景其实更符合 @see WriteLock 的实现。  
 * 
 * @author 商洋
 * 
 * @deprecated path 中包含了永久的“root path”以及临时的 “叶子节点”，“root path”必须先创建，然后再创建“叶子节点”；该实现没有考虑到这种情况
 */
public class OneLock0 extends ProtocolSupport{
	
	String path;
	
	public OneLock0( ZooKeeper keeper, String path ){
		
		super( keeper );
		
		this.path = path;
		
	}
	
	/**
	 * 创建临时的节点；采用 retry 机制。
	 * @throws KeeperException 
	 * @throws InterruptedException 
	 */
	public boolean lock() throws InterruptedException{

		OneLockOperation operation = new OneLockOperation( super.getAcl() );

		try {
			
			// 分布式环境中，需要 retry 的机制，因为，分布式环境中，我们编程的前提是，认为网络是不可靠的....
			retryOperation( operation );
			
		} catch (KeeperException e) {
			
			// 为什么是 info? 因为 KeeperException 是期望发生的；因为并发情况下，只允许一个线程能够成功创建子节点，其它的节点会报错，返回；
			
			System.out.println("info: "+ e.getMessage() );
			
		} catch (InterruptedException e) {
			
			e.printStackTrace();
			
			throw e;
			
		}	
		
		return operation.isLocked();
	
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
			
			System.out.println(" start to retrive the lock; path:"+path+"; zookeeper:" + zookeeper.toString() );			
			
	        Stat stat = zookeeper.exists( path, false );
	        
	        System.out.println( "Stat:" + ( stat == null ? "null" : stat.toString() ) );
	        
	        if( stat == null ){
	        	
	        	// 如果 path 对应的 znode 对象已经存在，则会抛出一个 KeeperException..
	        	zookeeper.create( path, null, acls, CreateMode.EPHEMERAL );
	        	
	        	System.out.println(" the znode "+path+" created successful");
	        	
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
