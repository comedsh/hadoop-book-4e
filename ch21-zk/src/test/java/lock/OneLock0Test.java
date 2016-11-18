package lock;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import common.ZooKeeperGenerator;

/**
 * 该测试用例，模拟多个 Client 同时试图创建一个相同的锁，在分布式环境下，我们只允许其中一个 Client 成功获得锁，其它 Client 失败；
 * 
 * 场景是，在周期性的某一个时刻(通常是每天一次的 job)，在分布式的场景当中，只允许一个节点执行；
 * 比如，每天一次的结算系统，在分布式订单系统部署中，我们希望只能有一个节点能够成功触发结算的 scheduler job，否则，会因为同事竞争临界资源，而导致数据不一致的并发错误。
 * 
 * @author 商洋
 *
 * @createTime：Nov 18, 2016 12:08:18 PM
 */
public class OneLock0Test{
	
	private static final int SESSION_TIMEOUT = 5000;
	
	@SuppressWarnings("deprecation")
	@Test
	public void testOneLock0() throws IOException, InterruptedException, KeeperException{
		
		// 这里要注意的是，临时节点，必须创建在永久节点之下，所以，必须先创建 /test 永久节点，然后才能创建 /test/lock 临时节点；并且需要注意的是，临时节点是不能够创建子节点的。
		ZooKeeper keeper = ZooKeeperGenerator.generate("localhost", SESSION_TIMEOUT );
		
		if( keeper.exists( "/onelock", false ) == null ){
			keeper.create( "/onelock", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
		}
		
		OneLock0[] locks = new OneLock0[3];
		
		locks[0] = new OneLock0( ZooKeeperGenerator.generate("localhost", SESSION_TIMEOUT ), "/onelock/lock" );
		
		locks[1] = new OneLock0( ZooKeeperGenerator.generate("localhost", SESSION_TIMEOUT ), "/onelock/lock" );
		
		locks[2] = new OneLock0( ZooKeeperGenerator.generate("localhost", SESSION_TIMEOUT ), "/onelock/lock" );
		
		CountDownLatch latch = new CountDownLatch(3);
		
		TryingLock tryingLock1 = new TryingLock( locks[0], latch );
		tryingLock1.start();
		
		latch.countDown();
		
		TryingLock tryingLock2 = new TryingLock( locks[1], latch );
		tryingLock2.start();
		
		latch.countDown();
		
		TryingLock tryingLock3 = new TryingLock( locks[2], latch );
		tryingLock3.start();
		
		Thread.sleep(1000); // 等待 1s, 为了从日志上能够清晰的看到，三个 OneLock 对象同时执行 lock()
		
		latch.countDown();
		
		// 主进程必须等待所有的线程执行完毕；否则，主进程会继续执行并立即退出，并且会杀死正在执行中的线程。
		tryingLock1.join();
		tryingLock2.join();
		tryingLock3.join();

		// 验证，只能有一个 OneLock 对象成功获得锁对象
		int locktimes = 0;
		
		if( tryingLock1.isLocked() ) locktimes ++;
		
		if( tryingLock2.isLocked() ) locktimes ++;
		
		if( tryingLock3.isLocked() ) locktimes ++;
		
		assertEquals(" there could only one OneLock get the lock", locktimes, 1 );
		
	}
	
	class TryingLock extends Thread{
		
		CountDownLatch latch;
		
		@SuppressWarnings("deprecation")
		OneLock0 lock;
		
		boolean locked; // if successfully get the lock
		
		@SuppressWarnings("deprecation")
		public TryingLock( OneLock0 lock, CountDownLatch latch ){
			
			this.lock = lock;
			
			this.latch = latch;
			
		}
		
		@SuppressWarnings("deprecation")
		public void run(){
			
			try {
				
				System.out.println( "current thread " + Thread.currentThread().getId() + " is waiting ~~~" );
				
				latch.await();
				
				System.out.println(lock.toString()+" trying to get lock just started ~~~");
				
				locked = lock.lock();
				
			} catch (InterruptedException e) {
				
				e.printStackTrace();
				
			} 
			
		}
		
		public boolean isLocked(){
			
			return this.locked;
		}
		
	}
	
}
