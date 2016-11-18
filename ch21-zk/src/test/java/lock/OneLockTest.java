package lock;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import common.ZooKeeperGenerator;


public class OneLockTest {

	private static final int SESSION_TIMEOUT = 5000;

	@Test
	public void testOneLock() throws IOException, InterruptedException, KeeperException{

		
		OneLock[] locks = new OneLock[3];
		
		locks[0] = new OneLock( ZooKeeperGenerator.generate("localhost", SESSION_TIMEOUT ), "/onelock" ); // 使用默认的子节点 lock
		
		locks[1] = new OneLock( ZooKeeperGenerator.generate("localhost", SESSION_TIMEOUT ), "/onelock" );
		
		locks[2] = new OneLock( ZooKeeperGenerator.generate("localhost", SESSION_TIMEOUT ), "/onelock" );
		
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
		
		OneLock locked = null;
		
		if( tryingLock1.isLocked() ) {
			
			locktimes ++;
			
			locked = locks[0];
			
			System.out.println("lock 1 finally get the lock");
			
		}
		
		if( tryingLock2.isLocked() ){
			
			locktimes ++;
			
			locked = locks[1];
			
			System.out.println("lock 2 finally get the lock");
			
		}
		
		if( tryingLock3.isLocked() ){
			
			locktimes ++;
			
			locked = locks[2];
			
			System.out.println("lock 3 finally get the lock");
			
		}
		
		assertEquals(" there could only one OneLock get the lock", locktimes, 1 );
		
		// 得到锁的 Client 就可以互斥的进行临界资源的操作了。
		
		// 最终释放锁
		locked.unlock();
		
	}
	
	/**
	 * 为每一个 OneLock 对象创建线程模拟并发环境下获得锁的过程...
	 * 
	 * @author 商洋
	 *
	 * @createTime：Nov 18, 2016 2:48:03 PM
	 */
	class TryingLock extends Thread{
		
		CountDownLatch latch;
		
		OneLock lock;
		
		boolean locked; // if successfully get the lock
		
		public TryingLock( OneLock lock, CountDownLatch latch ){
			
			this.lock = lock;
			
			this.latch = latch;
			
		}
		
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
