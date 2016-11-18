package znodevlue;

//cc ConfigWatcher An application that watches for updates of a property in ZooKeeper and prints them to the console
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

// vv ConfigWatcher
public class ConfigWatcher implements Watcher {

	private ActiveKeyValueStore store;

	public ConfigWatcher( String hosts ) throws IOException, InterruptedException {
		
		store = new ActiveKeyValueStore();
		
		store.connect( hosts );
		
	}

	public void displayConfig() throws InterruptedException, KeeperException {
		
		/** this 作为回调的 Watcher; 注意，这里会一直监听，知道有变化为止 **/
		String value = store.read( ConfigUpdater.PATH, this );
		
		System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);
		
	}

	/**
	 * 一旦有变化，监听终止，所以，如果需要继续监听，则需要调用代码继续侦听...
	 */
	@Override
	public void process(WatchedEvent event) {
		
		// 如果没有变化，则继续监听
		if (event.getType() == EventType.NodeDataChanged) {
			
			try {
				
				displayConfig();
				
			} catch (InterruptedException e) {
				
				System.err.println("Interrupted. Exiting.");
				
				Thread.currentThread().interrupt();
				
			} catch (KeeperException e) {
				
				System.err.printf("KeeperException: %s. Exiting.\n", e);
				
			}
		}
	}

	public static void main(String[] args) throws Exception {
		
		ConfigWatcher configWatcher = new ConfigWatcher("localhost");
		
		configWatcher.displayConfig();

		// stay alive until process is killed or thread is interrupted
		Thread.sleep( Long.MAX_VALUE );
		
	}
}
// ^^ ConfigWatcher
