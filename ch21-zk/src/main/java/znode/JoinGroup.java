package znode;

//cc JoinGroup A program that joins a group

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

// vv JoinGroup
/**
 * zoo 就是 group, duck、cow and goat 是 znodes...
 * 
 * @author mac
 *
 */
public class JoinGroup extends AbstractConnectionWatcher {

	public void join(String groupName, String memberName) throws KeeperException, InterruptedException {
		
		String path = "/" + groupName + "/" + memberName;
		
		/** CreateMode.EPHEMERAL 临时的节点，当客户端的连接断开，就会被删除**/
		String createdPath = zk.create(path, null/* data */, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		System.out.println("Created " + createdPath);
		
	}

	public static void main(String[] args) throws Exception {
		
		JoinGroup joinGroup = new JoinGroup();
		
		joinGroup.connect("localhost");
		
		// joinGroup.join(args[1], args[2]);
		
		joinGroup.join("zoo", "duck");
		
		joinGroup.join("zoo", "cow");
		
		joinGroup.join("zoo", "goat");

		// stay alive until process is killed or thread is interrupted
		Thread.sleep(Long.MAX_VALUE);
		
	}
}
// ^^ JoinGroup
