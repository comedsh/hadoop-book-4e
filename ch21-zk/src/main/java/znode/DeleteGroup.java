package znode;

//cc DeleteGroup A program to delete a group and its members
import java.util.List;

import org.apache.zookeeper.KeeperException;

// vv DeleteGroup
/**
 * 删除整个组
 * 
 * @author mac
 *
 */
public class DeleteGroup extends AbstractConnectionWatcher {
	
	/**
	 * 删除这里要注意，删除的时候，默认的删除方式是，zookeeper会检查节点的版本号，只有当版本号匹配，才回删除该节点
	 * 但是，我们可以将版本号设置为 -1 而绕过这个检测！
	 * 
	 * @param groupName
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void delete(String groupName) throws KeeperException, InterruptedException {
		
		String path = "/" + groupName;

		try {
			
			List<String> children = zk.getChildren(path, false);
			
			for (String child : children) {
				
				/** 可见 child 只会返回当前自己的节点目录名，不会包含其父类的节点目录名 **/
				zk.delete(path + "/" + child, -1);
				
			}
			
			zk.delete(path, -1);
			
		} catch (KeeperException.NoNodeException e) {
			
			System.out.printf("Group %s does not exist\n", groupName);
			
			System.exit(1);
			
		}
	}

	/**
	 * 执行之前，先创建 /zoo 以及相关的子节点 CreateGroup.java | JoinGroup.java
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		DeleteGroup deleteGroup = new DeleteGroup();
		
		deleteGroup.connect("localhost");
		
		deleteGroup.delete("dubbo");
		
		deleteGroup.close();
		
	}
}
// ^^ DeleteGroup
