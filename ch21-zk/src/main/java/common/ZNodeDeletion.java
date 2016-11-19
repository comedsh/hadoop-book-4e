package common;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * util methods for deletion the znodes。
 * 
 * 只有当 znode 的所有子节点被删除以后，znode 才有可能被删除。 
 * 
 * @author 商洋
 *
 * @createTime：Nov 19, 2016 10:48:48 AM
 */
public class ZNodeDeletion {

	/**
	 * 递归删除节点; 几点注意事项
	 * 1. zk.getChild(path...) 返回的是当前目录自己的目录名，不会包含任何父类的节点名；比如 /test/test1, zk.getChild("/test", false) 只会返回 test1。
	 *    所以每次递归删除的时候，都必须将 parentPath 传入。
	 *    
	 * 2、zk.getChild(path...) 返回的路径是不包含 "/" 目录符号前缀的，必须手动添加。
	 * 
	 * 
	 * 
	 * @param path
	 * @param zk
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void delete( String parentPath, String path, ZooKeeper zk ) throws KeeperException, InterruptedException{
		
		String absolutePath = new String( parentPath == null ? path : ( parentPath + path ) ); // 还原当前真实路径
		
		List<String> children = zk.getChildren( absolutePath, false );

//      下面注释掉的实现方案有个 bug，就是不能删除根节点，为什么，没有去深究 ~~ 压栈顺序的问题，应该是先递归删除，然后再压栈删除方法
//		if( children.isEmpty() ){
//			
//			zk.delete( p, -1 );
//			
//		}else{
//		
//			for( String child : children ){
//				
//				delete( p, "/" + child, zk );
//				
//			}
//			
//		}
		
		// 修复了上面实现的 bug
		for( String child : children ){
		
			delete( absolutePath, "/" + child, zk );
		
		}
		
		/**
		 * 这里有几点逻辑要重点考量，
		 * 
		 * 1. 站在叶子节点的角度
		 *    当递归到叶子节点，没有子节点了，当然就删除了 -> 可以当做是(某个分支上的)第一次删除，递归在某一个分支的最深处；（脑海里面可以有这么一幅图景了）
		 *    
		 * 2. 站在子节点的角度（非叶子节点）
		 *    当前子节点的下面一行的删除操作是被压栈的，当轮到它被删除的时候，因为 #1，已经确保了它的所有子节点都已经被成功删除了，这个时候，它已经“变成了”叶子节点，所以可以成功删除。
		 *    
		 * #1 和 #2 体现了递归删除的精华所在，和思考模型的方式和方法。   
		 */
		zk.delete( absolutePath, -1 );
		
	}
	
}
