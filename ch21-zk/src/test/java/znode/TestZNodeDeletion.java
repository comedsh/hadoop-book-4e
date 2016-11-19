package znode;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import common.ZNodeDeletion;
import common.ZooKeeperGenerator;

public class TestZNodeDeletion {
	
	@Test
	public void testDeleteNode() throws IOException, InterruptedException, KeeperException{
		
		ZooKeeper keeper = ZooKeeperGenerator.generate( "localhost", 5000 );
		
		ZNodeDeletion.delete( null, "/dubbo", keeper );
		
	}
	
	
}
