package dlock.dlock;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZooKeeperLockServiceImpl implements ZooKeeperLockService {
	private final ZooKeeper zookeeper;

	public ZooKeeperLockServiceImpl(ZooKeeper zookeeper) {
		this.zookeeper = zookeeper;
	}

	@Override
	public String createLockNode(String path) throws KeeperException, InterruptedException {
		return zookeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	@Override
	public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
		return zookeeper.getChildren(path, watcher);
	}

	@Override
	public void deleteLockNode(String path) throws InterruptedException, KeeperException {
		zookeeper.delete(path, -1);
	}

	@Override
	public void close() throws InterruptedException {
		zookeeper.close();
	}

}
