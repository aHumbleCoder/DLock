package dlock.dlock;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

/**
 * A wrapper of zookeeper.
 * It is intended to make the DLock testable
 */
public interface ZooKeeperLockService {
	String createLockNode(String path) throws KeeperException, InterruptedException;
	
	List<String> getChildren(final String path, Watcher watcher) throws KeeperException, InterruptedException;
	
	void deleteLockNode(String path) throws InterruptedException, KeeperException;
	
	void close() throws InterruptedException;
}
