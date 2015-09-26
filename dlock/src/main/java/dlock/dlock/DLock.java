package dlock.dlock;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class DLock {
	private final ZooKeeper zookeeper;
	private final String resourcePath;

	private final int RETRY_INTERVAL = 500;

	private String lockPath = null;

	public DLock(ZooKeeper zookeeper, String resourcePath) {
		this.zookeeper = zookeeper;
		this.resourcePath = resourcePath;
	}

	public void lock() throws DLockException {
		try {
			doLock();
		} catch (KeeperException | InterruptedException e) {
			throw new DLockException("failed to get the lock", e);
		} finally {
			try {
				release();
			} catch (Exception e) {

			}
		}
	}

	private void doLock() throws KeeperException, InterruptedException {
		final String lockName = "lock-";
		lockPath = zookeeper.create(resourcePath + "/" + lockName, null, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);

		final Object lock = new Object();

		while (true) {
			synchronized (lock) {
				List<String> children = zookeeper.getChildren(resourcePath, new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						lock.notifyAll();
					}
				});
				
				Collections.sort(children);
				if (lockPath.endsWith(children.get(0))) {
					return;
				} else {
					// some events may get lost
					// retry interval should be set to avoid wait forever
					lock.wait(RETRY_INTERVAL);
				}
			}
		}
	}

	public void release() throws DLockException {
		if (Objects.isNull(lockPath)) {
			return;
		}

		try {
			zookeeper.delete(lockPath, -1);
			lockPath = null;
		} catch (InterruptedException | KeeperException e) {
			throw new DLockException("failed to release the lock", e);
		}
	}
}