package dlock.dlock;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class DLock {
	private final ZooKeeperLockService zookeeper;
	private final String resourcePath;

	private final int RETRY_INTERVAL = 500;

	private String lockPath = null;

	public DLock(ZooKeeperLockService zookeeper, String resourcePath) {
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
		lockPath = zookeeper.createLockNode(resourcePath + "/" + lockName);

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
				for (String child : children) {
					if (!child.startsWith(lockName)) {
						continue;
					}

					if (lockPath.endsWith(child)) {
						return;
					} else {
						break;
					}
				}

				// some events may get lost
				// retry interval should be set to avoid wait forever
				lock.wait(RETRY_INTERVAL);
			}
		}
	}

	public void release() throws DLockException {
		if (Objects.isNull(lockPath)) {
			return;
		}

		try {
			zookeeper.deleteLockNode(lockPath);
			lockPath = null;
		} catch (InterruptedException | KeeperException e) {
			throw new DLockException("failed to release the lock", e);
		}
	}
}
