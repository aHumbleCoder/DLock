package dlock.dlock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class Demo {
	private ExecutorService executor;
	private ZooKeeper zookeeper;
	private String resourcePath;

	public static void main(String[] args)
			throws IOException, InterruptedException, KeeperException, ExecutionException {
		
		ZooKeeper zookeeper = getZooKeeper();

		// the following znode path should exist in zookeeper server(s)
		final String path = "/test";
		new Demo(zookeeper, path).run();
		
		zookeeper.close();
	}
	
	private static ZooKeeper getZooKeeper() throws IOException, InterruptedException {
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final String CONN_STRING = "localhost:2181";
		final int TIMEOUT = 2000;
		ZooKeeper zookeeper = new ZooKeeper(CONN_STRING, TIMEOUT, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected) {
					countDownLatch.countDown();
				}
			}
		});

		countDownLatch.await();
		
		return zookeeper;
	}

	private Demo(ZooKeeper zookeeper, String resourcePath) {
		this.executor = Executors.newFixedThreadPool(4);
		this.zookeeper = zookeeper;
		this.resourcePath = resourcePath;
	}

	void run() {
		try {
			doRun();
		} catch (InterruptedException | ExecutionException e) {

		} finally {
			executor.shutdown();
		}
	}

	private void doRun() throws InterruptedException, ExecutionException {
		List<Future<?>> results = new ArrayList<>();

		final int N = 10;
		for (int i = 0; i < N; i++) {
			Future<?> result = executor.submit(new Locker(zookeeper, resourcePath, i));
			results.add(result);
		}

		// wait...
		for (int i = 0; i < N; i++) {
			results.get(i).get();
		}
	}
}

class Locker implements Runnable {
	private ZooKeeper zookeeper;
	private String path;
	private int id;

	public Locker(ZooKeeper zookeeper, String path, int id) {
		this.zookeeper = zookeeper;
		this.path = path;
		this.id = id;
	}

	@Override
	public void run() {
		DLock lock = new DLock(zookeeper, path);

		System.out.println(id + " try to get the lock");
		try {
			lock.lock();
			System.out.println(id + " get the lock");
		} catch (DLockException e) {
			System.out.println(id + " fail to get the lock");
			return;
		}

		try {
			Thread.sleep(1000);
			System.out.println(id + " sleep for a while");
		} catch (InterruptedException e1) {

		}

		try {
			lock.release();
			System.out.println(id + " release the lock");
		} catch (DLockException e) {
			System.out.println(id + " fail to release the lock");
			return;
		}
	}
}
