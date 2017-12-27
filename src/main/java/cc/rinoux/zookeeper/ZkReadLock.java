package cc.rinoux.zookeeper;

import cc.rinoux.LockConstants;
import cc.rinoux.exception.LockException;
import cc.rinoux.lock.ClusterFileLock;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ZkReadLock extends ZkBaseLock implements ClusterFileLock {
    /**
     * 需要获取的资源名称，在Zookeeper中映射节点
     */
    private final String basePath;

    /**
     * 获取锁创建的顺序节点
     */
    private String currentLockNode;

    private boolean shared = true;

    public ZkReadLock(ZooKeeper zooKeeper, String basePath) {
        super(zooKeeper, basePath, LockConstants.READ_LOCK_PREFIX);
        this.basePath = basePath;
    }

    public ZkReadLock(ZooKeeper zooKeeper, String basePath, boolean shared) {
        this(zooKeeper, basePath);
        this.shared = shared;
    }

    @Override
    public void lock() throws LockException {
        // -1 表示永不超时
        currentLockNode = tryAcquire(-1, null);
        if (currentLockNode == null) {
            throw new LockException("Fail lock resource：" + basePath);
        }
    }

    @Override
    public boolean tryLock(long timeOut, TimeUnit timeUnit) throws LockException {
        currentLockNode = tryAcquire(timeOut, timeUnit);
        return currentLockNode != null;
    }

    @Override
    public void release() {
        releaseLock(currentLockNode);
    }

    @Override
    public boolean locked(List<String> children, String currentNode) throws LockException {
        int currentIndex = getCurrentIndex(children, currentNode);
        for (int i = 0; i < currentIndex; i++) {
            //已经有写锁，加锁失败
            if (children.get(i).contains(LockConstants.WRITE_LOCK_PREFIX))
                return false;
        }

        if (currentIndex != 0 && !shared) {
            return false;
        }
        return true;
    }

    @Override
    public String getWatchedNode(List<String> children, String currentNode) throws LockException {
        int currentIndex = getCurrentIndex(children, currentNode);
        int watchIndex = 0;
        for (int i = 0; i < currentIndex; i++) {
            //存在写锁
            if (children.get(i).contains(LockConstants.WRITE_LOCK_PREFIX)) {
                watchIndex = i;
            }
        }
        //都是读锁的时候，就都去竞争
        return basePath.concat("/").concat(children.get(watchIndex));
    }

}
