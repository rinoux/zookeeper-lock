package cc.rinoux.zookeeper;


import cc.rinoux.LockConstants;
import cc.rinoux.exception.LockException;
import cc.rinoux.lock.ClusterFileLock;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ZkWriteLock extends ZkBaseLock implements ClusterFileLock {
    /**
     * 需要获取的资源名称，在Zookeeper中映射节点
     */
    private final String basePath;

    /**
     * 获取锁以后自己创建的那个顺序节点的路径
     */
    private String currentLockNode;

    public ZkWriteLock(ZooKeeper zooKeeper, String basePath) {
        super(zooKeeper, basePath, LockConstants.WRITE_LOCK_PREFIX);
        this.basePath = basePath;
    }

    @Override
    public void lock() throws LockException {
        //-1：永不超时
        currentLockNode = tryAcquire(LockConstants.NO_TIME_OUT, null);
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
        return getCurrentIndex(children, currentNode) == 0;
    }


    @Override
    public String getWatchedNode(List<String> children, String currentPath) throws LockException {
        int currentIndex = getCurrentIndex(children, currentPath);
        // 如果不是第一位,监听比自己小的那个节点的删除事件
        String pathToWatch = children.get(currentIndex - 1);
        return basePath.concat("/").concat(pathToWatch);
    }
}
