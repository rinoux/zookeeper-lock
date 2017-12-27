package cc.rinoux.zookeeper;


import cc.rinoux.LockConstants;
import cc.rinoux.exception.LockException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class ZkBaseLock {

    private ZooKeeper zooKeeper;
    /**
     * 锁节点名称（不带zk seq编号）
     */
    private String path;
    /**
     * 根节点(需要加锁的文件path)
     */
    private String basePath;


    public ZkBaseLock(ZooKeeper zooKeeper, String basePath, String lockName) {
        this.zooKeeper = zooKeeper;
        if (!basePath.startsWith("/")) {
            this.basePath = "/".concat(basePath);
        } else {
            this.basePath = basePath;
        }
        this.path = basePath.concat("/").concat(lockName);
    }

    /**
     * 等待获取锁
     *
     * @param startMillis  起点时间
     * @param millisToWait 还剩下等待的时间
     * @param currentNode  当前节点
     * @return
     * @throws LockException
     */
    private boolean waitLock(long startMillis, Long millisToWait, String currentNode) throws LockException {

        // 是否得到锁
        boolean locked = false;
        // 是否需要删除当前锁的节点
        boolean needDeleteCurrentNode = false;

        try {
            while (!locked) {

                locked = locked(getSortedLockNodes(), currentNode);

                if (!locked) {
                    //获取监听节点路径逻辑
                    //前一个节点
                    final String watchPath = getWatchedNode(getSortedLockNodes(), currentNode);

                    final CountDownLatch waitLatch = new CountDownLatch(1);
                    //监听前一个节点， stat为null时表示watchPath被删除
                    Stat stat = zooKeeper.exists(watchPath, new Watcher() {
                        @Override
                        public void process(WatchedEvent watchedEvent) {
                            if (watchedEvent.getType() == Event.EventType.NodeDeleted
                                    && watchedEvent.getPath().equals(watchPath)) {
                                waitLatch.countDown();
                            }
                        }
                    });

                    //如果监听节点还没有删除，计时
                    if (stat != null) {
                        if (millisToWait != null) {
                            millisToWait -= (System.currentTimeMillis() - startMillis);
                            startMillis = System.currentTimeMillis();
                            if (millisToWait <= 0) {
                                //等待超时，放弃获取锁
                                needDeleteCurrentNode = true;
                                break;
                            }
                            waitLatch.await(millisToWait, TimeUnit.MICROSECONDS);
                        } else {
                            waitLatch.await();
                        }
                    }
                }
            }
        } catch (Exception e) {
            //发生异常需要删除节点
            needDeleteCurrentNode = true;
            throw new LockException(e);
        } finally {
            //如果需要删除节点
            if (needDeleteCurrentNode) {
                deleteCurrentNode(currentNode);
            }
        }

        return locked;
    }

    /**
     * 根据锁名称获得序列号
     *
     * @param nodePath
     * @return
     */
    private String getLockNodeSequenceNumber(String nodePath) {
        int index = nodePath.lastIndexOf(LockConstants.LOCK_ID_SPLIT);
        if (index >= 0) {
            index += LockConstants.LOCK_ID_SPLIT.length();
            return index <= nodePath.length() ? nodePath.substring(index) : "";
        }
        return nodePath;
    }

    /**
     * 获取所有锁节点(/locker下的子节点)并排序
     *
     * @return
     * @throws Exception
     */
    private List<String> getSortedLockNodes() throws Exception {
        try {
            List<String> children = zooKeeper.getChildren(basePath, null);
            Collections.sort(children, new Comparator<String>() {
                        public int compare(String lhs, String rhs) {
                            return getLockNodeSequenceNumber(lhs).compareTo(getLockNodeSequenceNumber(rhs));
                        }
                    }
            );
            return children;

        } catch (Exception e) {
            createPersistNode(basePath, true);
            return getSortedLockNodes();

        }
    }


    /**
     * 创建永久节点
     *
     * @param path          节点
     * @param createParents 是否创建父节点（永久）
     * @throws LockException
     */
    private void createPersistNode(String path, boolean createParents) throws LockException {
        try {
            zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            if (createParents) {
                //已经创建也会失败
                if (e.code() == KeeperException.Code.NODEEXISTS) {
                    return;
                }
                String parentDir = path.substring(0, path.lastIndexOf('/'));
                createPersistNode(parentDir, true);
                createPersistNode(path, true);
            } else {
                throw new LockException(e);
            }
        } catch (InterruptedException e) {
            throw new LockException(e);
        }
    }

    protected void releaseLock(String lockPath) {
        try {
            deleteCurrentNode(lockPath);
        } catch (LockException e) {
            e.printStackTrace();
            System.out.println("Release lock failed for " + e.getMessage());
        }
    }

    /**
     * 尝试获取锁
     *
     * @param timeOut  等待超时时间
     * @param timeUnit 时间单位
     * @return 锁节点的路径没有获取到锁返回null
     * @throws LockException
     */
    protected String tryAcquire(long timeOut, TimeUnit timeUnit) throws LockException {
        long startMillis = System.currentTimeMillis();
        Long millisToWait = (timeUnit != null) ? timeUnit.toMillis(timeOut) : null;

        String currentNode = null;
        boolean ownLock = false;
        boolean done = false;
        int retryCount = 0;

        while (!done) {
            done = true;
            try {
                //创建临时顺序节点
                currentNode = createLockNode(path);
                // 判断你自己是否获得了锁，如果没获得那么我们等待直到获取锁或者超时
                ownLock = waitLock(startMillis, millisToWait, currentNode);
            } catch (LockException e) {
                if (retryCount++ < LockConstants.MAX_RETRY_COUNT) {
                    done = false;
                } else {
                    throw e;
                }
            }
        }

        if (ownLock) {
            return currentNode;
        }

        return null;
    }

    /**
     * 删除节点
     *
     * @param currentNode 待删除节点
     * @throws LockException
     */
    private void deleteCurrentNode(String currentNode) throws LockException {
        if (currentNode == null) {
            return;
        }
        try {
            zooKeeper.delete(currentNode, -1);
        } catch (Exception e) {
            throw new LockException("Delete node " + currentNode + " counter error, for " + e.getMessage());
        }
    }

    /**
     * 创建临时节点
     *
     * @param path
     * @return 创建的临时节点名称
     * @throws LockException
     */
    private String createLockNode(String path) throws LockException {
        if (!path.startsWith("/")) {
            path = "/".concat(path);
        }
        // 创建临时循序节点
        try {
            return zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (InterruptedException e) {
            throw new LockException(e);
        } catch (KeeperException e) {
            //临时节点不能有子节点，全路径不存在时，相当于全部创建临时节点，会发生异常，必须创建持久的父节点
            createPersistNode(getParent(path), true);
            return createLockNode(path);
        }
    }

    private String getParent(String path) {
        int lastSeparator = path.lastIndexOf("/");
        return path.substring(0, lastSeparator);

    }

    /**
     * 是否是获取锁的状态
     *
     * @param children
     * @param currentNode
     * @return
     */
    public abstract boolean locked(List<String> children, String currentNode) throws LockException;

    /**
     * 获得前一个子节点作为监视的节点
     *
     * @param children
     * @param ourPath
     * @return
     */
    public abstract String getWatchedNode(List<String> children, String ourPath) throws LockException;

    public int getCurrentIndex(List<String> children, String currentNode) throws LockException {
        // 获取顺序节点的名字
        String sequentialNodeName = currentNode.substring(basePath.length() + 1);

        // 获取该节点在所有有序子节点位置
        int currentIndex = children.indexOf(sequentialNodeName);
        if (currentIndex < 0) {
            //可能连接断开导致临时节点被删除
            throw new LockException("No such node found: " + sequentialNodeName);
        }

        return currentIndex;
    }
}
