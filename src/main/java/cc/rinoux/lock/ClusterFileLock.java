package cc.rinoux.lock;




import cc.rinoux.exception.LockException;

import java.util.concurrent.TimeUnit;


/**
 * 集群文件锁
 * Created by rinoux on 2017/12/14.
 */
public interface ClusterFileLock {
    /**
     * 获取锁
     *
     * @throws LockException
     */
    void lock() throws LockException;

    /**
     * 获取锁
     *
     * @param time 超时等待时间
     * @param unit 时间单位
     * @return 是否成功
     * @throws LockException
     */
    boolean tryLock(long time, TimeUnit unit) throws LockException;

    /**
     * 释放锁
     */
    void release();

}

