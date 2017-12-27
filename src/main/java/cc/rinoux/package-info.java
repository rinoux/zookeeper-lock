/**
 * Created by rinoux on 2017/12/27.
 */
package cc.rinoux;


import cc.rinoux.exception.LockException;
import cc.rinoux.lock.ClusterFileLock;
import cc.rinoux.zookeeper.ZkReadLock;
import cc.rinoux.zookeeper.ZkWriteLock;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class LockReadTest {

    static String filePath = "/Users/rinoux/Desktop/lock.txt";

    static ZooKeeper zooKeeper;

    static {
        try {
            zooKeeper = new ZooKeeper("localhost:2181", 30000, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        ExecutorService pool = Executors.newCachedThreadPool();

        pool.execute(new ReadTask());
        pool.execute(new WriteTask());
    }


    static class ReadTask implements Runnable {

        ClusterFileLock lock = new ZkReadLock(zooKeeper, filePath);
        @Override
        public void run() {
            try {
                lock.lock();
                System.out.println(Thread.currentThread().getName() + " get Read lock success! --" + System.currentTimeMillis());
                FileInputStream fin = new FileInputStream(filePath);
                byte[] b = new byte[128];
                while (fin.read(b) > 0) {
                    System.out.println(new String(b).trim());
                }

                Thread.sleep(3000);
            } catch (IOException | LockException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.release();
                System.out.println(Thread.currentThread().getName() + " release Read lock! --" + System.currentTimeMillis());
            }
        }
    }



    static class WriteTask implements Runnable {
        ClusterFileLock lock = new ZkWriteLock(zooKeeper, filePath);
        @Override
        public void run() {
            try {
                lock.lock();
                System.out.println(Thread.currentThread().getName() + " get Write lock success! --" + System.currentTimeMillis());

                FileWriter writer = new FileWriter(filePath);


                writer.append("\n + this is append message!");
                writer.flush();
                writer.close();

                Thread.sleep(3000);
            } catch (IOException | LockException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.release();
                System.out.println(Thread.currentThread().getName() + " release Write lock! --" + System.currentTimeMillis());
            }
        }
    }
}