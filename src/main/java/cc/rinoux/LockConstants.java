package cc.rinoux;

/**
 * Created by rinoux on 2017/11/7.
 */
public class LockConstants {

    /**
     * 锁文件间隔符
     */
    public static final String LOCK_FILE_SEPARATOR = "-";

    /**
     * 写锁文件前缀
     */
    public static final String WRITE_LOCK_PREFIX = "write-lock-";

    /**
     * 读锁文件前缀
     */
    public static final String READ_LOCK_PREFIX = "read-lock-";

    /**
     * 尝试获得锁的次数
     */
    public static final int MAX_RETRY_COUNT = 10;

    /**
     * 锁文件的标识符
     */
    public static final String LOCK_ID_SPLIT = "-lock-";

    /**
     * 默认的等待锁时间：-1不超时
     */
    public static final int NO_TIME_OUT = -1;
}
