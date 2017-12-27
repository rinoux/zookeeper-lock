package cc.rinoux.exception;

/**
 * Created by rinoux on 2017/12/6.
 */
public class LockException extends Exception {
    private String message;

    public LockException() {
        super();
    }

    public LockException(String message) {
        super(message);
        this.message = message;
    }


    public LockException(Throwable cause) {
        super(cause);
        this.message = cause.getMessage();
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        if (message == null) {
            message = "Error while locking";
        }
        return "LockException{" +
                "message='" + message + '\'' +
                '}';
    }
}
