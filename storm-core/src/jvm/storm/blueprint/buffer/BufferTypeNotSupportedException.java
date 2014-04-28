package storm.blueprint.buffer;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 9:07 PM
 */
public class BufferTypeNotSupportedException extends Exception {
    public BufferTypeNotSupportedException() {
    }

    public BufferTypeNotSupportedException(String message) {
        super(message);
    }

    public BufferTypeNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public BufferTypeNotSupportedException(Throwable cause) {
        super(cause);
    }

    public BufferTypeNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
