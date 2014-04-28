package storm.blueprint.function;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 6:55 PM
 */
public class FunctionNotSupportedException extends Exception {
    public FunctionNotSupportedException() {
    }

    public FunctionNotSupportedException(String message) {
        super(message);
    }

    public FunctionNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public FunctionNotSupportedException(Throwable cause) {
        super(cause);
    }

    public FunctionNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
