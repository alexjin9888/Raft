package misc;

/**
 * A wrapper class for a fatal exception that occurred when trying to execute
 * a command sent by a client.
 */
public class CommandExecutorException extends RuntimeException {

    /**
     * @param s The detailed error message
     */
    public CommandExecutorException(String s) {
        super(s);
    }
}
