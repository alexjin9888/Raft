package misc;

public class CommandExecutorException extends RuntimeException {
    /**
     * A wrapper class for an Exception that occurred when trying to execute
     * a command sent by a client.
     * 
     * When the execution fails, we do not know whether the same failure
     * occurred across different servers and can no longer guarantee
     * consistency. We thus terminate the program.
     * @param s The detailed error message
     */
    public CommandExecutorException(String s) {
        super(s);
    }
}
