package misc;

public class PersistentStateException extends RuntimeException {
    /**
     * A wrapper class for an Exception that occurred when trying to persist
     * server specific information to disk.
     * 
     * When we fail to persist to disk, we can no longer guarantee
     * consistency across raft servers and thus terminate the program.
     * @param s The detailed error message
     */
    public PersistentStateException(String s) {
        super(s);
    }
}