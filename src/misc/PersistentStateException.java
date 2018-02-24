package misc;

@SuppressWarnings("serial")
public class PersistentStateException extends RuntimeException {
    /**
     * A wrapper class for a fatal exception that occurred when trying to:
     * 1) load the persistent state from disk, or
     * 2) keep the server's in-memory state consistent with the state on disk.
     * @param s The detailed error message
     */
    public PersistentStateException(String s) {
        super(s);
    }
}
