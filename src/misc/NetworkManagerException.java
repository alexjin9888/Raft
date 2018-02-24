package misc;

/**
 * A wrapper class for a fatal exception that occurred when trying to create
 * a listener socket or accepting incoming connection(s).
 * @param s The detailed error message
 */
@SuppressWarnings("serial")
public class NetworkManagerException extends RuntimeException {
    public NetworkManagerException(String s) {
        super(s);
    }
}
