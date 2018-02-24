package misc;

public class NetworkManagerException extends RuntimeException {
    /**
     * A wrapper class for an Exception that occurred when trying to create
     * a listener socket or accept incoming connections.
     * 
     * This is likely fatal and we terminate the program because we can no
     * longer receive incoming messages.
     * @param s The detailed error message
     */
    public NetworkManagerException(String s) {
        super(s);
    }
}
