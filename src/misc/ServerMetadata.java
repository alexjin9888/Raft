package misc;
import java.net.InetSocketAddress;

/**
 * An instance of this class is created for every other server in the Raft
 * cluster. These instances are used to help the running server read properties
 * and keep track of state corresponding to the other servers.
 */
public class ServerMetadata {
    public String id; // Each server has a unique id
    public InetSocketAddress address; // Each server has a unique address
    // Index of the next log entry to send to that server
    public int nextIndex;
    // Index of highest log entry known to be replicated on server 
    public int matchIndex;

    /**
     * @param id      see above
     * @param address see above
     */
    public ServerMetadata(String id, InetSocketAddress address) {
        super();
        this.id = id;
        this.address = address;
        this.nextIndex = 0;
        this.matchIndex = -1;
    }

    @Override
    public String toString() {
        return "ServerMetadata [id=" + id + ", address=" + address
                + ", nextIndex=" + nextIndex + ", matchIndex=" + matchIndex
                + "]";
    }
}
