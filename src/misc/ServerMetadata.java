package misc;
import java.net.InetSocketAddress;

/**
 * Implementation of state of each server
 * See RAFT figure 2 for detailed descriptions
 *
 */
public class ServerMetadata {
    /**
     * See RAFT figure 2 for explanation of these variables
     */
    public String id;
    public InetSocketAddress address;
    public int nextIndex;
    public int matchIndex;

    /**
     * @param id      Server's unique ID
     * @param address Server's unique address
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
