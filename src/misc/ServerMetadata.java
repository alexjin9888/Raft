package misc;
import java.net.InetSocketAddress;

/**
 * Implementation of state of each server
 * See RAFT figure 2 for detailed descriptions
 */
public class ServerMetadata {
    // Each server has a unique id
    public String id;
    // address    Each server has a unique address (port)
    public InetSocketAddress address;
    // nextIndex  Index of the next log entry to send to that server
    //            (initialized to leader last log index + 1)
    public int nextIndex;
    // matchIndex Index of highest log entry known to be replicated on server 
    //            (initialized to 0, increases monotonically)
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
