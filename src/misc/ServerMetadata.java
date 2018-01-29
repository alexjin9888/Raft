package misc;
import java.net.InetSocketAddress;

public class ServerMetadata {
    public String id;
    public InetSocketAddress address;

    // Volatile States on leaders
    // (Reinitialized after election)
    // * for each server, index of the next log entry to send to that server
    //   (initialized to leader last log index + 1)
    public int nextIndex;
    // * for each server, index of highest log entry known to be replicated on
    //   server (initialized to 0, increases monotonically)
    public int matchIndex;

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
