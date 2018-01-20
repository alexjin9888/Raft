import java.net.InetSocketAddress;

public class ServerMetadata {
    String id;
    InetSocketAddress address;

    // Volatile States on leaders
    // (Reinitialized after election)
    // * for each server, index of the next log entry to send to that server
    //   (initialized to leader last log index + 1)
    int nextIndex;
    // * for each server, index of highest log entry known to be replicated on
    //   server (initialized to 0, increases monotonically)
    int matchIndex;

    public ServerMetadata(String id, InetSocketAddress address) {
        super();
        this.id = id;
        this.address = address;
        this.nextIndex = 0;
        this.matchIndex = -1;
    }
}
