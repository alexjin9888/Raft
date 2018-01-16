import java.net.InetSocketAddress;

public class ServerMetadata {
    String id;
    InetSocketAddress address;

    public ServerMetadata(String id, InetSocketAddress address) {
        super();
        this.id = id;
        this.address = address;
    }
}
