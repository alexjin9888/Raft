import java.net.InetSocketAddress;
import java.util.HashMap;

public class TestRaft {

    public static void main(String[] args) {
        HashMap<String, InetSocketAddress> serverAddressesMap = new HashMap<String, InetSocketAddress>();
        serverAddressesMap.put("Server1", new InetSocketAddress("localhost", 6060));
        serverAddressesMap.put("Server2", new InetSocketAddress("localhost", 6061));
        serverAddressesMap.put("Server3", new InetSocketAddress("localhost", 6062));

        Server server1 = new Server("Server1", serverAddressesMap);
        Server server2 = new Server("Server2", serverAddressesMap);
        Server server3 = new Server("Server3", serverAddressesMap);
        (new Thread(server1)).start();
        (new Thread(server2)).start();
        (new Thread(server3)).start();
    }
}
