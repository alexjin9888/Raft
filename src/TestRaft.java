import java.net.InetSocketAddress;
import java.util.HashMap;


/**
 * Code to test Raft server instances by running them using threads. 
 * There is an alternative (preferable) way of running the instances using 
 * processes. See the top-level README.md for details.
 */
public class TestRaft {

    public static void main(String[] args) {
        System.setProperty("log4j.configurationFile", "./src/log4j2.xml");
        HashMap<String, InetSocketAddress> serverAddressesMap =
                new HashMap<String, InetSocketAddress>();
        serverAddressesMap.put("Server0", new InetSocketAddress("localhost",
                6060));
        serverAddressesMap.put("Server1", new InetSocketAddress("localhost",
                6061));
        serverAddressesMap.put("Server2", new InetSocketAddress("localhost",
                6062));

        RaftServer server0 = new RaftServer(serverAddressesMap, "Server0");
        RaftServer server1 = new RaftServer(serverAddressesMap, "Server1");
        RaftServer server2 = new RaftServer(serverAddressesMap, "Server2");
    }
}
