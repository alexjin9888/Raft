import java.net.InetSocketAddress;
import java.util.HashMap;

/**
 * Code to test Raft server log output by starting up multiple Raft servers.
 * See the top-level README.md for a more general way to start up Raft servers
 * to test.
 */
public class TestRaftServer {
    
    final static int NUM_SERVERS = 3;

    public static void main(String[] args) {
        System.setProperty("log4j.configurationFile", "./src/log4j2.xml");
        
        HashMap<String, InetSocketAddress> serverAddressesMap =
                new HashMap<String, InetSocketAddress>();
        
        for (int i = 0; i < NUM_SERVERS; i++) {
            serverAddressesMap.put("Server" + i, new InetSocketAddress("localhost",
                    6060 + i));            
        }

        for (String serverId : serverAddressesMap.keySet()) {
            RaftServer server = new RaftServer(serverAddressesMap, serverId);
        }
    }
}
