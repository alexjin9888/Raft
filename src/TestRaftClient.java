import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Code to test Raft client application command execution via command line 
 * input by starting up multiple Raft servers and a Raft client instance.
 * See the top-level README.md for a more general way to start up Raft 
 * servers and Raft clients.
 */
public class TestRaftClient {

    final static int NUM_SERVERS = 3;

    public static void main(String[] args) {
        Configurator.setRootLevel(Level.OFF);

        HashMap<String, InetSocketAddress> serverAddressesMap =
                new HashMap<String, InetSocketAddress>();

        for (int i = 0; i < NUM_SERVERS; i++) {
            serverAddressesMap.put("Server" + i, 
                    new InetSocketAddress("localhost", 6060 + i));            
        }

        for (String serverId : serverAddressesMap.keySet()) {
            @SuppressWarnings("unused")
            RaftServer server = new RaftServer(serverAddressesMap, serverId);
        }

        // Start up a single client
        ArrayList<InetSocketAddress> serverAddresses =
                new ArrayList<InetSocketAddress>(
                        serverAddressesMap.values());
        @SuppressWarnings("unused")
        RaftClient client = new RaftClient(
                new InetSocketAddress("localhost", 6070), serverAddresses);
    }

}
