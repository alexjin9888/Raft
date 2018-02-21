import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;

import messages.ClientReply;
import messages.ClientRequest;
import misc.CheckingCancelTimerTask;
import misc.NetworkManager;

public class RaftClient {
    
    /**
     * Amount of time to wait for a reply before we send another request.
     */
    private static final int RETRY_TIMEOUT_MS = 5000;
    
    /**
     * The address that I use to receive replies.
     */
    private InetSocketAddress myAddress;
    
    /**
     * Address of the server that we think is the current leader.
     */
    private InetSocketAddress leaderAddress;
    
    /**
     * List of addresses of servers in the Raft cluster.
     */
    private ArrayList<InetSocketAddress> serverAddresses;
    
    /**
     * Timer used to facilitate the retrying of requests.
     */
    private Timer retryRequestTimer;

    private CheckingCancelTimerTask retryRequestTask;
        
    private NetworkManager networkManager;
    
    /**
     * The current request for which we have not yet received a successful reply
     */
    private ClientRequest outstandingRequest;
    
    /**
     * Number of commands read from the command line.
     */
    private int numCommandsRead;
        
    /**
     * Scanner for receiving command line input from the user.
     */
    private Scanner commandReader;
    
    public RaftClient(InetSocketAddress myAddress, ArrayList<InetSocketAddress> serverAddresses) {
        synchronized(RaftClient.this) {
            this.myAddress = myAddress;
            this.serverAddresses = serverAddresses;

            commandReader = new Scanner(System.in);
            outstandingRequest = null;
            numCommandsRead = 0;
            retryRequestTimer = new Timer();

            networkManager = new NetworkManager(this.myAddress, this::handleSerializable);
            
            waitForAndProcessInput();
        }
    }
    
    public synchronized void handleSerializable(Serializable object) {
        if (!(object instanceof ClientReply)) {
            System.out.println("Don't know how to handle the serializable object: " + object);
            return;
        }
        
        ClientReply reply = (ClientReply) object;
        
        if (outstandingRequest == null || outstandingRequest.commandId != reply.commandId) {
            // This reply is no longer relevant to us.
            return;
        }
        
        if (!reply.success) {
            leaderAddress = reply.leaderAddress;
            assert(outstandingRequest != null);
            sendRetryingRequest();
            return;
        }
        System.out.println(reply.result);
        outstandingRequest = null;
        retryRequestTask.cancel();
        waitForAndProcessInput();
    }
    
    /**
     * Sends the outstanding request containing the command to-be-executed and
     * periodically retries the request in the event that we don't receive a
     * reply.
     * Precondition: The client's outstanding request is not null. 
     */
    private synchronized void sendRetryingRequest() {
        if (leaderAddress == null) {
            leaderAddress = serverAddresses.get(ThreadLocalRandom.current().nextInt(
                    serverAddresses.size()));
        }
        
        networkManager.sendSerializable(leaderAddress, outstandingRequest);
        
        if (retryRequestTask != null) {
            retryRequestTask.cancel();
        }
        
        // Create a timer task to periodically retry the request.
        retryRequestTask = new CheckingCancelTimerTask() {
                public void run() {
                    synchronized(RaftClient.this) {
                        if (this.isCancelled) {
                            return;
                        }
                        leaderAddress = serverAddresses.get(ThreadLocalRandom.current().nextInt(
                                serverAddresses.size()));
                        assert(outstandingRequest != null);
                        networkManager.sendSerializable(leaderAddress, outstandingRequest);
                    }
                }
        };
        
        retryRequestTimer.scheduleAtFixedRate(retryRequestTask, RETRY_TIMEOUT_MS, RETRY_TIMEOUT_MS);
    }
 
    private synchronized void waitForAndProcessInput() {
        System.out.println("Please enter a bash command:");
        String command = commandReader.nextLine();
        numCommandsRead += 1;
        outstandingRequest = new ClientRequest(numCommandsRead, myAddress, command);
        sendRetryingRequest();
  }
    
    public static void main(String[] args) {
        // A2DO: do argument parsing here to get the list of server addresses
        // A2DO: ensure that list of server addresses passed in is non-empty
        // A2DO: start up a single client using the passed-in arguments
        
        ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();

        int myPortIndex = -1;
        String[] allHostsStrings = null;
        int[] allPorts = null;
        boolean validArgs = true;
        String[] addPort = null;
        int port = null;
        InetSocketAddress serverAddress = null;
        InetSocketAddress myAddress = null;

        if (args.length != 2) {
            validArgs = false;
        } else {
            addPort = args[0].split(":")
            if (addPort.length != 2) {
                validArgs = false;
                break;
            }
            try {
                port = Integer.parseInt(addPort[1]);
                myAddress = new InetSocketAddress(addPort[0], port);
            } catch (Exception e) {
                validArgs = false;
                break;
            }
            allHostsStrings = args[1].split(",");
            for (int i=0; i<allHostsStrings.length; i++) {
                addPort = allHostsStrings[i].split(":")
                if (addPort.length != 2) {
                    validArgs = false;
                    break;
                }
                try {
                    port = Integer.parseInt(addPort[1]);
                    serverAddress = new InetSocketAddress(addPort[0], port);
                } catch (Exception e) {
                    validArgs = false;
                    break;
                }
                serverAddresses.add(serverAddress);
            }
        }

        if (!validArgs) {
            System.out.println("Please supply exactly two valid arguments");
            System.out.println(
                    "Usage: <myHostname:myPort> <hostname0:port0>,<hostname1:port1>,...,<hostname$n-1$,port$n-1$>");
            System.out.println("Note: List of ports is 0-indexed");
            System.exit(1);
        }
        
        RaftClient client = new RaftClient(myAddress, serverAddresses);
    }
}
