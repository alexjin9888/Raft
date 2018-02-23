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
import misc.AddressUtils;

public class RaftClient {
    
    /**
     * Amount of time to wait for a reply before we send another request.
     */
    private static final int RETRY_TIMEOUT_MS = 1000;
    
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

    /**
     * A wrapped TimerTask object that allows checking of task cancellation.
     */
    private CheckingCancelTimerTask retryRequestTask;
        
    /**
     * An instances that manages sending and receiving messages.
     */
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
    
    /**
     * A callback method that is executed after our network manager receives
     * a message.
     * @param object A serializable object we received
     */
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
        System.out.print(reply.result);
        outstandingRequest = null;
        retryRequestTask.cancel();
        waitForAndProcessInput();
    }
    
    /**
     * Sends the outstanding request containing the command to-be-executed
     * and periodically retries the request in the event that we don't
     * receive a reply.
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
 
    /**
     * A method that prompts for a use input. Sends it to the Raft cluster
     * and blocks until we get a reply (retries indefinitely).
     */
    private synchronized void waitForAndProcessInput() {
        System.out.println("Please enter a bash command:");
        String command = commandReader.nextLine();
        numCommandsRead += 1;
        outstandingRequest = new ClientRequest(numCommandsRead, myAddress, command);
        sendRetryingRequest();
  }
    
    /**
     * Creates+runs a client instance that can communicate with a Raft
     * cluster.
     * @param args args[0] is my address port formatted as <address:port>
     *             args[1] is a list of comma-delimited server addresses and
     *             ports formatted as [address:port].
     */
    public static void main(String[] args) {        
        InetSocketAddress myAddress = null;
        ArrayList<InetSocketAddress> serverAddresses = null;
        
        if (args.length == 2) {
            myAddress = AddressUtils.parseAddress(args[0]);
            serverAddresses = AddressUtils.parseAddresses(args[1]);
        }

        if (args.length != 2 || myAddress == null || serverAddresses == null) {
            System.out.println("Please supply exactly two valid arguments");
            System.out.println(
                    "Usage: <myHostname:myPort> <hostname0:port0>,<hostname1:port1>,...,<hostname$n-1$,port$n-1$>");
            System.exit(1);
        }

        // Java doesn't like calling constructors without an
        // assignment to a variable, even if that variable is not used.
        RaftClient client = new RaftClient(myAddress, serverAddresses);
    }
}
