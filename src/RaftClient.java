import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;

import messages.ClientReply;
import messages.ClientRequest;

public class RaftClient implements SerializableReceiver.Handler {
    
    /**
     * Amount of time to wait for a reply before we send another request.
     */
    private static final int RETRY_TIMEOUT_MS = 5000;
    
    // TODO: figure out which instance variables could be moved to the
    // constructor instead.
    
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
    private Timer retryTimer;

    class TimerTaskInfo {
        TimerTask task;
        boolean taskCancelled;
    }
    private TimerTaskInfo retryRequestTaskInfo;
        
    private SerializableSender serializableSender;
    
    /**
     * The current request for which we have not yet received a successful reply
     */
    private ClientRequest outstandingRequest;
        
    /**
     * Scanner for receiving command line input from the user.
     */
    private Scanner commandReader;
    
    public RaftClient(InetSocketAddress myAddress, ArrayList<InetSocketAddress> serverAddresses) {
        synchronized(this) {
            this.myAddress = myAddress;
            this.serverAddresses = serverAddresses;
                    
            randomizeLeaderAddress();

            commandReader = new Scanner(System.in);
            outstandingRequest = null;
            retryTimer = new Timer();

            serializableSender = new SerializableSender();
            SerializableReceiver serializableReceiver =
                    new SerializableReceiver(this.myAddress, this);
            
            waitForAndProcessInput();
        }
    }
    
    public synchronized void handleSerializable(Serializable object) {
        if (!(object instanceof ClientReply)) {
            System.out.println("Don't know how to process the serializable object: " + object);
            return;
        }
        
        ClientReply reply = (ClientReply) object;
        if (!reply.success) {
            if (reply.leaderAddress == null) {
                return;
            }
            leaderAddress = reply.leaderAddress;
            serializableSender.send(leaderAddress, outstandingRequest);
            return;
        }
        
        System.out.println(reply.result);
        outstandingRequest = null;
        retryRequestTaskInfo.task.cancel();
        retryRequestTaskInfo.taskCancelled = true;
        waitForAndProcessInput();
    }
    
    private synchronized void randomizeLeaderAddress() {
        leaderAddress = serverAddresses.get(ThreadLocalRandom.current().nextInt(
                serverAddresses.size())); 
    }
    
    private synchronized void waitForAndProcessInput() {
        System.out.println("Please enter a bash command:");
        String command = commandReader.nextLine();
        outstandingRequest = new ClientRequest(myAddress, command);
        serializableSender.send(leaderAddress, outstandingRequest);
        
        retryRequestTaskInfo = new TimerTaskInfo(); 
        retryRequestTaskInfo.task = new TimerTask() {
                public void run() {
                    synchronized(RaftClient.this) {
                        if (retryRequestTaskInfo.taskCancelled) {
                            return;
                        }

                        randomizeLeaderAddress();
                        serializableSender.send(leaderAddress, outstandingRequest);
                    }
                }
        };
        retryRequestTaskInfo.taskCancelled = false;
        retryTimer.scheduleAtFixedRate(retryRequestTaskInfo.task, RETRY_TIMEOUT_MS, RETRY_TIMEOUT_MS);
  }
    
    public static void main(String[] args) {
        // TODO: do argument parsing here to get the list of server addresses
        // TODO: ensure that list of server addresses passed in is non-empty
        
        // TODO: get rid of all of this hardcoding
        ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();
        
        for (int i = 0; i < 3; i++) {
            serverAddresses.add(new InetSocketAddress("localhost", 6060 + i));
        }
        
        RaftClient myClient = new RaftClient(new InetSocketAddress("localhost", 6070), serverAddresses);
    }
}
