import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;

import messages.ClientReply;
import messages.ClientRequest;

public class RaftClient implements SerializableReceiver.Handler {
    
    // TODO: see if this retry actually works
    /**
     * Amount of time elapsed without receiving an associated response before
     * we retry sending a request.
     */
    private static final int RETRY_TIMEOUT_MS = 5000;
    
    // TODO: figure out which instance variables could be moved to the
    // constructor instead.
    
    /**
     * The address that I use to receive replies.
     */
    private InetSocketAddress myAddress;
    
    /**
     * Address of the server that we think is a leader.
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
     * Scanner for user command line input.
     */
    private Scanner commandReader;
    
    public RaftClient(InetSocketAddress myAddress, ArrayList<InetSocketAddress> serverAddresses) {
        synchronized(this) {
            this.myAddress = myAddress;
            this.serverAddresses = serverAddresses;
                    
            randomizeLeaderAddress();

            commandReader = new Scanner(System.in);
            
            retryTimer = new Timer();
            
            outstandingRequest = null;
            
            serializableSender = new SerializableSender();
            SerializableReceiver serializableReceiver =
                    new SerializableReceiver(this.myAddress, this);
            
            waitForAndProcessInput();
        }
    }
    
    private synchronized void randomizeLeaderAddress() {
        leaderAddress = serverAddresses.get(ThreadLocalRandom.current().nextInt(
                serverAddresses.size())); 
    }
    
    private synchronized void waitForAndProcessInput() {
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
    
    public synchronized void handleSerializable(Serializable object) {
        if (!(object instanceof ClientReply)) {
            System.out.println("Don't know how to process serializable object: " + object);
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
        retryRequestTaskInfo.task.cancel();
        retryRequestTaskInfo.taskCancelled = true;
        outstandingRequest = null;
        waitForAndProcessInput();
    }
    
    public static void main(String[] args) {
        // TODO: do argument parsing here to get the list of server addresses
        // TODO: ensure that list of servers is non-empty
        
        // TODO: get rid of this hardcoding
        ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();
        
        for (int i = 0; i < 3; i++) {
            serverAddresses.add(new InetSocketAddress("localhost", 6060 + i));
        }
        
        RaftClient myClient = new RaftClient(new InetSocketAddress("localhost", 6070), serverAddresses);
    }
}
