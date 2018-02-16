import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import messages.AppendEntriesReply;
import messages.AppendEntriesRequest;
import messages.RaftMessage;
import messages.RequestVoteReply;
import messages.RequestVoteRequest;
import misc.PersistentState;
import misc.PersistentStateException;


/**
 * Each server in the Raft cluster should create and maintain its own
 * Server instance. Each instance runs the Raft protocol.
 * TODO: reword comment so that it better describes what this class represents.
 * Make the comment more direct.
 */
public class RaftServer implements SerializableReceiver.SerializableHandler {
    // At most one thread should be accessing Raft server state at any point
    // in time. To help ensure this, all instance methods within this class
    // are synchronized. Furthermore, if any code in this file accesses server
    // state in a new thread without using one of the synchronized methods, 
    // a synchronized block is used with the lock being the RaftServer instance.
    
    /**
     * heartbeat timeout
     */
    private static final int HEARTBEAT_TIMEOUT_MS = 1000;

    // The election timeout is a random variable with the distribution
    // discrete Uniform(min. election timeout, max election timeout).
    // The endpoints are inclusive.
    private static final int MIN_ELECTION_TIMEOUT_MS = 3000;
    private static final int MAX_ELECTION_TIMEOUT_MS = 5000;

    /**
     * My server id
     */
    private String myId;
    /**
     * Current leader's id
     */
    @SuppressWarnings("unused")
    private String leaderId;

    // A map that maps server id to a server metadata object. This map
    // enables us to read properties and keep track of state
    // corresponding to other servers in the Raft cluster.
    private HashMap<String, ServerMetadata> peerMetadataMap;

    /**
     * PersistentState instance is used to read and write server
     * state that should be persisted onto disk.
     */
    private PersistentState persistentState;
    
    private Timer myTimer;
    private TimerTask timeoutTask;
    
    private static enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    };
    private Role role;

    /**
     * Log-specific volatile state:
     * * commitIndex - index of highest log entry known to be committed
     *                 (initialized to 0, increases monotonically)
     * * lastApplied - index of highest log entry applied to state machine
     *                 (initialized to 0, increases monotonically)
     */
    private int commitIndex;
    @SuppressWarnings("unused")
    private int lastApplied;
    
    /**
     * (Candidate-specific) Set containing server IDs that voted for me during
     * the current election term.
     */
    private Set<String> votedForMeSet;
    
    
    private SerializableSender serializableSender;

    /**
     * Tracing and debugging logger;
     * see: https://logging.apache.org/log4j/2.x/manual/api.html
     */
    private static final Logger myLogger = LogManager.getLogger();


    /**
     * @param serverAddressesMap map that maps server id to address.
     *        Contains entries for all servers in Raft cluster.
     * @param myId my server id
     */
    public RaftServer(HashMap<String, InetSocketAddress> serverAddressesMap, String myId) {
        this.myId = myId;
        leaderId = null;
        peerMetadataMap = new HashMap<String, ServerMetadata>();
        for (HashMap.Entry<String, InetSocketAddress> entry
             : serverAddressesMap.entrySet()) {
            String elemId = entry.getKey();
            InetSocketAddress elemAddress = entry.getValue();  

            if (!elemId.equals(this.myId)) {
                this.peerMetadataMap.put(elemId,
                        new ServerMetadata(elemId, elemAddress));
            }
        }
  
        persistentState = new PersistentState(this.myId);
        myTimer = new Timer();
        transitionRole(RaftServer.Role.FOLLOWER);

        this.commitIndex = -1;
        this.lastApplied = -1;

        serializableSender = new SerializableSender();
        
        // Spins up a receiver thread that manages a server socket that listens
        // for incoming messages.
        // TODO: add a note about why we don't use serializableReceiver var.
        SerializableReceiver serializableReceiver =
                new SerializableReceiver(serverAddressesMap.get(myId), this);
        
        myLogger.debug(this.myId + " :: Configuration File Defined To Be :: "+
                System.getProperty("log4j.configurationFile"));
        logMessage("successfully booted");
    }
    
    /**
     * Processes an incoming message and conditionally sends a reply depending
     * on type of message.
     * @param object incoming message
     */
    public synchronized void handleSerializable(Serializable object) {
        RaftMessage message = (RaftMessage) object;
        logMessage("Received " + message);
        if (message.term > this.persistentState.currentTerm) {
            updateTerm(message.term);
            transitionRole(RaftServer.Role.FOLLOWER);
        }
        if (message instanceof AppendEntriesRequest) {
            handleAppendEntriesRequest((AppendEntriesRequest) message);
        } else if (message instanceof RequestVoteRequest) {
            handleRequestVoteRequest((RequestVoteRequest) message);
        } else if (message instanceof AppendEntriesReply) {
            handleAppendEntriesReply((AppendEntriesReply) message);
        } else if (message instanceof RequestVoteReply) {
            handleRequestVoteReply((RequestVoteReply) message);
        } else {
            // We only support processing the message types listed above.
            assert(false);
        } 
    }

    /**
     * Processes an AppendEntries request from a leader and sends a reply.
     * @param request data corresponding to AppendEntries request
     */
    private synchronized void handleAppendEntriesRequest(AppendEntriesRequest request) {
        boolean successfulAppend = tryAndCheckSuccessfulAppend(
                (AppendEntriesRequest) request);
        AppendEntriesReply reply = new AppendEntriesReply(myId, 
            this.persistentState.currentTerm, successfulAppend);
        serializableSender.send(peerMetadataMap.get(
            request.serverId).address, reply);
    }
    
    /**
     * Examines the log and attempts to append log entry if one is present in
     * the request. Conditionally modifies server state and reports back whether
     * a successful append has taken place.
     * Only called when processing a AppendEntries request.
     * @param request data corresponding to AppendEntries request
     * @return true iff sender term is not stale and recipient log satisfies
     *              AppendEntries success conditions as specified by Raft paper.
     */
    private synchronized boolean tryAndCheckSuccessfulAppend(AppendEntriesRequest request) {
        if (request.term < this.persistentState.currentTerm) {
            return false;
        } else {
            // AppendEntries request is valid
            
            // If we were a leader, we should have downgraded to follower
            // prior to processing a valid AppendEntries request.
            assert(this.role != RaftServer.Role.LEADER);
            
            // As a candidate or follower, transition to follower to reset
            // the election timer.
            transitionRole(RaftServer.Role.FOLLOWER);
        }
        this.leaderId = request.serverId;

        boolean logIndexIsValid = request.prevLogIndex >= 0 && 
            request.prevLogIndex < this.persistentState.log.size();
        if (!logIndexIsValid) {
            return false;
        }

        // Proj2: test this code relating to the log
        // Proj2: check prevLogTerm
        boolean logTermsMatch = this.persistentState.log.get(
            request.prevLogIndex).term == request.prevLogTerm;
        if (!logTermsMatch) {
            this.persistentState.truncateAt(request.prevLogIndex);
            return false;
        }

        // Proj2: is this okay to add log entry unconditionally?
        // Otherwise, check whether this if cond. is necessary
        if (!this.persistentState.log.contains(request.entry)) {
            this.persistentState.appendLogEntry(request.entry);
        }
        // Proj2: See Figure 2, All servers section. Consider implementing the
        // the items mentioned there here.
        if (request.leaderCommit > this.commitIndex) {
            this.commitIndex = Math.min(request.leaderCommit, 
                this.persistentState.log.size() - 1);
        }
        return true;
    }

    /**
     * Processes an RequestVote request from a candidate and sends a reply.
     * @param request data corresponding to RequestVotes request
     */
    private synchronized void handleRequestVoteRequest(RequestVoteRequest request) {        
        boolean grantVote = checkGrantVote(request);
        
        if (grantVote) {
            assert(this.role == RaftServer.Role.FOLLOWER);
            // Re-transition to follower to reset election timer.
            transitionRole(RaftServer.Role.FOLLOWER);
            persistentState.setVotedFor(request.serverId);
            logMessage("granting vote to " + request.serverId);            
        }

        RequestVoteReply reply = new RequestVoteReply(myId, 
                this.persistentState.currentTerm, grantVote);
        serializableSender.send(peerMetadataMap.get(request.serverId).address, reply);
    }

    /**
     * Determines whether or not we should grant the vote based on the message.
     * Only called when processing a RequestVote request.
     * @param request data corresponding to RequestVotes request
     * @return true iff sender term is not stale, recipient can vote
     *         for the candidate, and candidate's log is at least as
     *         up-to-date as ours.
     */
    private synchronized boolean checkGrantVote(RequestVoteRequest request) {
        if (request.term < this.persistentState.currentTerm) {
            return false;
        }

        boolean canVote = this.persistentState.votedFor == null || 
            this.persistentState.votedFor.equals(request.serverId);

        if (!canVote) {
            return false;
        }

        int lastLogIndex = this.persistentState.log.size() - 1;
        int lastLogTerm = lastLogIndex < 0 ? 
                          -1 : 
                          this.persistentState.log.get(lastLogIndex).term;
        // Proj2: make sure that this logic is correct for checking that a 
        //        candidate's log is at least as up-to-date as ours.
        //        Test this logic afterwards
        boolean candidateLogIsUpdated = request.lastLogIndex >= lastLogIndex && 
            request.lastLogTerm >= lastLogTerm;
        if (!candidateLogIsUpdated) {
            return false;
        }
        
        return true;
    }

    /**
     * Processes an AppendEntries reply.
     * @param AppendEntriesReply reply to AppendEntriesReply request
     */
    private synchronized void handleAppendEntriesReply(AppendEntriesReply reply) {
        if (this.role != RaftServer.Role.LEADER) {
            return;
        }
        if (reply.term < this.persistentState.currentTerm) {
            return;
        }
        
        // Proj2: write logic to handle AppendEntries message (as leader)
        ServerMetadata meta = peerMetadataMap.get(reply.serverId);
        if (reply.successfulAppend) {
            // The current implementation assumes that we send at most one
            // log entry per AppendEntries requests (that is, no optimizations
            // to the Raft protocol).
            meta.matchIndex = meta.nextIndex;
            meta.nextIndex += 1;
            if (testMajorityN(meta.matchIndex)) {
                logMessage("updating commitIndex to " + meta.matchIndex);
                commitIndex = meta.matchIndex;
            }
        } else {
            if (meta.nextIndex > 0) {
                meta.nextIndex -= 1;
            }
        }
    }

    /**
     * Checks whether there currently exists a majority of servers whose
     * logs are identical to one another up to and including the first
     * candidateN entries.
     * Only called when processing a AppendEntries reply.
     * @param candidateN the proposed log index for which we are
     *                   checking whether we should commit up to
     * @return true iff we should update commitIndex to the
     *         proposed log index
     */
    private synchronized boolean testMajorityN(int candidateN) {
        if (candidateN<=commitIndex) {
            return false;
        }
        // count will be computed as the # of servers with >= candidateN log
        // entries. We include the leader in the count because its log index
        // is >= candidateN.
        int count = 1;
        for (ServerMetadata meta : peerMetadataMap.values()) {
            if (meta.matchIndex >= candidateN) {
                count += 1;
            }
        }
        if (count <= peerMetadataMap.size() / 2) {
            return false;
        }
        if (persistentState.log.get(candidateN).term != 
            persistentState.currentTerm) {
            return false;
        }
        return true;
    }

    /**
     * Processes an RequestVote reply.
     * @param RequestVoteReply reply to RequestVote request
     */
    private synchronized void handleRequestVoteReply(RequestVoteReply reply) {
        if (this.role != RaftServer.Role.CANDIDATE) {
            return;
        }
        if (reply.term < this.persistentState.currentTerm) {
            return;
        }
        if (reply.grantVote) {
            votedForMeSet.add(reply.serverId);
        }
        if (votedForMeSet.size() > (peerMetadataMap.size()+1)/2) {
            transitionRole(RaftServer.Role.LEADER);
        }
    }
    
    /**
     * Wrapper around role assignment that:
     * 1) changes the role of the server
     * 2) runs initial behavior corresponding to new role, even if the new role
     *    is the same as the old one.
     * 
     * We define the behavior of the following transitions in addition to the
     * transitions mentioned in the Raft paper:
     * * Follower -> Follower :: resets the election timeout.
     * * Candidate -> Candidate :: starts a new election.
     * 
     * @param role new role that the server instance transitions to. 
     */
    private synchronized void transitionRole(Role role) {
        // The defined transitions above allow us to put all the timer logic
        // and accesses in a single place/method (this method).
        
        if (this.role != role) {
            logMessage("updating role to " + role);
        }
        this.role = role;
        
        if (timeoutTask != null) {
            timeoutTask.cancel();
        }
        
        // Define a Runnable that restarts (or starts) the election timer
        // when run.
        Runnable restartElectionTimer = () -> {
            // Create a timer task to start a new election
            timeoutTask = new TimerTask() {
                public void run() {
                    transitionRole(RaftServer.Role.CANDIDATE);
                }
            };
            myTimer.schedule(timeoutTask, ThreadLocalRandom.current().nextInt(
                    MIN_ELECTION_TIMEOUT_MS, MAX_ELECTION_TIMEOUT_MS + 1));
        };
        
        switch (role) {
            case FOLLOWER:
                restartElectionTimer.run();
                break;
            case CANDIDATE:
                // Start an election
                updateTerm(persistentState.currentTerm + 1);
                persistentState.setVotedFor(myId);
                votedForMeSet = new HashSet<String>();
                votedForMeSet.add(myId);
                int lastLogIndex = persistentState.log.size()-1;
                // lastLogTerm = -1 means there are no log entries
                int lastLogTerm = lastLogIndex < 0 ?
                        -1 : persistentState.log.get(lastLogIndex).term;

                logMessage("new election - broadcasting RequestVote requests");
                RaftMessage message = new RequestVoteRequest(myId, 
                    persistentState.currentTerm, lastLogIndex, lastLogTerm);

                for (ServerMetadata meta : peerMetadataMap.values()) {
                    serializableSender.send(meta.address, message);
                }
                restartElectionTimer.run();
                break;
            case LEADER:
                // Initializes volatile state specific to leader role.
                for (ServerMetadata meta : peerMetadataMap.values()) {
                    // Subtracting 1 makes it apparent that we want to send
                    // the entry corresponding to the next available index
                    // With proper synchronization, this ensures that we will
                    // initially send empty AppendEntries requests.
                    meta.nextIndex = (persistentState.log.size() - 1) + 1;
                    meta.matchIndex = -1;
                }

                // Create a timer task to send heartbeats
                timeoutTask = new TimerTask() {
                    public void run() {
                        synchronized(RaftServer.this) {
                            // send heartbeat messages with zero or more log
                            // entries after a heartbeat timeout has passed
                            logMessage("broadcasting heartbeat messages");

                            for (ServerMetadata meta : peerMetadataMap.values()) {
                                // Proj2: send server-tailored messages to each server
                                // Proj2: add suitable log entry (if needed) as argument into
                                //        AppendEntriesRequest
                                RaftMessage message = new AppendEntriesRequest(myId, 
                                    persistentState.currentTerm, -1, -1, null, commitIndex);
                                serializableSender.send(meta.address, message);
                            }
                        }
                    }
                };
                
                myTimer.scheduleAtFixedRate(timeoutTask, 0, HEARTBEAT_TIMEOUT_MS);
                break;
        }
    }
    
    /**
     * Wrapper around current term setter that enforces some invariants
     * relating to updating our knowledge of the current term.
     * @param newTerm new term that we want to update the current term to.
     */
    private synchronized void updateTerm(int newTerm) {
        this.persistentState.setTerm(newTerm);
        this.persistentState.setVotedFor(null);
    }

    /**
     * Wrapper around info logging that makes sure our logs are annotated with
     * server-specific properties of interest (e.g., server id, current term).
     * @param message any message that we wish to log
     */
    private synchronized void logMessage(Object message) {
        myLogger.info(myId + " :: term=" + this.persistentState.currentTerm + 
            " :: " + role + " :: " + message);
    }
    
    /**
     * An instance of this class is created for every other server in the Raft
     * cluster. These instances are used to help the running server read
     * properties and keep track of state corresponding to the other servers.
     */
    public class ServerMetadata {
        public String id; // Each server has a unique id
        public InetSocketAddress address; // Each server has a unique address
        // Index of the next log entry to send to that server
        public int nextIndex;
        // Index of highest log entry known to be replicated on server 
        public int matchIndex;

        /**
         * @param id      see above
         * @param address see above
         */
        public ServerMetadata(String id, InetSocketAddress address) {
            this.id = id;
            this.address = address;
            this.nextIndex = 0;
            this.matchIndex = -1;
        }
    }
    
    /**
     * Creates+runs a server instance that follows the Raft protocol.
     * @param args args[0] is a comma-delimited ports list (0-indexed)
     *             args[1] is a port index to determine the port for
     *               which the server will start a listener channel on
     */
    public static void main(String[] args) {
        int myPortIndex = -1;
        String[] allPortStrings = null;
        int[] allPorts = null;
        boolean validArgs = true;

        // This if-else block checks to see if supplied arguments are valid.
        if (args.length != 2) {
            validArgs = false;
        } else {
            allPortStrings = args[0].split(",");
            allPorts = new int[allPortStrings.length];
            try {
                myPortIndex = Integer.parseInt(args[1]);
                if (myPortIndex < 0 || myPortIndex >= allPortStrings.length) {
                    validArgs = false;
                }
                for (int i=0; i<allPortStrings.length; i++) {
                    allPorts[i] = Integer.parseInt(allPortStrings[i]);
                }
            } catch (NumberFormatException e) {
                validArgs = false;
            }
        }
        if (!validArgs) {
            System.out.println("Please supply exactly two valid arguments");
            System.out.println(
                "Usage: <port0>,<port1>,...,<port$n-1$> <myPortIndex>");
            System.out.println("Note: List of ports is 0-indexed");
            System.exit(1);
        }

        System.setProperty("log4j.configurationFile", "./src/log4j2.xml");
        HashMap<String, InetSocketAddress> serverAddressesMap = 
            new HashMap<String, InetSocketAddress>();
        for (int i=0; i<allPorts.length; i++) {
            serverAddressesMap.put("Server" + i, new 
                InetSocketAddress("localhost", allPorts[i]));   
        }
        
        // Java doesn't like calling constructors without an
        // assignment to a variable, even if that variable is not used.
        RaftServer myServer = new RaftServer(serverAddressesMap, "Server"+myPortIndex);
    }

}
