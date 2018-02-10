import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.HashMap;
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
import singletons.PersistentState;
import units.ServerMetadata;


/**
 * Each server in the Raft cluster should create and maintain its own
 * Server instance. Each instance runs the Raft protocol.
 * TODO: reword comment so that it better describes what this class represents.
 * Make the comment more direct.
 */
public class Server implements SerializableReceiver.SerializableHandler {
    // All synchronized blocks and methods present in this class are to ensure
    // that at most one thread reads and modifies server state at any given
    // time. Any instance method called in this class that does not have the
    // synchronized keyword has a top-level ancestor method in the call stack
    // whose method signature contains the synchronized keyword.
    /**
     * heartbeat interval in ms
     */
    private static final int HEARTBEAT_INTERVAL = 1000;

    // The election timeout is a random variable with the distribution
    // discrete Uniform(min. election timeout, max election timeout).
    // The endpoints are inclusive.
    private static final int MIN_ELECTION_TIMEOUT = 3000; // in ms
    private static final int MAX_ELECTION_TIMEOUT = 5000; // in ms

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
     * (Candidate-specific) Number of votes received for an election.
     */
    private int votesReceived;
    
    private Timer electionTimer;
    private Timer heartbeatTimer;
    
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
    public Server(HashMap<String, InetSocketAddress> serverAddressesMap, String myId) {
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

        try {
            persistentState = new PersistentState(this.myId);
        } catch (IOException e) {
            // TODO: print more specific error message depending on the
            // type of I/O error (e.g., persistent state failed to load).
            // Note: This is a fatal error.
            // TODO: Maybe I will want to throw a runtime exception in
            // the PersistentState class instead.
            e.printStackTrace();
            System.exit(1);
        }
        
        transitionRole(Server.Role.FOLLOWER);

        this.commitIndex = -1;
        this.lastApplied = -1;

        serializableSender = new SerializableSender();
        
        // Spins up a receiver thread that manages a server socket that listens
        // for incoming messages.
        SerializableReceiver serializableReceiver =
                new SerializableReceiver(serverAddressesMap.get(myId), this);
        
        myLogger.debug(this.myId + " :: Configuration File Defined To Be :: "+
                System.getProperty("log4j.configurationFile"));
        logMessage("successfully booted");
    }
    
    /**
     * Changes the role of the server instance and runs initial behavior
     * corresponding to new role, even if the new role is the same as the old
     * one.
     * @param role new role that the server instance transitions to. 
     */
    private void transitionRole(Role role) {
        
        if (this.role != role) {
            logMessage("updating role to " + role);
        }
        this.role = role;
        
        if (electionTimer != null) {
            electionTimer.cancel();
        }
        
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
        }
        
        switch (role) {
            case FOLLOWER:
                resetCountdownUntilElection();
                break;
            case CANDIDATE:
                // Start an election
                updateTerm(persistentState.currentTerm + 1);
                persistentState.votedFor = myId;
                votesReceived = 1;
                int lastLogIndex = persistentState.log.size()-1;
                // lastLogTerm = -1 means there are no log entries
                int lastLogTerm = lastLogIndex < 0 ?
                        -1 : persistentState.log.get(lastLogIndex).term;

                logMessage("new election - broadcasting RequestVote requests");
                RaftMessage message = new RequestVoteRequest(myId, 
                    persistentState.currentTerm, lastLogIndex, lastLogTerm);

                for (ServerMetadata meta : peerMetadataMap.values()) {
                    saveStateAndSendMessage(meta, message);
                }
                resetCountdownUntilElection();
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

                heartbeatTimer = new Timer();
                TimerTask sendHeartbeats = new TimerTask() {
                    public void run() {
                        synchronized(Server.this) {
                            // send heartbeat messages with zero or more log
                            // entries after a heartbeat interval has passed
                            logMessage("broadcasting heartbeat messages");

                            for (ServerMetadata meta : peerMetadataMap.values()) {
                                // Proj2: send server-tailored messages to each server
                                // Proj2: add suitable log entry (if needed) as argument into
                                //        AppendEntriesRequest
                                RaftMessage message = new AppendEntriesRequest(myId, 
                                    persistentState.currentTerm, -1, -1, null, commitIndex);
                                saveStateAndSendMessage(meta, message);
                            }
                        }
                    }
                };
                
                heartbeatTimer.scheduleAtFixedRate(sendHeartbeats, 0, HEARTBEAT_INTERVAL);
                break;
        }
    }
    
    private void resetCountdownUntilElection() {
        electionTimer = new Timer();
        TimerTask startElection = new TimerTask() {
            public void run() {
                synchronized(Server.this) {
                    // Starts a new election
                    transitionRole(Server.Role.CANDIDATE);
                }
            }
        };
        electionTimer.schedule(startElection, ThreadLocalRandom.current().nextInt(
                MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
    }

    /**
     * Wrapper around NetworkUtils.sendMessage that enforces the
     * invariant that we save persistent state to disk before sending
     * any network requests to recipients.
     * @param recipientMeta metadata about the recipient incl. address
     * @param message message that we want to send to recipient
     */
    private void saveStateAndSendMessage(ServerMetadata recipientMeta, 
        RaftMessage message) {
        // If I experience an I/O error while saving the persistent state,
        // don't try to send the request.
        try {
            this.persistentState.save();
            serializableSender.send(recipientMeta.address, message); 
        } catch (IOException e) {
            // TODO: Verify that the only type of error reported here
            // should be a persistent state error.
            
            // TODO: we need to check if the IO error came from persistent state
            // if so, we will probably want to die here instead of continuing
            // operation, since this is a fatal error.
        }
    }

    /**
     * Wrapper around info logging that makes sure our logs are annotated with
     * server-specific properties of interest (e.g., server id, current term).
     * @param message any message that we wish to log
     */
    private void logMessage(Object message) {
        myLogger.info(myId + " :: term=" + this.persistentState.currentTerm + 
            " :: " + role + " :: " + message);
    }

    /**
     * Processes an incoming message and conditionally sends a reply depending
     * on type of message.
     * @param object incoming message
     */
    public synchronized void handleSerializable(Serializable object) {
        RaftMessage message = (RaftMessage) object;
        logMessage("received " + message);
        if (message.term > this.persistentState.currentTerm) {
            updateTerm(message.term);
            transitionRole(Server.Role.FOLLOWER);
        }
        if (message instanceof AppendEntriesRequest) {
            processAppendEntriesRequest((AppendEntriesRequest) message);
        } else if (message instanceof RequestVoteRequest) {
            processRequestVoteRequest((RequestVoteRequest) message);
        } else if (message instanceof AppendEntriesReply) {
            processAppendEntriesReply((AppendEntriesReply) message);
        } else if (message instanceof RequestVoteReply) {
            processRequestVoteReply((RequestVoteReply) message);
        } else {
            // We only support processing the message types listed above.
            assert(false);
        } 
    }

    /**
     * Processes an AppendEntries request from a leader and sends a reply.
     * @param request data corresponding to AppendEntries request
     */
    private void processAppendEntriesRequest(AppendEntriesRequest request) {
        boolean successfulAppend = tryAndCheckSuccessfulAppend(
                (AppendEntriesRequest) request);
        AppendEntriesReply reply = new AppendEntriesReply(myId, 
            this.persistentState.currentTerm, successfulAppend);
        saveStateAndSendMessage(peerMetadataMap.get(
            request.serverId), reply);
    }
    
    /**
     * Examines the log and conditionally modifies server state to check
     * whether a successful append has taken place.
     * Only called when processing a AppendEntries request.
     * @param request data corresponding to AppendEntries request
     * @return true iff sender term is not stale and recipient log
     *         contained entry matching prevLogIndex and prevLogTerm
     */
    private boolean tryAndCheckSuccessfulAppend(AppendEntriesRequest request) {
        if (request.term < this.persistentState.currentTerm) {
            return false;
        } else {
            // AppendEntries request is valid
            
            // If we were a leader, we should have downgraded to follower
            // prior to processing a valid AppendEntries request.
            assert(this.role != Server.Role.LEADER);
            
            switch(role) {
                case FOLLOWER:
                    resetCountdownUntilElection();
                    break;
                case CANDIDATE:
                    transitionRole(Server.Role.FOLLOWER);
                    break;
            }
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
            this.persistentState.log =
                this.persistentState.log.subList(0, request.prevLogIndex);
            return false;
        }

        // Proj2: is this okay to add log entry unconditionally?
        // Otherwise, check whether this if cond. is necessary
        if (!this.persistentState.log.contains(request.entry)) {
            this.persistentState.log.add(request.entry);
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
    private void processRequestVoteRequest(RequestVoteRequest request) {        
        boolean grantVote = checkGrantVote(request);
        
        if (grantVote) {
            assert(this.role == Server.Role.FOLLOWER);
            resetCountdownUntilElection();
            this.persistentState.votedFor = request.serverId;
            logMessage("granting vote to " + request.serverId);            
        }

        RequestVoteReply reply = new RequestVoteReply(myId, 
                this.persistentState.currentTerm, grantVote);
        saveStateAndSendMessage(peerMetadataMap.get(request.serverId), reply);
    }

    /**
     * Determines whether or not we should grant the vote based on the message.
     * Only called when processing a RequestVote request.
     * @param request data corresponding to RequestVotes request
     * @return true iff sender term is not stale, recipient can vote
     *         for the candidate, and candidate's log is at least as
     *         up-to-date as ours.
     */
    private boolean checkGrantVote(RequestVoteRequest request) {
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
    private void processAppendEntriesReply(AppendEntriesReply reply) {
        if (this.role != Server.Role.LEADER) {
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
    private boolean testMajorityN(int candidateN) {
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
    private void processRequestVoteReply(RequestVoteReply reply) {
        if (this.role != Server.Role.CANDIDATE) {
            return;
        }
        if (reply.term < this.persistentState.currentTerm) {
            return;
        }
        if (reply.grantVote) {
            votesReceived += 1;
        }
        if (votesReceived > (peerMetadataMap.size()+1)/2) {
            transitionRole(Server.Role.LEADER);
        }
    }
    
    /**
     * Wrapper around current term setter that enforces some invariants
     * relating to updating our knowledge of the current term.
     * @param newTerm new term that we want to update the current term to.
     */
    private void updateTerm(int newTerm) {
        this.persistentState.currentTerm = newTerm;
        this.persistentState.votedFor = null;
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
        Server myServer = new Server(serverAddressesMap, "Server"+myPortIndex);
    }

}
