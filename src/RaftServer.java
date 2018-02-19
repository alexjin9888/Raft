import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import messages.ClientReply;
import messages.ClientRequest;
import messages.RaftMessage;
import messages.RequestVoteReply;
import messages.RequestVoteRequest;
import misc.PersistentState;
import misc.PersistentStateException;
import misc.CheckingCancelTimerTask;
import units.LogEntry;


/**
 * Each server in the Raft cluster should create and maintain its own
 * Server instance. Each instance runs the Raft protocol.
 * TODO: reword comment so that it better describes what this class represents.
 * Make the comment more direct.
 */
public class RaftServer implements NetworkManager.SerializableHandler {
    // To ensure that at most one thread accesses server state at any given
    // time, all instance methods within this class are synchronized.
    // Furthermore, if any code in this file accesses server state in a new
    // thread without using one of the synchronized methods, a synchronized
    // block is used with the lock being the RaftServer instance.

    /**
     * Time elapsed before we send out another round of heartbeat messages
     */
    private static final int HEARTBEAT_TIMEOUT_MS = 1000;

    // TODO differentiate min and max
    /**
     * The election timeout is a random variable with the distribution
     * discrete Uniform(min. election timeout, max election timeout).
     * The endpoints are inclusive.
     */
    private static final int MIN_ELECTION_TIMEOUT_MS = 3000;
    /**
     * The election timeout is a random variable with the distribution
     * discrete Uniform(min. election timeout, max election timeout).
     * The endpoints are inclusive.
     */
    private static final int MAX_ELECTION_TIMEOUT_MS = 5000;

    /**
     * Value used when trying to get the term of a non-existent log entry.
     */
    private static final int UNDEFINED_LOG_TERM = -1;

    /**
     * My server id
     */
    private String myId;
    /**
     * My server address
     */
    private InetSocketAddress myAddress;
    /**
     * Current leader's id
     */
    @SuppressWarnings("unused")
    private String leaderId;

    /**
     * An instance of this class is created for every other server in the Raft
     * cluster. These instances are used to help the running server read
     * properties and keep track of state corresponding to the other servers.
     */
    private class ServerMetadata {
        InetSocketAddress address; // Each server has a unique address
        // Index of the next log entry to send to that server
        int nextIndex;
        // Index of highest log entry known to be replicated on server 
        int matchIndex;
    }
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

    private CheckingCancelTimerTask myTimerTask;

    private static enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    };
    private Role role;

    /**
     * index of highest log entry known to be committed 
     * (initialized to index of last applied log entry, increases monotonically)
     */
    private int commitIndex;

    /**
     * (Candidate-specific) Set containing server IDs that voted for me during
     * the current election term.
     */
    private Set<String> votedForMeSet;


    private NetworkManager networkManager;

    /**
     * Single thread manager that will execute the commands for us in order.
     */
    private ExecutorService commandApplierService;
    /**
     * Map that we can query to see whether we need to reply to a particular
     * client after applying the command of a log entry.
     */
    private HashMap<LogEntry, ClientRequest> outstandingClientRequestsMap;
    
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
        synchronized(RaftServer.this) {
            // As part of initialization, spins up two threads to help this inst.
            // carry out the Raft protocol. One thread is a timer thread (see
            // myTimer initialization) and another thread is a Serializable receiver
            // thread (see serializableReceiver initialization). 
            // The thread that runs this constructor will exit after starting up
            // the receiver thread.

            this.myId = myId;
            leaderId = null;
            peerMetadataMap = new HashMap<String, ServerMetadata>();
            for (HashMap.Entry<String, InetSocketAddress> entry
                    : serverAddressesMap.entrySet()) {
                String elemId = entry.getKey();
                InetSocketAddress elemAddress = entry.getValue();  

                if (!elemId.equals(this.myId)) {
                    ServerMetadata peerMetadata = new ServerMetadata();
                    peerMetadata.address = elemAddress;
                    this.peerMetadataMap.put(elemId, peerMetadata);
                } else {
                    myAddress = elemAddress;
                }
            }

            persistentState = new PersistentState(this.myId);
            // The last applied value from persistent state may not be the
            // most up-to-date commit index in the Raft cluster, but we will
            // quickly update our commit index as necessary after talking with
            // peers.
            commitIndex = this.persistentState.lastApplied;
       
            this.commandApplierService = Executors.newSingleThreadExecutor();
            outstandingClientRequestsMap = new HashMap<LogEntry, ClientRequest>();

            // Spins up a thread that allows us to schedule periodic tasks
            myTimer = new Timer();

            // TODO: mention that this may also depend on serializable sender
            // being initialized. look into this
            // Assumes that persistent state and timer have been initialized before
            // we transition to follower and perform initial follower behavior.
            this.role = null;
            transitionRole(Role.FOLLOWER);

            networkManager = new NetworkManager(myAddress, this);
            logMessage("successfully booted");
        }
    }

    /**
     * Processes an incoming message and conditionally sends a reply depending
     * on type of message.
     * @param object incoming message
     */
    public synchronized void handleSerializable(Serializable object) {
        logMessage("Received " + object);
        if (object instanceof ClientRequest) {
            handleClientRequest((ClientRequest) object);
            return;
        }
        if (!(object instanceof RaftMessage)) {
            logMessage("Don't know how to process the serializable object: " + object);
            return;
        }
        RaftMessage message = (RaftMessage) object;
        if (message.term > this.persistentState.currentTerm) {
            updateTerm(message.term);
            transitionRole(Role.FOLLOWER);
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
            logMessage("Don't know how to process the Raft message: " + message);
            return;
        }
    }

    /**
     * Processes a request sent from client and schedule sending a reply.
     * @param request data corresponding to client request
     */
    private synchronized void handleClientRequest(ClientRequest request) {
        if (this.role==Role.LEADER) {
            LogEntry logEntry = new LogEntry(this.persistentState.log.size(), 
                    this.persistentState.currentTerm, request.command);
            ArrayList<LogEntry> logEntries = new ArrayList<LogEntry>();
            logEntries.add(logEntry);
            this.persistentState.appendLogEntries(logEntries);
            outstandingClientRequestsMap.put(logEntry, request);
        } else {
            InetSocketAddress leaderAddress =
                    peerMetadataMap.get(leaderId).address;
            ClientReply reply = new ClientReply(request.commandId, leaderAddress, false, null);
            logMessage("Sending " + reply);
            networkManager.sendSerializable(request.clientAddress, reply);
        }
    }

    /**
     * Processes an AppendEntries request from a leader and sends a reply.
     * @param request data corresponding to AppendEntries request
     */
    private synchronized void handleAppendEntriesRequest(AppendEntriesRequest request) {
        boolean senderTermStale = request.term < this.persistentState.currentTerm;
        
        if (!senderTermStale) {
            // Here, AppendEntries request is valid

            // If we were a leader, we should have downgraded to follower
            // prior to processing a valid AppendEntries request.
            assert(this.role != Role.LEADER);

            // As a candidate or follower, transition to follower to reset
            // the election timer.
            transitionRole(Role.FOLLOWER);
            this.leaderId = request.serverId;
        }
        
        boolean successfulAppend = false;
        int nextIndex;

        if (!senderTermStale && checkCanAppend(request.prevLogIndex, request.prevLogTerm)) {
            this.persistentState.truncateAt(request.prevLogIndex+1);
            this.persistentState.appendLogEntries(request.entries);
            successfulAppend = true;
            updateCommitIndex(Math.min(request.leaderCommit,
                this.persistentState.log.size() - 1));
            nextIndex = request.prevLogIndex + 1 + request.entries.size();
        } else {
            nextIndex = Math.max(request.prevLogIndex, 0);
        }

        AppendEntriesReply reply = new AppendEntriesReply(myId,
                this.persistentState.currentTerm, successfulAppend, nextIndex);
        networkManager.sendSerializable(peerMetadataMap.get(
                request.serverId).address, reply);
    }

    // TODO implement this
    private String execute(String command) {
        return command;
    }

    /**
     * Checks whether we can append the sent log entries to our log.
     * Only called when processing a AppendEntries request.
     * @param senderPrevLogIndex prevLogIndex field of the request.
     * @param senderPrevLogterm  prevLogTerm field of the request.
     * @return true iff we can append the sent entries.
     */
    private synchronized boolean checkCanAppend(int senderPrevLogIndex, int senderPrevLogTerm) {
        if (senderPrevLogIndex == -1) {
            // Here, the sender is trying to get us to append zero or more
            // entries starting at index zero
            return true;
        }

        if (!logEntryHasIndex(senderPrevLogIndex)) {
            return false;
        }

        return this.persistentState.log.get(senderPrevLogIndex).term
                == senderPrevLogTerm;
    }

    /**
     * Processes an RequestVote request from a candidate and sends a reply.
     * @param request data corresponding to RequestVotes request
     */
    private synchronized void handleRequestVoteRequest(RequestVoteRequest request) {        
        boolean grantVote = checkGrantVote(request);

        if (grantVote) {
            assert(this.role == Role.FOLLOWER);
            // Re-transition to follower to reset election timer.
            transitionRole(Role.FOLLOWER);
            persistentState.setVotedFor(request.serverId);
            logMessage("granting vote to " + request.serverId);            
        }

        RequestVoteReply reply = new RequestVoteReply(myId, 
                this.persistentState.currentTerm, grantVote);
        networkManager.sendSerializable(peerMetadataMap.get(request.serverId).address, reply);
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

        boolean votedForAnotherServer = !(this.persistentState.votedFor == null
                || this.persistentState.votedFor.equals(request.serverId));

        if (votedForAnotherServer) {
            return false;
        }

        // The rest of this method makes sure that we grant vote iff the
        // candidate log is up-to-date.

        if (this.persistentState.log.size()==0) {
            return true;
        }
        if (request.lastLogIndex == -1) {
            // Here, our sender's log is empty and ours is not
            return false;
        }

        int lastLogIndex = this.persistentState.log.size() - 1;
        int lastLogTerm = this.persistentState.log.get(lastLogIndex).term;

        if (request.lastLogTerm > lastLogTerm) {
            return true;
        }
        if (request.lastLogTerm < lastLogTerm) {
            return false;
        }

        return request.lastLogIndex >= lastLogIndex;
    }

    /**
     * Processes an AppendEntries reply.
     * @param AppendEntriesReply reply to AppendEntriesReply request
     */
    private synchronized void handleAppendEntriesReply(AppendEntriesReply reply) {
        if (this.role != Role.LEADER) {
            return;
        }
        if (reply.term < this.persistentState.currentTerm) {
            return;
        }

        ServerMetadata meta = peerMetadataMap.get(reply.serverId);
        meta.nextIndex = reply.nextIndex;

        if (reply.successfulAppend) {
            meta.matchIndex = meta.nextIndex - 1;
            Integer[] commitIndices = new Integer[peerMetadataMap.size()+1];
            int i = 0;
            for (ServerMetadata m : peerMetadataMap.values()) {
                commitIndices[i] = m.matchIndex;
                i += 1;
            }
            commitIndices[commitIndices.length-1] =
                this.persistentState.log.size();
            Arrays.sort(commitIndices, Collections.reverseOrder());
            updateCommitIndex(commitIndices[commitIndices.length / 2]);
        }
    }

    /**
     * Processes an RequestVote reply.
     * @param RequestVoteReply reply to RequestVote request
     */
    private synchronized void handleRequestVoteReply(RequestVoteReply reply) {
        if (this.role != Role.CANDIDATE) {
            return;
        }
        if (reply.term < this.persistentState.currentTerm) {
            return;
        }
        if (reply.grantVote) {
            votedForMeSet.add(reply.serverId);
        }
        if (votedForMeSet.size() > (peerMetadataMap.size()+1)/2) {
            transitionRole(Role.LEADER);
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
        // The defined transitions above allow us to put/contain all the Raft
        // timer logic and accesses in this method.

        if (this.role != role) {
            logMessage("updating role to " + role);
        }
        this.role = role;

        if (myTimerTask != null) {
            myTimerTask.cancel();
        }

        // This restarts (or starts) the election timer when run.
        Runnable restartElectionTimer = () -> {

            // Create a timer task to start a new election.
            myTimerTask = new CheckingCancelTimerTask() {
                public void run() {
                    synchronized(RaftServer.this) {
                        // Any role transition that happens prior to the
                        // RaftServer instance lock being acquired will
                        // invalidate this task.
                        if (this.isCancelled) {
                            return;
                        }
                        transitionRole(Role.CANDIDATE);
                    }
                }
            };

            myTimer.schedule(myTimerTask, ThreadLocalRandom.current().nextInt(
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
            int lastLogIndex = persistentState.log.size() - 1;
            int lastLogTerm = logEntryHasIndex(lastLogIndex) ?
                    persistentState.log.get(lastLogIndex).term : UNDEFINED_LOG_TERM;

                    logMessage("new election - broadcasting RequestVote requests");
                    RaftMessage request = new RequestVoteRequest(myId, 
                            persistentState.currentTerm, lastLogIndex, lastLogTerm);

                    for (ServerMetadata meta : peerMetadataMap.values()) {
                        networkManager.sendSerializable(meta.address, request);
                    }
                    restartElectionTimer.run();
                    break;
        case LEADER:
            // Initializes volatile state specific to leader role.
            for (ServerMetadata meta : peerMetadataMap.values()) {
                meta.nextIndex = persistentState.log.size();
                meta.matchIndex = -1;
            }

            this.leaderId = myId;
            
            this.outstandingClientRequestsMap.clear();

            // Create a timer task to send heartbeats.
            myTimerTask = new CheckingCancelTimerTask() {
                public void run() {
                    synchronized(RaftServer.this) {
                        // Any role transition that happens prior to the
                        // RaftServer instance lock being acquired will
                        // invalidate this task.
                        if (this.isCancelled) {
                            return;
                        }

                        // send heartbeat messages with zero or more log
                        // entries after a heartbeat timeout has passed
                        logMessage("broadcasting heartbeat messages");

                        for (ServerMetadata meta : peerMetadataMap.values()) {
                            int prevLogIndex = meta.nextIndex - 1;
                            int prevLogTerm = logEntryHasIndex(prevLogIndex)
                                    ? persistentState.log.get(prevLogIndex).term
                                            : UNDEFINED_LOG_TERM;
                                    ArrayList<LogEntry> logEntries =
                                            new ArrayList<LogEntry>();
                                    // Note: Currently, we send at most one log
                                    // entry to the recipient. This may be something
                                    // we want to optimize in the future.
                                    if (logEntryHasIndex(meta.nextIndex)) {
                                        logEntries.add(persistentState.log.get(meta.nextIndex));
                                    }
                                    AppendEntriesRequest request =
                                            new AppendEntriesRequest(myId, 
                                                    persistentState.currentTerm, prevLogIndex,
                                                    prevLogTerm, logEntries, commitIndex);
                                    networkManager.sendSerializable(meta.address, request);
                        }
                    }
                }
            };

            myTimer.scheduleAtFixedRate(myTimerTask, 0, HEARTBEAT_TIMEOUT_MS);
            break;
        }
    }

    private boolean logEntryHasIndex(int index) {
        return index >= 0 && index < this.persistentState.log.size();
    }

    private synchronized void updateCommitIndex(int newCommitIndex) {
        if (newCommitIndex <= this.commitIndex) {
            return;
        }
        for (int i=commitIndex+1; i <= newCommitIndex; i++) {
            LogEntry logEntry = this.persistentState.log.get(i);
            commandApplierService.submit(() -> {
                String result = execute(logEntry.command);
                synchronized(RaftServer.this) {                    
                    this.persistentState.incrementLastApplied();
                    if (this.role!=Role.LEADER) {
                        return;
                    }
                    ClientRequest clientRequest = 
                            this.outstandingClientRequestsMap.get(logEntry);
                    if (clientRequest == null) {
                        return;
                    }
                    ClientReply reply =
                            new ClientReply(clientRequest.commandId, myAddress, true, result);
                                        
                    networkManager.sendSerializable(clientRequest.clientAddress, reply);
                    this.outstandingClientRequestsMap.remove(logEntry);
                }
            });
        }
        this.commitIndex = newCommitIndex;
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
     * Creates+runs a server instance that follows the Raft protocol.
     * @param args args[0] is a comma-delimited ports list (0-indexed)
     *             args[1] is a port index to determine the port for
     *               which the server will start a listener channel on
     */
    public static void main(String[] args) {
        // TODO: ensure that list of server addresses passed in is non-empty

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
