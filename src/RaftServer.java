import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import misc.LogEntry;
import misc.NetworkManager;


/**
 * Each server in the Raft cluster should create and maintain its own
 * Server instance. Each instance runs the Raft protocol.
 * COMMENT2DO: reword comment so that it better describes what this class represents.
 * Make the comment more direct.
 */
public class RaftServer {
    // To ensure that at most one thread accesses server state at any given
    // time, all instance methods within this class are synchronized.
    // Furthermore, if any code in this file accesses server state in a new
    // thread without using one of the synchronized methods, a synchronized
    // block is used with the lock being the RaftServer instance.

    /**
     * Time elapsed before we send out another round of heartbeat messages
     */
    private static final int HEARTBEAT_TIMEOUT_MS = 1000;

    // COMMENT2DO differentiate min and max
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
     * Value used when asked to specify the term of a non-existent log entry.
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

    private Timer timeoutTimer;

    private CheckingCancelTimerTask timeoutTask;

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
     * Map that we can query to see whether we need to reply to a particular
     * client after applying the command of a log entry.
     */
    private HashMap<LogEntry, ClientRequest> outstandingClientRequestsMap;
    /**
     * Single thread manager that will execute the commands for us in order.
     */
    private ExecutorService inOrderCommandApplier;
    
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
        // As part of initialization, spins up three threads to help this
        // server carry out the Raft protocol. One thread is a timeout tasks
        // executor thread (see Timer initialization below), another thread is a
        // command executor thread (see ExecutorService initialization below),
        // and another thread is a connections acceptor thread (see
        // NetworkManager initialization).
        
        synchronized(RaftServer.this) {
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
       
            outstandingClientRequestsMap = new HashMap<LogEntry, ClientRequest>();
            inOrderCommandApplier = Executors.newSingleThreadExecutor();
            timeoutTimer = new Timer();

            this.role = null;
            transitionRole(Role.FOLLOWER);

            networkManager = new NetworkManager(myAddress, this::handleSerializable);
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
            logMessage("Don't know how to handle the serializable object: " + object);
            return;
        }
        RaftMessage message = (RaftMessage) object;
        if (message.term > this.persistentState.currentTerm) {
            updateTerm(message.term);
            transitionRole(Role.FOLLOWER);
        }
        if (message.term < this.persistentState.currentTerm) {
            // In the case of a stale sender term, if the message is a request,
            // then we want to send a reply. For all messages, we want to stop
            // any further processing of the message.
            if (message instanceof AppendEntriesRequest) {
                // The `nextIndex` value specified in the reply should not
                // matter in this case.
                AppendEntriesReply reply = new AppendEntriesReply(myId,
                        this.persistentState.currentTerm, false, 0);
                networkManager.sendSerializable(peerMetadataMap.get(
                        message.serverId).address, reply);
            } else if (message instanceof RequestVoteRequest) {
                RequestVoteReply reply = new RequestVoteReply(myId, 
                        this.persistentState.currentTerm, false);
                networkManager.sendSerializable(peerMetadataMap.get(
                        message.serverId).address, reply);
            }
            return;
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
            logMessage("Don't know how to handle the Raft message: " + message);
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
            InetSocketAddress leaderAddress = leaderId != null
                    ? peerMetadataMap.get(leaderId).address
                    : null;
            ClientReply reply = new ClientReply(request.commandId, leaderAddress, false, null);
            logMessage("Sending " + reply);
            networkManager.sendSerializable(request.clientAddress, reply);
        }
    }

    /**
     * Processes a valid AppendEntries request from a leader and sends a reply.
     * @param request data corresponding to AppendEntries request
     */
    private synchronized void handleAppendEntriesRequest(AppendEntriesRequest request) {
        // If we were a leader, we should have downgraded to follower
        // prior to processing a valid AppendEntries request.
        assert(this.role != Role.LEADER);

        // As a candidate or follower, transition to follower to reset
        // the election timer.
        transitionRole(Role.FOLLOWER);
        this.leaderId = request.serverId;
        
        boolean successfulAppend = false;
        int nextIndex;

        if (canAppend(request.prevLogIndex, request.prevLogTerm)) {
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

    /**
     * Checks whether we can append the sent log entries to our log.
     * Only called when processing a AppendEntries request.
     * @param senderPrevLogIndex prevLogIndex field of the request.
     * @param senderPrevLogterm  prevLogTerm field of the request.
     * @return true iff we can append the sent entries.
     */
    private synchronized boolean canAppend(int senderPrevLogIndex, int senderPrevLogTerm) {
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
     * Processes a valid RequestVote request from a candidate and sends a reply.
     * @param request data corresponding to RequestVotes request
     */
    private synchronized void handleRequestVoteRequest(RequestVoteRequest request) {        
        boolean grantVote =
                checkGrantVote(request.serverId, request.lastLogIndex, request.lastLogTerm);

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
     * Determines whether or not we should grant the vote to the sender.
     * Only called when processing a RequestVotes request.
     * @param senderId server id of the sender
     * @param senderLastLogIndex index of the last log entry of the sender
     * @param senderLastLogTerm term of the last log entry of the sender
     * @return true iff recipient can vote for the sender, and sender's log
     *         is at least as up-to-date as ours.
     */
    private synchronized boolean checkGrantVote(String senderId, int senderLastLogIndex, int senderLastLogTerm) {
        boolean votedForAnotherServer = !(this.persistentState.votedFor == null
                || this.persistentState.votedFor.equals(senderId));

        if (votedForAnotherServer) {
            return false;
        }

        // The rest of this method makes sure that we grant vote iff the
        // candidate log is up-to-date.

        if (this.persistentState.log.size()==0) {
            return true;
        }
        if (senderLastLogIndex == -1) {
            // Here, our sender's log is empty and ours is not
            return false;
        }

        int lastLogIndex = this.persistentState.log.size() - 1;
        int lastLogTerm = this.persistentState.log.get(lastLogIndex).term;

        if (senderLastLogTerm > lastLogTerm) {
            return true;
        }
        if (senderLastLogTerm < lastLogTerm) {
            return false;
        }

        return senderLastLogIndex >= lastLogIndex;
    }

    /**
     * Processes a valid AppendEntries reply.
     * @param AppendEntriesReply reply to AppendEntriesReply request
     */
    private synchronized void handleAppendEntriesReply(AppendEntriesReply reply) {
        if (this.role != Role.LEADER) {
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
     * Processes a valid RequestVote reply.
     * @param RequestVoteReply reply to RequestVote request
     */
    private synchronized void handleRequestVoteReply(RequestVoteReply reply) {
        if (this.role != Role.CANDIDATE) {
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
     * Precondition: timeoutTask timer and persistentState state have been init.
     * @param role new role that the server instance transitions to. 
     */
    private synchronized void transitionRole(Role role) {
        // The defined transitions above allow us to put/contain all the Raft
        // timer logic and accesses in this method.

        if (this.role != role) {
            logMessage("updating role to " + role);
        }
        this.role = role;

        if (timeoutTask != null) {
            timeoutTask.cancel();
        }

        // This restarts (or starts) the election timer when run.
        Runnable restartElectionTimer = () -> {

            // Create a timer task to start a new election.
            timeoutTask = new CheckingCancelTimerTask() {
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

            timeoutTimer.schedule(timeoutTask, ThreadLocalRandom.current().nextInt(
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
            timeoutTask = new CheckingCancelTimerTask() {
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

            timeoutTimer.scheduleAtFixedRate(timeoutTask, 0, HEARTBEAT_TIMEOUT_MS);
            break;
        }
    }

    private boolean logEntryHasIndex(int index) {
        return index >= 0 && index < this.persistentState.log.size();
    }
    
    // A2DO implement this
    private String execute(String command) {
        StringBuffer outputBuffer = new StringBuffer();

        Process p;
        try {
            p = Runtime.getRuntime().exec("bash -c " + command);
            p.waitFor();
            BufferedReader reader = 
                new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";           
            while ((line = reader.readLine())!= null) {
                outputBuffer.append(line + "\n");
            }
        } catch (Exception e) {
            // If executing a command lead to exception, we should note this and
            // continue
            e.printStackTrace();
        }

        return outputBuffer.toString();
    }

    // Wrapper method around setting of commitIndex
    private synchronized void updateCommitIndex(int newCommitIndex) {
        if (newCommitIndex <= this.commitIndex) {
            return;
        }
        for (int i=commitIndex+1; i <= newCommitIndex; i++) {
            LogEntry logEntry = this.persistentState.log.get(i);
            inOrderCommandApplier.submit(() -> {
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
        // A2DO: ensure that list of server addresses passed in is non-empty
        //       not sure if we have to implement any extra checks to achieve this

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
