import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;

/* 
 * The Server class implements features as per the RAFT consensus protocol.
 *  Raft defines a distributed consensus algorithm for maintaining a shared
 *  state machine. Each Raft node maintains a complete copy of the state
 *  machine. Cluster nodes elect a leader who collects and distributes updates
 *  and provides for consistent reads. As long as as a node is part of a
 *  majority, the state machine is fully operational.
 *
 * It has the following features:
 *   1) leader election (Proj1) and log replication (Proj2)
 *   2) Dynamic membership change
 *      a) Upon initialization, Servers switch to FOLLOWER state
 *      b) Elections occur automatically once election timeout is hit
 *      c) Upon successful leader election, client communications are directed
 *         to the leader.
 *   3) Persistent data-storage is implemented so that accidental server
 *      failures can be restored efficiently
 *   4) Cluster will be fully functional as long as a majority of the servers
 *      in the cluster are running and respond promptly to requests.
 *
 *
 *
 *
 */
public class Server implements Runnable {
    
    private static final int HEARTBEAT_INTERVAL = 1000;
    private static final int MIN_ELECTION_TIMEOUT = 3000;
    private static final int MAX_ELECTION_TIMEOUT = 5000;

    private String myId; // Unique identification (Id) per server
    private String leaderId; // current leader's Id
    private InetSocketAddress myAddress; // Unique address per server
    // * A HashMap that maps each server (excluding myself) to its
    // *   1) Id
    // *   1) Address
    // *   1) nextIndex (only applicable for leader)
    // *   1) matchIndex (only applicable for leader)
    private HashMap<String, ServerMetadata> otherServersMetadataMap;
    // private ROLE role; // my role, one of FOLLOWER, CANDIDATE, or LEADER

    private Role role;
    // PersistentStorage
    private PersistentState persistentState;
    
    private Timer timer;
    private ListenerThread listenerThread;
    private ExecutorService networkService;

    // Volatile State on all servers
    // * index of highest log entry known to be committed (initialized to 0,
    //   increases monotonically)
    private int commitIndex;
    // * index of highest log entry applied to state machine (initialized to 0,
    //   increases monotonically)
    private int lastApplied;

    // Tracing and debugging logger
    // see: https://logging.apache.org/log4j/2.0/manual/api.html
    private static final Logger myLogger = LogManager.getLogger(Server.class);

    // Initialize a server with its Id and a HashMap between servers and their
    // meta-data.
    // Upon initialization, we do 4 things:
    //   1) store our own information in our class, and turn other servers'
    //      meta-data into a new HashMap and store a reference to it.
    //   2) change our role to FOLLOWER.
    //   3) create a socket for all incoming communications
    //   4) initialize and load (Proj2) variables
    public Server(String id, HashMap<String, InetSocketAddress> serverAddressesMap) {
        myId = id;
        leaderId = null;
        otherServersMetadataMap = new HashMap<String, ServerMetadata>();
        for (HashMap.Entry<String, InetSocketAddress> entry : serverAddressesMap.entrySet()) {
            String elemId = entry.getKey();
            InetSocketAddress elemAddress = entry.getValue();  
            
            if (elemId.equals(this.myId)) {
                myAddress = elemAddress;
            } else {
                this.otherServersMetadataMap.put(elemId, new ServerMetadata(elemId, elemAddress));
            }
        }
        
        timer = new Timer();

        // Start a thread to accept connections and add them to read selector
        try {
            updateRole(new Follower());
            persistentState = new PersistentState(myId);
            listenerThread = new ListenerThread(myAddress);
        } catch (IOException e) {
            if (e.getMessage().equals("Address already in use")) {
                logMessage("address " + myAddress + " already in use");
            } else {
                logMessage(e.getMessage());
            }
            System.exit(1);
        }
        listenerThread.start();
        networkService = Executors.newFixedThreadPool(this.otherServersMetadataMap.size());

        this.commitIndex = -1;
        this.lastApplied = -1;

        myLogger.debug(myId + " :: Configuration File Defined To Be :: "+System.getProperty("log4j.configurationFile"));
        logMessage("successfully booted");
    }

    // All servers shall listen and respond to incoming requests. Below is a
    // summary of how each role should behave:
    //**************************************************************************
    //* Follower does the following 2 tasks:                                   *
    //*   1) Respond to requests from candidates and leaders                   *
    //*   2) If election timeout elapses without receiving a valid             *
    //*      AppendEntries request from current leader or granting vote to     *
    //*      candidate: convert to candidate                                   *
    //* Candidate does the following 4 tasks:                                  *
    //*   1) On conversion to candidate, start election:                       *
    //*      a) Increment currentTerm                                          *
    //*      b) Vote for self                                                  *
    //*      c) Reset election timer                                           *
    //*      d) Send RequestVote requests to all other servers                 *
    //*   2) If votes received from majority of servers: become leader         *
    //*   3) If AppendEntries request received from new leader: convert to     *
    //*      follower                                                          *
    //*   4) If election timeout elapses: start new election                   *
    //* Leader does the following 4 tasks:                                     *
    //*   1) Upon election: send initial empty AppendEntries requests          *
    //*      (heartbeat) to each server; repeat during idle periods to prevent *
    //*      election timeouts (§5.2)                                          *
    //*   2) If command received from client: append entry to local log,       *
    //*      respond after entry applied to state machine (§5.3)               *
    //*   3) If last log index ≥ nextIndex for a follower: send AppendEntries  *
    //*      requests with log entries starting at nextIndex                   *
    //*      a) If successful: update nextIndex and matchIndex for follower    *
    //*         (§5.3)                                                         *
    //*      b) If AppendEntries fails because of log inconsistency: decrement *
    //*         nextIndex and retry (§5.3)                                     *
    //*   4) If there exists an N such that N > commitIndex, a majority of     *
    //*      matchIndex[i] ≥ N, and log[N].term == currentTerm:                *
    //*      set commitIndex = N (§5.3, §5.4).                                 *
    //**************************************************************************
    public void run() {
        try {
            while (true) {
                if (timer.timeIsUp()) {
                    this.role.performTimeoutAction();
                    this.role.resetTimeout();
                }
                if (listenerThread.readSelector.selectNow() == 0) {
                    continue;
                } else {
                    Set<SelectionKey> selectedKeys = listenerThread.readSelector.selectedKeys();

                    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                    while(keyIterator.hasNext()) {
                        logMessage("about to read");
                        SelectionKey key = keyIterator.next();
                        SocketChannel channel = (SocketChannel) key.channel();
                        Message message = (Message) NetworkUtils.receiveMessage(channel, true);
                        logMessage("received " + message);
                        boolean myTermStale = message.term > this.persistentState.currentTerm;
                        boolean senderTermStale = message.term < this.persistentState.currentTerm;
                        if (myTermStale) {
                            this.persistentState.currentTerm = message.term;
                            this.persistentState.votedFor = null;
                            this.updateRole(this.new Follower());
                        }
                        if (message instanceof AppendEntriesRequest) {
                            if (!senderTermStale) {
                                this.role.processValidityOfAppendEntriesRequest();
                            }
                            boolean success = processEntries((AppendEntriesRequest) message, senderTermStale);
                            AppendEntriesReply reply = new AppendEntriesReply(myId, this.persistentState.currentTerm, success);
                            saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId), reply);
                        } else if (message instanceof RequestVoteRequest) {
                            boolean grantingVote = grantVote((RequestVoteRequest) message, senderTermStale);
                            if (grantingVote) {
                                assert(this.role instanceof Follower);
                                this.role.resetTimeout();
                            }
                            RequestVoteReply reply = new RequestVoteReply(myId, this.persistentState.currentTerm, grantingVote);
                            saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId), reply);
                        } else if (message instanceof AppendEntriesReply) {
                            if (this.role instanceof Leader) {
                                ((Leader) this.role).processAppendEntriesReply((AppendEntriesReply) message);
                            }
                        } else if (message instanceof RequestVoteReply) {
                            if (this.role instanceof Candidate) {
                                ((Candidate) this.role).processRequestVoteReply((RequestVoteReply) message);
                            }
                        } else {
                            assert(false);
                        }
                        keyIterator.remove();
                    }
                }
            }
        } catch (IOException e) {
            logMessage(e.getMessage());
        }
    }

    private void updateRole(Role role) throws IOException {
        this.role = role;

        if (this.role instanceof Candidate) {
            this.role.performTimeoutAction();
        } else if (this.role instanceof Leader) {
            // send initial empty AppendEntriesRequests upon promotion to leader
            logMessage("broadcasting initial heartbeat messages");

            // Proj2: see if initial heartbeat messages need to be
            // tailored to the target servers in any way.
            // Proj2: consider merging code for sending initial
            // heartbeat messages with code for sending subsequent
            // rounds of heartbeat messages.
            Message message = new AppendEntriesRequest(myId, persistentState.currentTerm, -1, -1, null, commitIndex);
            for (ServerMetadata meta : otherServersMetadataMap.values()) {
                saveStateAndSendMessage(meta, message);
            }
        }
        
        this.role.resetTimeout();
    }

    private void saveStateAndSendMessage(ServerMetadata recipientMeta, Message message) throws IOException {
        this.persistentState.save();
        networkService.submit(() -> {
            try {
                NetworkUtils.sendMessage(recipientMeta.address, message);
            } catch (IOException e) {
                if (e.getMessage().equals("Connection refused")) {
                    logMessage("connection to " + recipientMeta.id + " refused");
                } else {
                    logMessage(e.getMessage());
                }
            }
        });
    }

    // Helper logger that logs to a log4j2 logger instance
    private void logMessage(Object message) {
        myLogger.info(myId + " :: term=" + this.persistentState.currentTerm + " :: " + role + " :: " + message);
    }

    // Processes an AppendEntries request from leader ($5.3 Log replication)
    // If conflicts exist, we delete our record up to the conflicting one
    // Otherwise add new entries to our log
    // Update commitIndex when necessary
    // Returns whether follower contained entry matching prevLogIndex and prevLogTerm
    private boolean processEntries(AppendEntriesRequest message, boolean senderTermStale) {
        if (senderTermStale) {
            return false;
        } else {
            this.leaderId = message.serverId;
            // Proj2: test this code
            if (message.prevLogIndex >= 0 && message.prevLogIndex < this.persistentState.log.size()) {
                // Proj2: check prevLogTerm
                if (this.persistentState.log.get(message.prevLogIndex).term != message.prevLogTerm) {
                    this.persistentState.log = this.persistentState.log.subList(0, message.prevLogIndex);
                    return false;
                } else {
                    // Proj2: is this okay to add log entry unconditionally?
                    // Otherwise, check whether this if cond. is necessary
                    if (!this.persistentState.log.contains(message.entry)) {
                        this.persistentState.log.add(message.entry);
                    }
                    // Proj2: Consider implementing Figure 2, All servers, bullet point 1/2 here
                    if (message.leaderCommit > this.commitIndex) {
                        this.commitIndex = Math.min(message.leaderCommit, this.persistentState.log.size() - 1);
                    }
                    return true;
                }
            } else {
                return false;
            }
        }
    }
    
    // Checks if we grant the sender our vote ($5.2 Leader election)
    private boolean grantVote(RequestVoteRequest message, boolean senderTermStale) {
        if (senderTermStale) {
            return false;
        } else {
            if (this.persistentState.votedFor == null || this.persistentState.votedFor.equals(message.serverId)) {
                int lastLogIndex = this.persistentState.log.size() - 1;
                int lastLogTerm = lastLogIndex < 0 ? -1 : this.persistentState.log.get(lastLogIndex).term;
                // Proj2: make sure that this logic is correct for checking that a candidate's
                // log is at least as up-to-date as ours. Test this logic afterwards
                if (message.lastLogIndex >= lastLogIndex && message.lastLogTerm >= lastLogTerm) {
                    assert(!senderTermStale);
                    this.persistentState.votedFor = message.serverId;
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
    
    // The Role class and its nested subclasses are used to help
    // facilitate dynamic dispatch of methods based on the server's
    // current role at any given time.
    interface Role {
        void resetTimeout();
        void performTimeoutAction() throws IOException;
        void processValidityOfAppendEntriesRequest() throws IOException;
    }
    class Follower implements Role {
        public void resetTimeout() {
            timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
        }
        public void performTimeoutAction() throws IOException {
            updateRole(new Candidate());
        }
        public void processValidityOfAppendEntriesRequest() throws IOException {
            this.resetTimeout();
        }
        @Override
        public String toString() {
            return "Follower";
        }
    }
    class Candidate implements Role {
        private int votesReceived;

        public void resetTimeout() {
            timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
        }
        
        // Starts an election.
        public void performTimeoutAction() throws IOException {
            persistentState.currentTerm += 1;
            this.votesReceived = 1;
            persistentState.votedFor = myId;
            int lastLogIndex = persistentState.log.size()-1;
            // lastLogTerm = -1 means there are no log entries
            int lastLogTerm = lastLogIndex < 0 ?
                -1 : persistentState.log.get(lastLogIndex).term;

            logMessage("broadcasting RequestVote requests");
            Message message = new RequestVoteRequest(myId, persistentState.currentTerm, lastLogIndex, lastLogTerm);

            for (ServerMetadata meta : otherServersMetadataMap.values()) {
                saveStateAndSendMessage(meta, message);
            }
        }
        public void processValidityOfAppendEntriesRequest() throws IOException {
            updateRole(new Follower());
        }

        public void processRequestVoteReply(RequestVoteReply reply) throws IOException {
            if (reply.voteGranted) {
                votesReceived += 1;
            }
            if (votesReceived > (otherServersMetadataMap.size()+1)/2) {
                updateRole(new Leader());
            }
        }
        @Override
        public String toString() {
            return "Candidate";
        }
    }
    class Leader implements Role {        
        public Leader() {
            // initialize volatile state on leaders
            for (ServerMetadata meta : otherServersMetadataMap.values()) {
                // Subtracting 1 makes it apparent that we want to send the entry
                // corresponding to the next available index
                meta.nextIndex = (persistentState.log.size() - 1) + 1;
                meta.matchIndex = -1;
            }
        }
        public void resetTimeout() {
            timer.reset(HEARTBEAT_INTERVAL);
        }

        public void performTimeoutAction() throws IOException {
            // send regular heartbeat messages with zero or more log entries after a heartbeat interval has passed
            logMessage("broadcasting heartbeat messages");

            for (ServerMetadata meta : otherServersMetadataMap.values()) {
                // Proj2: send server-tailored messages to each server
                // Proj2: add suitable log entry (if needed) as argument into AppendEntriesRequest
                Message message = new AppendEntriesRequest(myId, persistentState.currentTerm, -1, -1, null, commitIndex);
                saveStateAndSendMessage(meta, message);
            }
        }

        public void processValidityOfAppendEntriesRequest() throws IOException {
            // As a leader, server should always downgrade to follower
            // prior to processing a valid request.
            assert(false);
        }
   
        public void processAppendEntriesReply(AppendEntriesReply reply) {
            // Proj2: write logic to handle AppendEntries message (as leader)
            ServerMetadata meta = otherServersMetadataMap.get(reply.serverId);
            if (reply.success) {
                meta.matchIndex = meta.nextIndex;
                meta.nextIndex += 1;
                if (testMajorityN(meta.matchIndex)) {
                    commitIndex = meta.matchIndex;
                }
            } else {
                if (meta.nextIndex > 0) {
                    meta.nextIndex -= 1;
                }
            }
        }
        
        // Called by the leader to determine if we should update the commitIndex
        // ($5.3, $5.4)
        private boolean testMajorityN(int candidateN) {
            if (candidateN<=commitIndex) {
                return false;
            }
            // count is the # of servers with at least candidateN log entries
            // We include the leader in the count because its log index is >= candidateN
            int count = 1;
            for (ServerMetadata meta : otherServersMetadataMap.values()) {
                if (meta.matchIndex >= candidateN) {
                    count += 1;
                }
            }
            if (count <= otherServersMetadataMap.size() / 2) {
                return false;
            }
            if (persistentState.log.get(candidateN).term != persistentState.currentTerm) {
                return false;
            }
            return true;
        }
        @Override
        public String toString() {
            return "Leader";
        }
    }

    public static void main(String[] args) {
        if (args.length!=2) {
            System.out.println("Please suppply exactly two arguments");
            System.out.println("Usage: <myPortIndex> <port0>,<port1>,...");
            System.out.println("Note: List of ports is 0-indexed");
            System.exit(1);
        }

        String[] allPorts = args[1].split(",");
        int myPortIndex = Integer.parseInt(args[0]); 
        
        if (myPortIndex < 0 || myPortIndex >= allPorts.length) {
            System.out.println("Please supply a valid index for first argument");
            System.out.println("Usage: <myPortIndex> <port0>,<port1>,...");
            System.out.println("Note: List of ports is 0-indexed");
            System.exit(1);
        }

        System.setProperty("log4j.configurationFile", "./src/log4j2.xml");
        HashMap<String, InetSocketAddress> serverAddressesMap = new HashMap<String, InetSocketAddress>();
        for (int i=0; i<allPorts.length; i++) {
            serverAddressesMap.put("Server" + i, new InetSocketAddress("localhost", Integer.parseInt(allPorts[i])));   
        }

        Server myServer = new Server("Server" + myPortIndex, serverAddressesMap);
        myServer.run();
    }
}
