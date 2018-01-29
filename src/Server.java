import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import messages.AppendEntriesReply;
import messages.AppendEntriesRequest;
import messages.Message;
import messages.RequestVoteReply;
import messages.RequestVoteRequest;
import misc.ServerMetadata;
import singletons.ListenerThread;
import singletons.PersistentState;
import singletons.Timer;
import utils.NetworkUtils;


/**
 * Each server in the Raft cluster should create and maintain its own
 * Server instance. Each instance runs the Raft protocol.
 */
public class Server implements Runnable {

    private static final int HEARTBEAT_INTERVAL = 1000;

    // The election timeout is a random variable with the distribution
    // Uniform(min. election timeout, max election timeout).
    private static final int MIN_ELECTION_TIMEOUT = 3000;
    private static final int MAX_ELECTION_TIMEOUT = 5000;

    private String myId; // unique id
    @SuppressWarnings("unused")
    private String leaderId; // current leader's id
    private InetSocketAddress myAddress; // unique address

    // A map that maps server id to a server metadata object. This map
    // enables us to read properties and keep track of state
    // corresponding to other servers in the Raft cluster.
    private HashMap<String, ServerMetadata> otherServersMetadataMap;

    /*
     * Singletons:
     * * Timer instance is used to manage time for election and
     *   heartbeat timeouts.
     * 
     * * PersistentState instance is used to read and write server
     *   state that should be persisted onto disk.
     * 
     * * ListenerThread instance spins up a thread that starts+runs
     *   a listener channel for other servers to send requests to. We
     *   can poll for read events by accessing what this instance
     *   exposes.
     * 
     * * ExecutorService instance manages a thread pool for us, which
     *   we use to send concurrent requests to other servers.
     */
    private Timer timer;
    private PersistentState persistentState;
    private ListenerThread listenerThread;
    private ExecutorService threadPoolService;

    private Role role; // One of follower, candidate, leader

    /*
     * Log-specific volatile state:
     * * commitIndex - index of highest log entry known to be committed (initialized to 0, increases monotonically)
     * * lastApplied - index of highest log entry applied to state machine (initialized to 0, increases monotonically)
     */
    private int commitIndex;
    @SuppressWarnings("unused")
    private int lastApplied;

    // Tracing and debugging logger
    // see: https://logging.apache.org/log4j/2.x/manual/api.html
    private static final Logger myLogger = LogManager.getLogger(Server.class);


    /**
     * @param id server id
     * @param serverAddressesMap map that maps server id to address.
     *        Contains entries for all servers in Raft cluster.
     */
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

        try {
            persistentState = new PersistentState(myId);
            listenerThread = new ListenerThread(myAddress);
            transitionRole(new Follower());
        } catch (IOException e) {
            if (e.getMessage().equals("Address already in use")) {
                logMessage("address " + myAddress + " already in use");
            } else {
                logMessage(e.getMessage());
            }
            System.exit(1);
        }
        listenerThread.start();
        threadPoolService = Executors.newFixedThreadPool(this.otherServersMetadataMap.size());

        this.commitIndex = -1;
        this.lastApplied = -1;

        myLogger.debug(myId + " :: Configuration File Defined To Be :: "+System.getProperty("log4j.configurationFile"));
        logMessage("successfully booted");
    }

    /* 
     * Starts an event loop on the main thread where we:
     * 1) Perform timeout actions depending on remaining time and role.
     * 2) Process incoming requests as they arrive.
     */
    public void run() {
        Selector readSelector = listenerThread.getReadSelector();
        try {
            while (true) {
                if (timer.timeIsUp()) {
                    this.role.performTimeoutAction();
                    this.role.resetTimeout();
                }

                readSelector.selectNow();
                Set<SelectionKey> selectedKeys = readSelector.selectedKeys();
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
                        this.transitionRole(new Follower());
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
        } catch (IOException e) {
            logMessage(e.getMessage());
        }
    }

    /**
     * Changes the role of the server instance.
     * Depending on new role, there may be some post-transition
     * behavior that will need to be run.
     * @param role new role that the server instance transitions to. 
     * @throws IOException
     */
    private void transitionRole(Role role) throws IOException {
        this.role = role;

        if (this.role instanceof Candidate) {
            // Start an election upon transition to candidate.
            this.role.performTimeoutAction();
        } else if (this.role instanceof Leader) {
            // send initial empty AppendEntries requests upon
            // transition to leader.
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

    /**
     * Wrapper around NetworkUtils.sendMessage that enforces the
     * invariant that we save persistent state to disk before sending
     * any network requests to recipients.
     * @param recipientMeta recipient metadata
     * @param message message that we want to send to recipient
     * @throws IOException
     */
    private void saveStateAndSendMessage(ServerMetadata recipientMeta, Message message) throws IOException {
        this.persistentState.save();
        threadPoolService.submit(() -> {
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

    /**
     * Wrapper around info logging that makes sure our logs are
     * annotated with server-specific properties of interest.
     * @param message any message that we wish to log
     */
    private void logMessage(Object message) {
        myLogger.info(myId + " :: term=" + this.persistentState.currentTerm + " :: " + role + " :: " + message);
    }

    /**
     * Processes an AppendEntries request from a leader.
     * @param message data corresponding to AppendEntries request
     * @param senderTermStale tells you whether sender term is stale
     * @return true iff sender term is not stale and recipient log
     *         contained entry matching prevLogIndex and prevLogTerm
     */
    private boolean processEntries(AppendEntriesRequest message, boolean senderTermStale) {
        if (senderTermStale) {
            return false;
        }
        this.leaderId = message.serverId;

        boolean logIndexIsValid = message.prevLogIndex >= 0 && message.prevLogIndex < this.persistentState.log.size();
        if (!logIndexIsValid) {
            return false;
        }

        // Proj2: test this code relating to the log
        // Proj2: check prevLogTerm
        boolean logTermsMatch = this.persistentState.log.get(message.prevLogIndex).term == message.prevLogTerm;
        if (!logTermsMatch) {
            this.persistentState.log = this.persistentState.log.subList(0, message.prevLogIndex);
            return false;
        }

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

    /**
     * Check whether we should grant the sender our vote.
     * @param message data corresponding to RequestVotes request
     * @param senderTermStale tells you whether sender term is stale
     * @return true iff sender term is not stale, recipient can vote
     *         for the candidate, and candidate's log is at least as
     *         up-to-date as ours.
     */
    private boolean grantVote(RequestVoteRequest message, boolean senderTermStale) {
        if (senderTermStale) {
            return false;
        }

        boolean canVote = this.persistentState.votedFor == null || this.persistentState.votedFor.equals(message.serverId);

        if (!canVote) {
            return false;
        }

        int lastLogIndex = this.persistentState.log.size() - 1;
        int lastLogTerm = lastLogIndex < 0 ? -1 : this.persistentState.log.get(lastLogIndex).term;
        // Proj2: make sure that this logic is correct for checking that a candidate's
        // log is at least as up-to-date as ours. Test this logic afterwards
        boolean candidateLogIsUpdated = message.lastLogIndex >= lastLogIndex && message.lastLogTerm >= lastLogTerm;
        if (!candidateLogIsUpdated) {
            return false;
        }

        this.persistentState.votedFor = message.serverId;
        return true;
    }
    
    /**
     * The nested Role interface and the subclasses that implement it
     * together allow for dynamic dispatch of methods based on the
     * server's current role at any given time.
     */
    interface Role {
        /**
         * Resets timer interval to value depending on server's role.
         */
        void resetTimeout();

        /**
         * Performs timeout action after timer's timeout occurred.
         * @throws IOException
         */
        void performTimeoutAction() throws IOException;

        /**
         * Called if AppendEntriesRequest is valid, so that the server
         * can react accordingly.
         * @throws IOException
         */
        void processValidityOfAppendEntriesRequest() throws IOException;
    }

    // Contains and groups follower-specific behavior.
    class Follower implements Role {
        public void resetTimeout() {
            timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
        }
        public void performTimeoutAction() throws IOException {
            transitionRole(new Candidate());
        }
        public void processValidityOfAppendEntriesRequest() throws IOException {
            this.resetTimeout();
        }
        @Override
        public String toString() {
            return "Follower";
        }
    }

    // Contains and groups candidate-specific behavior.
    class Candidate implements Role {
        private int votesReceived;

        public void resetTimeout() {
            timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
        }

        // Start a new election.
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
            transitionRole(new Follower());
        }

        /**
         * Candidate-specific method to process RequestVoteReply
         * message.
         * @param RequestVoteReply reply to RequestVote request
         * @throws IOException
         */
        public void processRequestVoteReply(RequestVoteReply reply) throws IOException {
            if (reply.voteGranted) {
                votesReceived += 1;
            }
            if (votesReceived > (otherServersMetadataMap.size()+1)/2) {
                transitionRole(new Leader());
            }
        }
        @Override
        public String toString() {
            return "Candidate";
        }
    }

    // Contains and groups leader-specific behavior.
    class Leader implements Role {        
        public Leader() {
            // initialize volatile state specific to leaders
            for (ServerMetadata meta : otherServersMetadataMap.values()) {
                // Subtracting 1 makes it apparent that we want to send
                // the entry corresponding to the next available index
                meta.nextIndex = (persistentState.log.size() - 1) + 1;
                meta.matchIndex = -1;
            }
        }
        public void resetTimeout() {
            timer.reset(HEARTBEAT_INTERVAL);
        }

        // Send out a round of heartbeat messages to all servers.
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

        /**
         * Leader-specific method to process AppendEntriesReply
         * message.
         * @param AppendEntriesReply reply to AppendEntriesReply request
         * @throws IOException
         */
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
        /**
         * Leader-specific method that checks whether we should commit
         * any entries in the log.
         * @param candidateN the proposed log index for which we are
         *                   checking whether we should commit up to
         * @return true iff we should update commitIndex to the
         *         proposed log index
         */
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

    /**
     * Creates+runs a server instance that follows the Raft protocol.
     * @param args args[0] is comma-delimited list of ports (0-indexed).
     *             args[1] is port index to determine the port for which
     *               the server will start a listener channel on.
     */
    public static void main(String[] args) {
        if (args.length!=2) {
            System.out.println("Please suppply exactly two arguments");
            System.out.println("Usage: <port0>,<port1>,... <myPortIndex>");
            System.out.println("Note: List of ports is 0-indexed");
            System.exit(1);
        }

        String[] allPorts = args[0].split(",");
        int myPortIndex = Integer.parseInt(args[1]); 

        if (myPortIndex < 0 || myPortIndex >= allPorts.length) {
            System.out.println("Please supply a valid index for first argument");
            System.out.println("Usage: <port0>,<port1>,... <myPortIndex>");
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
