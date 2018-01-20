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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;

/* 
 * The Server class implements features as per the RAFT concensus protocol.
 *  Raft defines a distributed consensus algorithm for maintaining a shared
 *  state machine. Each Raft node maintains a complete copy of the state
 *  machine. Cluster nodes elect a leader who collects and distributes updates
 *  and provides for consistent reads. As long as as a node is part of a
 *  majority, the state machine is fully operational.
 *
 * It has the following features:
 *   1) leader election (project 1) and log replication (TODO project 2)
 *   2) Dynamic membership change
 *      a) Upon initialization, Servers switch to FOLLOWER state
 *      b) Elections occur automatically once election timeout is hit
 *      c) Upon successful leader election, client communications are directed
 *         to the leader.
 *   3) Persistent data-storage is implemented so that accidental server
 *      failures can be restored efficiently
 *   4) Cluster will be fully functional as long as a majority of the servers
 *      in the cluster are running and respond promptly to RPC requests.
 *
 *
 *
 *
 */
public class Server implements Runnable {
    // Magical constants
    public static final int HEARTBEAT_INTERVAL = 400;
    public static final int MIN_ELECTION_TIMEOUT = 1500;
    public static final int MAX_ELECTION_TIMEOUT = 3000;
    
    public static enum ROLE { FOLLOWER, CANDIDATE, LEADER; }

    // TODO Add detailed comments for all instance variables
    private String myId; // Unique identification (Id) per server
    private String leaderId; // current leader's Id
    private InetSocketAddress myAddress; // Unique address per server
    // * A HashMap that maps each server (excluding myself) to its
    // *   1) Id
    // *   1) Address
    // *   1) nextIndex (only applicable for leader)
    // *   1) matchIndex (only applicable for leader)
    private HashMap<String, ServerMetadata> otherServersMetadataMap;
    private ROLE role; // my role, one of FOLLOWER, CANDIDATE, or LEADER
    
    private ServerSocketChannel myListenerChannel; // singleton
    private Selector selector;

    // Persistent State
    // * Latest term server has seen (initialized to 0 on first boot, increases
    //   monotonically)
    private int currentTerm;
    // * candidateId that received vote in current term (or null if none)
    private String votedFor;
    // * log entries; each entry contains command for state machine, and term
    //   when entry was received by leader (first index is 0)
    private List<LogEntry> log;

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
    //   4) initialize and load (TODO project 2) variables
    public Server(String serverId, HashMap<String, InetSocketAddress> serverAddressesMap) {
        this.leaderId = null;
        this.myId = serverId;
        this.otherServersMetadataMap = new HashMap<String, ServerMetadata>();
        for (HashMap.Entry<String, InetSocketAddress> entry : serverAddressesMap.entrySet()) {
            String elemId = entry.getKey();
            InetSocketAddress elemAddress = entry.getValue();  

            if (elemId == this.myId) {
                myAddress = elemAddress;
            } else {
                this.otherServersMetadataMap.put(elemId, new ServerMetadata(elemId, elemAddress));
            }
        }
        role = ROLE.FOLLOWER;

        // Create a server to listen and respond to requests
        try {
            myListenerChannel = ServerSocketChannel.open();
            myListenerChannel.configureBlocking(false);
            myListenerChannel.socket().bind(myAddress);   
        } catch (IOException e) {
            e.printStackTrace();
        }

        // TODO Conditionally load in persistent state if present
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = new ArrayList<LogEntry>();

        this.commitIndex = -1;
        this.lastApplied = -1;

        // Debug
        startTime = Date.from(Instant.now());
        myLogger.info(myId + " :: Configuration File Defined To Be :: "+System.getProperty("log4j.configurationFile"));
    }

    // Startup the server
    // Essentially a while loop that calls different methods based on our role
    public void run() {
        try {
            while(true) {
                selector = Selector.open();
                myListenerChannel.register(selector, SelectionKey.OP_ACCEPT);
                // 3 While loops for different roles
                switch (role) {
                    case FOLLOWER:
                        followerListenAndRespond();
                        break;
                    case CANDIDATE: 
                        candidateRunForElection();
                        break;
                    case LEADER:
                        leaderSendHeartbeatsAndListen();
                        break;
                    default:
                        assert(false);
                }
                selector.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Helper function to send message to all other servers (excluding me)
    private void broadcast(Message message) {

        logMessage("broadcasting");

        for (ServerMetadata meta : otherServersMetadataMap.values()) {
            String[] entries = {};
            try {
                RPCUtils.sendMessage(meta.address, message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void acceptConnection() throws IOException {
        logMessage("about to accept");
        SocketChannel clientChannel = myListenerChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(selector, SelectionKey.OP_READ);
    }

    // TODO for final submission, replace this with an acceptable logging mechanism (e.g., log4j2)
    private void logMessage(Object message) {
        myLogger.info(myId + " :: " + role + " :: " + message);
    }

    // Compares the sender's term against ours
    // Does a term update when necessary
    private void processMessageTerm(Message message) {
        if (message.term > this.currentTerm) {
            this.currentTerm = message.term;
            this.votedFor = null;
        }
    }

    // Checks if we grant the sender our vote
    private boolean grantVote(RequestVoteRequest message, boolean senderTermStale) {
        if (senderTermStale) {
            return false;
        } else {
            if (this.votedFor == null || this.votedFor == message.serverId) {
                int lastLogIndex = this.log.size() - 1;
                int lastLogTerm = lastLogIndex < 0 ? -1 : this.log.get(lastLogIndex).term;
                // Proj2: make sure that this logic is correct for checking that a candidate's
                // log is at least as up-to-date as ours. Test this logic afterwards
                if (message.lastLogIndex >= lastLogIndex && message.lastLogTerm >= lastLogTerm) {
                    assert(!senderTermStale);
                    this.votedFor = message.serverId;
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    // If conflicts exist, we delete our record up to the conflicting one
    // Otherwise add new entries to our log
    // Update commitIndex when necessary
    // Returns whether follower contained entry matching prevLogIndex and prevLogTerm
    private boolean processAppendEntriesRequest(AppendEntriesRequest message, boolean senderTermStale) {
        if (senderTermStale) {
            return false;
        } else {
            this.leaderId = message.serverId;
            // Proj2: test this code
            if (message.prevLogIndex >= 0 && message.prevLogIndex < this.log.size()) {
                // Proj2: check prevLogTerm
                if (this.log.get(message.prevLogIndex).term != message.prevLogTerm) {
                    this.log = this.log.subList(0, message.prevLogIndex);
                    return false;
                } else {
                    // Proj2: is this okay to add log entry unconditionally?
                    // Otherwise, check whether this if cond. is necessary
                    if (!this.log.contains(message.entry)) {
                        this.log.add(message.entry);
                    }
                    // Proj2: Consider implementing Figure 2, All servers, bullet point 1/2 here
                    if (message.leaderCommit > this.commitIndex) {
                        this.commitIndex = Math.min(message.leaderCommit, this.log.size() - 1);
                    }
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    private boolean testMajorityN(int candidateN) {
        if (candidateN<=this.commitIndex) {
            return false;
        }
        // TODO ask ousterhout about a better name for count
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
        if (this.log.get(candidateN).term != currentTerm) {
            return false;
        }
        return true;
    }

    private void followerListenAndRespond() throws IOException {
        int readyChannels = 0;
        long electionTimeout = 0;
        Date beforeSelectTime = null;
        Date currTime = null;
        boolean resetTimeout = true;
        while (role==Server.ROLE.FOLLOWER) {
            if(resetTimeout) {
                beforeSelectTime = Date.from(Instant.now());
                electionTimeout = ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1);
                resetTimeout = false;
            }
            logMessage("about to enter timeout");
            readyChannels = selector.select(electionTimeout);
            if (readyChannels == 0) {
                role = Server.ROLE.CANDIDATE;
                break;
            } else {
                logMessage("about to iterate over keys");
                Set<SelectionKey> selectedKeys = selector.selectedKeys();

                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while(keyIterator.hasNext()) {

                    SelectionKey key = keyIterator.next();

                    if(key.isAcceptable()) {
                        acceptConnection();
                    } else if (key.isConnectable()) {
                        logMessage("about to connect");
                        // pass
                    } else if (key.isReadable()) {
                        logMessage("about to read");
                        SocketChannel channel = (SocketChannel) key.channel();

                        Message message = (Message) RPCUtils.receiveMessage(channel);
                        boolean senderTermStale = message.term < this.currentTerm;
                        processMessageTerm(message);
                        if (message instanceof AppendEntriesRequest) {
                            logMessage(message);
                            AppendEntriesReply reply = new AppendEntriesReply(myId, this.currentTerm, processAppendEntriesRequest((AppendEntriesRequest) message, senderTermStale));
                            resetTimeout = !senderTermStale;
                            RPCUtils.sendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                        } else if (message instanceof RequestVoteRequest) {
                            logMessage(message);
                            RequestVoteReply reply = null;
                            boolean grantingVote = grantVote((RequestVoteRequest) message, senderTermStale);

                            reply = new RequestVoteReply(myId, this.currentTerm, grantingVote);
                            resetTimeout = grantingVote;
                            RPCUtils.sendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                        } else {
                            assert(false);
                        }
                    } else if (key.isWritable()) {
                        logMessage("about to write");
                    }

                    keyIterator.remove();
                }
                if (!resetTimeout) {
                    currTime = Date.from(Instant.now());
                    electionTimeout -= currTime.getTime() - beforeSelectTime.getTime();
                    beforeSelectTime = currTime;
                }
            }
        }
    }

    private void candidateRunForElection() throws IOException {
        int readyChannels = 0;
        int votesReceived = 0;
        long electionTimeout = 0;
        Date beforeSelectTime = null;
        Date currTime = null;
        boolean resetTimeout = true;
        while (role==Server.ROLE.CANDIDATE) {
            if(resetTimeout) {
                beforeSelectTime = Date.from(Instant.now());
                electionTimeout = ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1);
                // Start Election
                currentTerm += 1;
                votesReceived = 1;
                this.votedFor = myId;
                resetTimeout = false;
                int lastLogIndex = this.log.size()-1;
                // lastLogTerm = -1 means there are no log entries
                int lastLogTerm = lastLogIndex < 0 ?
                                  -1 : this.log.get(lastLogIndex).term;
                broadcast(new RequestVoteRequest(myId, currentTerm, lastLogIndex, lastLogTerm));
            }
            logMessage("about to enter timeout");
            readyChannels = selector.select(electionTimeout);
            if (readyChannels == 0) {
                resetTimeout = true;
            } else {
                logMessage("about to iterate over keys");
                Set<SelectionKey> selectedKeys = selector.selectedKeys();

                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while(keyIterator.hasNext()) {

                    SelectionKey key = keyIterator.next();

                    if(key.isAcceptable()) {
                        acceptConnection();
                    } else if (key.isConnectable()) {
                        logMessage("about to connect");
                        // pass
                    } else if (key.isReadable()) {
                        logMessage("about to read");
                        SocketChannel channel = (SocketChannel) key.channel();
                        Message message = (Message) RPCUtils.receiveMessage(channel);
                        boolean senderTermStale = message.term < this.currentTerm;
                        boolean myTermStale = message.term > this.currentTerm;
                        processMessageTerm(message);
                        if (message instanceof RequestVoteReply) {
                            RequestVoteReply reply = (RequestVoteReply) message;
                            if (reply.voteGranted) {
                                votesReceived += 1;
                            }
                            if (votesReceived > (otherServersMetadataMap.size()+1)/2) {
                                role = Server.ROLE.LEADER;
                                break;
                            }
                        } else if (message instanceof AppendEntriesRequest) {
                            logMessage(message);
                            AppendEntriesReply reply = new AppendEntriesReply(myId, this.currentTerm, processAppendEntriesRequest((AppendEntriesRequest) message, senderTermStale));
                            RPCUtils.sendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                            // If the leader's term is at least as large as the candidate's current term, then the candidate
                            // recognizes the leader as legitimate and returns to follower state.
                            if (!senderTermStale) {
                                role = Server.ROLE.FOLLOWER;
                                break;
                            }
                        } else if (message instanceof RequestVoteRequest) {
                            logMessage(message);
                            RequestVoteReply reply = null;
                            boolean grantingVote = grantVote((RequestVoteRequest) message, senderTermStale);

                            reply = new RequestVoteReply(myId, this.currentTerm, grantingVote);
                            RPCUtils.sendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                        } else {
                            assert(false);
                        }
                        if (myTermStale) {
                            role = Server.ROLE.FOLLOWER;
                            break;
                        }
                    } else if (key.isWritable()) {
                        logMessage("about to write");
                    }

                    keyIterator.remove();
                }
                if (!resetTimeout) {
                    currTime = Date.from(Instant.now());
                    electionTimeout -= currTime.getTime() - beforeSelectTime.getTime();
                    beforeSelectTime = currTime;
                }
            }
        }
    }

    private void leaderSendHeartbeatsAndListen() throws IOException {
        // initialize volatile state on leaders
        for (ServerMetadata meta : otherServersMetadataMap.values()) {
            // Subtracting 1 makes it apparent that we want to send the entry
            // corresponding to the next available index
            meta.nextIndex = (this.log.size() - 1) + 1;
            meta.matchIndex = -1;
        }
        int readyChannels = 0;
        Date lastHeartbeatTime = null;
        Date currTime = null;
        while (role==Server.ROLE.LEADER) {
            readyChannels = selector.selectNow();
            if (readyChannels == 0) {
                // Proj2: we may not be able to broadcast the same message to all servers,
                //   and so we may need to change the interface of broadcast(..), or use a
                //   different method to send server-tailored messages to all servers.
                currTime = Date.from(Instant.now());
                // null check allows us to send initial empty AppendEntriesRPCs upon election
                if(lastHeartbeatTime==null) {
                    broadcast(new AppendEntriesRequest(myId, this.currentTerm, -1, -1, null, this.commitIndex));
                    lastHeartbeatTime = Date.from(Instant.now());
                }
                // send regular heartbeat messages with log entries after a heartbeat interval has passed
                // Proj2: add proper log entry (if needed) as argument into AppendEntriesRequest
                if(currTime.getTime()-lastHeartbeatTime.getTime()>=HEARTBEAT_INTERVAL) {
                    broadcast(new AppendEntriesRequest(myId, this.currentTerm, -1, -1, null, this.commitIndex));
                    lastHeartbeatTime = Date.from(Instant.now());
                }
            } else {
                logMessage("about to iterate over keys");
                Set<SelectionKey> selectedKeys = selector.selectedKeys();

                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while(keyIterator.hasNext()) {

                    SelectionKey key = keyIterator.next();

                    if(key.isAcceptable()) {
                        acceptConnection();
                    } else if (key.isConnectable()) {
                        logMessage("about to connect");
                        // pass
                    } else if (key.isReadable()) {
                        logMessage("about to read");
                        SocketChannel channel = (SocketChannel) key.channel();
                        Message message = (Message) RPCUtils.receiveMessage(channel);
                        boolean senderTermStale = message.term < this.currentTerm;
                        boolean myTermStale = message.term > this.currentTerm;
                        processMessageTerm(message);
                        if (message instanceof AppendEntriesReply) {
                            // Proj2: write logic to handle AppendEntries message (as leader)
                            AppendEntriesReply reply = (AppendEntriesReply) message;
                            ServerMetadata meta = this.otherServersMetadataMap.get(reply.serverId);
                            if (reply.success) {
                                meta.matchIndex = meta.nextIndex;
                                meta.nextIndex += 1;
                                if (testMajorityN(meta.matchIndex)) {
                                    this.commitIndex = meta.matchIndex;
                                }
                            } else {
                                meta.nextIndex -= 1;
                            }
                        } else if (message instanceof RequestVoteRequest) {
                            logMessage(message);
                            RequestVoteReply reply = null;
                            boolean grantingVote = grantVote((RequestVoteRequest) message, senderTermStale);

                            reply = new RequestVoteReply(myId, this.currentTerm, grantingVote);
                            RPCUtils.sendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                        } else if (message instanceof AppendEntriesRequest) {
                            logMessage(message);
                            AppendEntriesReply reply = new AppendEntriesReply(myId, this.currentTerm, processAppendEntriesRequest((AppendEntriesRequest) message, senderTermStale));
                            RPCUtils.sendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                        } else {
                            assert(false);
                        }
                        if (myTermStale) {
                            role = Server.ROLE.FOLLOWER;
                            break;
                        }
                    } else if (key.isWritable()) {
                        logMessage("about to write");
                    }
                    keyIterator.remove();
                }
            }
        }
    }
}
