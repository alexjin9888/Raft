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
 * The Server class implements features as per the RAFT consensus protocol.
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
 *      in the cluster are running and respond promptly to requests.
 *
 *
 *
 *
 */
public class Server implements Runnable {
    // Magical constants
    private static final int HEARTBEAT_INTERVAL = 1000;
    private static final int MIN_ELECTION_TIMEOUT = 3000;
    private static final int MAX_ELECTION_TIMEOUT = 5000;
    
    private static enum ROLE { FOLLOWER, CANDIDATE, LEADER; }

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

    private ListenerThread listenerThread;
    private Timer timer;

    // PersistentStorage
    private PersistentState myPersistentState;

    // Volatile State on all servers
    // * index of highest log entry known to be committed (initialized to 0,
    //   increases monotonically)
    private int commitIndex;
    // * index of highest log entry applied to state machine (initialized to 0,
    //   increases monotonically)
    private int lastApplied;

    // Role-specific Variables
    // * Follower
    private int votesReceived;
    // * Candidate
    private int lastLogIndex;
    private int lastLogTerm;
    // * Leader

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
            
            if (elemId.equals(this.myId)) {
                myAddress = elemAddress;
            } else {
                this.otherServersMetadataMap.put(elemId, new ServerMetadata(elemId, elemAddress));
            }
        }
        role = ROLE.FOLLOWER;

        // Start a thread to accept connections and add them to read selector
        listenerThread = new ListenerThread(myAddress);
        listenerThread.start();
        
        timer = new Timer();

        this.myPersistentState = new PersistentState(this.myId);

        this.commitIndex = -1;
        this.lastApplied = -1;

        // Role-specific Variables
        // * Follower
        this.votesReceived = -1;
        // * Candidate
        this.lastLogIndex = -1;
        this.lastLogTerm = -1;
        // * Leader

        // Debug
        myLogger.info(myId + " :: Configuration File Defined To Be :: "+System.getProperty("log4j.configurationFile"));
    }

    // Startup the server
    // Essentially a while loop that calls different methods based on our role
    public void run() {
        try {
            while(true) {
                timer.reset(0);
                // 3 While loops for different roles
                switch (role) {
                    case FOLLOWER:
                        // followerListenAndRespond();
                        serverListenAndRespond(Server.ROLE.FOLLOWER);
                        break;
                    case CANDIDATE: 
                        // candidateRunElection();
                        serverListenAndRespond(Server.ROLE.CANDIDATE);
                        break;
                    case LEADER:
                        // leaderSendHeartbeatsAndListen();
                        serverListenAndRespond(Server.ROLE.LEADER);
                        break;
                    default:
                        assert(false);
                }

                listenerThread.resetReadSelector();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveStateAndSendMessage(InetSocketAddress address, Message message) {
        try {
            this.myPersistentState.save();
            NetworkUtils.sendMessage(address, message);
        } catch (IOException e) {
            // Pass to fail silently
            // e.printStackTrace();
        }
    }

    // Helper function to send message to all other servers (excluding me)
    private void broadcast(Message message) throws IOException {

        logMessage("broadcasting");
        
        for (ServerMetadata meta : otherServersMetadataMap.values()) {
            saveStateAndSendMessage(meta.address, message);
        }
    }

    // Helper logger that logs to a log4j2 logger instance
    private void logMessage(Object message) {
        myLogger.info(myId + " :: " + role + " :: " + message);
    }

    // Compares the sender's term against ours
    // Does a term update when necessary
    private void processMessageTerm(Message message) {
        if (message.term > this.myPersistentState.currentTerm) {
            this.myPersistentState.currentTerm = message.term;
            this.myPersistentState.votedFor = null;
        }
    }

    // Checks if we grant the sender our vote ($5.2 Leader election)
    private boolean grantVote(RequestVoteRequest message, boolean senderTermStale) {
        if (senderTermStale) {
            return false;
        } else {
            if (this.myPersistentState.votedFor == null || this.myPersistentState.votedFor.equals(message.serverId)) {
                int lastLogIndex = this.myPersistentState.log.size() - 1;
                int lastLogTerm = lastLogIndex < 0 ? -1 : this.myPersistentState.log.get(lastLogIndex).term;
                // Proj2: make sure that this logic is correct for checking that a candidate's
                // log is at least as up-to-date as ours. Test this logic afterwards
                if (message.lastLogIndex >= lastLogIndex && message.lastLogTerm >= lastLogTerm) {
                    assert(!senderTermStale);
                    this.myPersistentState.votedFor = message.serverId;
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    // Processes an AppendEntries request from leader ($5.3 Log replication)
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
            if (message.prevLogIndex >= 0 && message.prevLogIndex < this.myPersistentState.log.size()) {
                // Proj2: check prevLogTerm
                if (this.myPersistentState.log.get(message.prevLogIndex).term != message.prevLogTerm) {
                    this.myPersistentState.log = this.myPersistentState.log.subList(0, message.prevLogIndex);
                    return false;
                } else {
                    // Proj2: is this okay to add log entry unconditionally?
                    // Otherwise, check whether this if cond. is necessary
                    if (!this.myPersistentState.log.contains(message.entry)) {
                        this.myPersistentState.log.add(message.entry);
                    }
                    // Proj2: Consider implementing Figure 2, All servers, bullet point 1/2 here
                    if (message.leaderCommit > this.commitIndex) {
                        this.commitIndex = Math.min(message.leaderCommit, this.myPersistentState.log.size() - 1);
                    }
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    // Called by the leader to determine if we should update the commitIndex
    // ($5.3, $5.4)
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
        if (this.myPersistentState.log.get(candidateN).term != this.myPersistentState.currentTerm) {
            return false;
        }
        return true;
    }

    // // Follower does the following 2 tasks:
    // //   1) Respond to requests from candidates and leaders
    // //   2) If election timeout elapses without receiving a valid AppendEntries request from
    // //      current leader or granting vote to candidate: convert to candidate
    // private void followerListenAndRespond() throws IOException {
    //     // Follower-specific action
    //     timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));

    //     while (role==Server.ROLE.FOLLOWER) {
    //         if(timer.timeIsUp()) {
    //             role = Server.ROLE.CANDIDATE;
    //             break;
    //         }
    //         if (listenerThread.readSelector.selectNow() == 0) {
    //             continue;
    //         } else {
    //             logMessage("about to iterate over keys");
    //             Set<SelectionKey> selectedKeys = listenerThread.readSelector.selectedKeys();

    //             Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

    //             while(keyIterator.hasNext()) {
    //                 SelectionKey key = keyIterator.next();
    //                 logMessage("about to read");
    //                 SocketChannel channel = (SocketChannel) key.channel();
    //                 Message message = (Message) NetworkUtils.receiveMessage(channel, true);
    //                 boolean senderTermStale = message.term < this.myPersistentState.currentTerm;
    //                 processMessageTerm(message);
    //                 if (message instanceof AppendEntriesRequest) {
    //                     logMessage(message);
    //                     AppendEntriesReply reply = new AppendEntriesReply(myId, this.myPersistentState.currentTerm, processAppendEntriesRequest((AppendEntriesRequest) message, senderTermStale));
    //                     if (!senderTermStale) {
    //                         timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
    //                     }
    //                     saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
    //                 } else if (message instanceof RequestVoteRequest) {
    //                     logMessage(message);
    //                     RequestVoteReply reply = null;
    //                     boolean grantingVote = grantVote((RequestVoteRequest) message, senderTermStale);

    //                     reply = new RequestVoteReply(myId, this.myPersistentState.currentTerm, grantingVote);
    //                     if (grantingVote) {
    //                         timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
    //                     }
    //                     saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
    //                 } else {
    //                     assert(false);
    //                 }
    //                 keyIterator.remove();
    //             }
    //         }
    //     }
    // }

    // // Candidate does the following 4 tasks:
    // //   1) On conversion to candidate, start election:
    // //      a) Increment currentTerm
    // //      b) Vote for self
    // //      c) Reset election timer
    // //      d) Send RequestVote requests to all other servers
    // //   2) If votes received from majority of servers: become leader
    // //   3) If AppendEntries request received from new leader: convert to follower
    // //   4) If election timeout elapses: start new election
    // private void candidateRunElection() throws IOException {
    //     // Candidate-specific properties
    //     int votesReceived = 0;

    //     while (role==Server.ROLE.CANDIDATE) {
    //         if(timer.timeIsUp()) {
    //             timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
    //             // Candidate-specific: Start Election
    //             this.myPersistentState.currentTerm += 1;
    //             votesReceived = 1;
    //             this.myPersistentState.votedFor = myId;
    //             int lastLogIndex = this.myPersistentState.log.size()-1;
    //             // lastLogTerm = -1 means there are no log entries
    //             int lastLogTerm = lastLogIndex < 0 ?
    //                               -1 : this.myPersistentState.log.get(lastLogIndex).term;
    //             broadcast(new RequestVoteRequest(myId, this.myPersistentState.currentTerm, lastLogIndex, lastLogTerm));
    //         }
    //         if (listenerThread.readSelector.selectNow() == 0) {
    //             continue;
    //         } else {
    //             logMessage("about to iterate over keys");
    //             Set<SelectionKey> selectedKeys = listenerThread.readSelector.selectedKeys();

    //             Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

    //             while(keyIterator.hasNext()) {
    //                 SelectionKey key = keyIterator.next();
    //                 logMessage("about to read");
    //                 SocketChannel channel = (SocketChannel) key.channel();
    //                 Message message = (Message) NetworkUtils.receiveMessage(channel, true);
    //                 boolean myTermStale = message.term > this.myPersistentState.currentTerm;
    //                 boolean senderTermStale = message.term < this.myPersistentState.currentTerm;
    //                 processMessageTerm(message);
    //                 if (message instanceof AppendEntriesRequest) {
    //                     logMessage(message);
    //                     AppendEntriesReply reply = new AppendEntriesReply(myId, this.myPersistentState.currentTerm, processAppendEntriesRequest((AppendEntriesRequest) message, senderTermStale));
    //                     saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
    //                     // If the leader's term is at least as large as the candidate's current term, then the candidate
    //                     // recognizes the leader as legitimate and returns to follower state.
    //                     if (!senderTermStale) {
    //                         role = Server.ROLE.FOLLOWER;
    //                         break;
    //                     }
    //                 } else if (message instanceof RequestVoteRequest) {
    //                     logMessage(message);
    //                     RequestVoteReply reply = null;
    //                     boolean grantingVote = grantVote((RequestVoteRequest) message, senderTermStale);

    //                     reply = new RequestVoteReply(myId, this.myPersistentState.currentTerm, grantingVote);
    //                     saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
    //                 } else if (message instanceof RequestVoteReply) {
    //                     RequestVoteReply reply = (RequestVoteReply) message;
    //                     if (reply.voteGranted) {
    //                         votesReceived += 1;
    //                     }
    //                     if (votesReceived > (otherServersMetadataMap.size()+1)/2) {
    //                         role = Server.ROLE.LEADER;
    //                         break;
    //                     }
    //                 } else {
    //                     assert(false);
    //                 }
    //                 if (myTermStale) {
    //                     role = Server.ROLE.FOLLOWER;
    //                     break;
    //                 }
    //                 keyIterator.remove();
    //             }
    //         }
    //     }
    // }

    // // Leader does the following 4 tasks:
    // // 1) Upon election: send initial empty AppendEntries requests (heartbeat) to
    // //    each server; repeat during idle periods to prevent election timeouts
    // //    (§5.2)
    // // 2) If command received from client: append entry to local log, respond
    // //    after entry applied to state machine (§5.3) 
    // // 3) If last log index ≥ nextIndex for a follower: send AppendEntries requests
    // //    with log entries starting at nextIndex
    // //    a) If successful: update nextIndex and matchIndex for follower (§5.3)
    // //    b) If AppendEntries fails because of log inconsistency: decrement
    // //       nextIndex and retry (§5.3)
    // // 4) If there exists an N such that N > commitIndex, a majority of
    // //    matchIndex[i] ≥ N, and log[N].term == currentTerm:
    // //    set commitIndex = N (§5.3, §5.4).
    // private void leaderSendHeartbeatsAndListen() throws IOException {
    //     // Leader-specific properties and actions
    //     // initialize volatile state on leaders
    //     for (ServerMetadata meta : otherServersMetadataMap.values()) {
    //         // Subtracting 1 makes it apparent that we want to send the entry
    //         // corresponding to the next available index
    //         meta.nextIndex = (this.myPersistentState.log.size() - 1) + 1;
    //         meta.matchIndex = -1;
    //     }
    //     // send initial empty AppendEntriesRequests upon promotion to leader
    //     broadcast(new AppendEntriesRequest(myId, this.myPersistentState.currentTerm, -1, -1, null, this.commitIndex));

    //     while (role==Server.ROLE.LEADER) {
    //         if (timer.timeIsUp()) {
    //             // Proj2: we may not be able to broadcast the same message to all servers,
    //             //   and so we may need to change the interface of broadcast(..), or use a
    //             //   different method to send server-tailored messages to all servers.
    //             // Proj2: add proper log entry (if needed) as argument into AppendEntriesRequest
    //             // send regular heartbeat messages with server-tailored log entries after a heartbeat interval has passed
    //             broadcast(new AppendEntriesRequest(myId, this.myPersistentState.currentTerm, -1, -1, null, this.commitIndex));
    //             timer.reset(HEARTBEAT_INTERVAL);
    //         }
    //         if (listenerThread.readSelector.selectNow() == 0) {
    //             continue;
    //         } else {
    //             logMessage("about to iterate over keys");
    //             Set<SelectionKey> selectedKeys = listenerThread.readSelector.selectedKeys();

    //             Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

    //             while(keyIterator.hasNext()) {
    //                 SelectionKey key = keyIterator.next();
    //                 logMessage("about to read");
    //                 SocketChannel channel = (SocketChannel) key.channel();
    //                 Message message = (Message) NetworkUtils.receiveMessage(channel, true);
    //                 boolean myTermStale = message.term > this.myPersistentState.currentTerm;
    //                 boolean senderTermStale = message.term < this.myPersistentState.currentTerm;
    //                 processMessageTerm(message);
    //                 if (message instanceof AppendEntriesRequest) {
    //                     logMessage(message);
    //                     AppendEntriesReply reply = new AppendEntriesReply(myId, this.myPersistentState.currentTerm, processAppendEntriesRequest((AppendEntriesRequest) message, senderTermStale));
    //                     saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
    //                 } else if (message instanceof RequestVoteRequest) {
    //                     logMessage(message);
    //                     RequestVoteReply reply = null;
    //                     boolean grantingVote = grantVote((RequestVoteRequest) message, senderTermStale);

    //                     reply = new RequestVoteReply(myId, this.myPersistentState.currentTerm, grantingVote);
    //                     saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
    //                 } else if (message instanceof AppendEntriesReply) {
    //                     // Proj2: write logic to handle AppendEntries message (as leader)
    //                     AppendEntriesReply reply = (AppendEntriesReply) message;
    //                     ServerMetadata meta = this.otherServersMetadataMap.get(reply.serverId);
    //                     if (reply.success) {
    //                         meta.matchIndex = meta.nextIndex;
    //                         meta.nextIndex += 1;
    //                         if (testMajorityN(meta.matchIndex)) {
    //                             this.commitIndex = meta.matchIndex;
    //                         }
    //                     } else {
    //                         if (meta.nextIndex > 0) {
    //                             meta.nextIndex -= 1;
    //                         }
    //                     }
    //                 } else {
    //                     assert(false);
    //                 }
    //                 if (myTermStale) {
    //                     role = Server.ROLE.FOLLOWER;
    //                     break;
    //                 }
    //                 keyIterator.remove();
    //             }
    //         }
    //     }
    // }

    // Helper function to initialize server according to its role
    private void serverInitialization() throws IOException {
        switch (role) {
            case FOLLOWER:
                // Follower-specific action
                timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
                break;
            case CANDIDATE:
                // Candidate-specific properties
                this.votesReceived = 0;
                break;
            case LEADER:
                // Leader-specific properties and actions
                // initialize volatile state on leaders
                for (ServerMetadata meta : otherServersMetadataMap.values()) {
                    // Subtracting 1 makes it apparent that we want to send the entry
                    // corresponding to the next available index
                    meta.nextIndex = (this.myPersistentState.log.size() - 1) + 1;
                    meta.matchIndex = -1;
                }
                // send initial empty AppendEntriesRequests upon promotion to leader
                broadcast(new AppendEntriesRequest(myId, this.myPersistentState.currentTerm, -1, -1, null, this.commitIndex));
                break;
            default:
                assert(false);
        }

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
    private void serverListenAndRespond(ROLE myCurrentRole) throws IOException {
        serverInitialization();
        while (role==myCurrentRole) {
            if (timer.timeIsUp()) {
                switch (role) {
                    case FOLLOWER:
                        role = Server.ROLE.CANDIDATE;
                        break;
                    case CANDIDATE:
                        timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
                        // Candidate-specific: Start Election
                        this.myPersistentState.currentTerm += 1;
                        this.votesReceived = 1;
                        this.myPersistentState.votedFor = myId;
                        this.lastLogIndex = this.myPersistentState.log.size()-1;
                        // lastLogTerm = -1 means there are no log entries
                        this.lastLogTerm = this.lastLogIndex < 0 ?
                            -1 : this.myPersistentState.log.get(lastLogIndex).term;
                        broadcast(new RequestVoteRequest(myId, this.myPersistentState.currentTerm, lastLogIndex, lastLogTerm));
                        break;
                    case LEADER:
                        // Proj2: we may not be able to broadcast the same message to all servers,
                        //   and so we may need to change the interface of broadcast(..), or use a
                        //   different method to send server-tailored messages to all servers.
                        // Proj2: add proper log entry (if needed) as argument into AppendEntriesRequest
                        // send regular heartbeat messages with server-tailored log entries after a heartbeat interval has passed
                        broadcast(new AppendEntriesRequest(myId, this.myPersistentState.currentTerm, -1, -1, null, this.commitIndex));
                        timer.reset(HEARTBEAT_INTERVAL);
                        break;
                    default:
                        assert(false);
                    }
            }
            if (listenerThread.readSelector.selectNow() == 0) {
                continue;
            } else {
                logMessage("about to iterate over keys");
                Set<SelectionKey> selectedKeys = listenerThread.readSelector.selectedKeys();

                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while(keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    logMessage("about to read");
                    SocketChannel channel = (SocketChannel) key.channel();
                    Message message = (Message) NetworkUtils.receiveMessage(channel, true);
                    boolean myTermStale = message.term > this.myPersistentState.currentTerm;
                    boolean senderTermStale = message.term < this.myPersistentState.currentTerm;
                    processMessageTerm(message);
                    if (message instanceof AppendEntriesRequest) {
                        logMessage(message);
                        AppendEntriesReply reply = new AppendEntriesReply(myId, this.myPersistentState.currentTerm, processAppendEntriesRequest((AppendEntriesRequest) message, senderTermStale));
                        if (role==Server.ROLE.FOLLOWER && !senderTermStale) {
                            timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
                        }
                        saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                    } else if (message instanceof RequestVoteRequest) {
                        logMessage(message);
                        RequestVoteReply reply = null;
                        boolean grantingVote = grantVote((RequestVoteRequest) message, senderTermStale);

                        reply = new RequestVoteReply(myId, this.myPersistentState.currentTerm, grantingVote);
                        if (role==Server.ROLE.FOLLOWER && grantingVote) {
                            timer.reset(ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1));
                        }
                        saveStateAndSendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                        // If the leader's term is at least as large as the candidate's current term, then the candidate
                        // recognizes the leader as legitimate and returns to follower state.
                        if (role==Server.ROLE.CANDIDATE && !senderTermStale) {
                            role = Server.ROLE.FOLLOWER;
                            break;
                        }
                    } else if (message instanceof AppendEntriesReply) {
                        assert(role==Server.ROLE.LEADER);
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
                            if (meta.nextIndex > 0) {
                                meta.nextIndex -= 1;
                            }
                        }
                    } else if (message instanceof RequestVoteReply) {
                        assert(role==Server.ROLE.CANDIDATE);
                        RequestVoteReply reply = (RequestVoteReply) message;
                        if (reply.voteGranted) {
                            votesReceived += 1;
                        }
                        if (votesReceived > (otherServersMetadataMap.size()+1)/2) {
                            role = Server.ROLE.LEADER;
                            break;
                        }
                    } else {
                        assert(false);
                    }
                    if (myTermStale) {
                        role = Server.ROLE.FOLLOWER;
                        break;
                    }
                    keyIterator.remove();
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length!=2) {
            System.out.println("Please suppply exactly two arguments");
            System.out.println("Usage: <myPortIndex> <port0>,<port1>,...");
            System.out.println("Note: List of ports is 0-indexed");
            System.exit(-1);
        }

        String[] allPorts = args[1].split(",");
        int myPortIndex = Integer.parseInt(args[0]); 
        
        if (myPortIndex < 0 || myPortIndex >= allPorts.length) {
            System.out.println("Please supply a valid index for first argument");
            System.out.println("Usage: <myPortIndex> <port0>,<port1>,...");
            System.out.println("Note: List of ports is 0-indexed");
            System.exit(-1);
        }

        System.setProperty("log4j.configurationFile", "./src/log4j2.xml");
        HashMap<String, InetSocketAddress> serverAddressesMap = new HashMap<String, InetSocketAddress>();
        for (int i=0; i<allPorts.length; i++) {
            serverAddressesMap.put("Server" + i, new InetSocketAddress("localhost", Integer.parseInt(allPorts[i])));   
        }

        Server myServer = new Server("Server" + myPortIndex, serverAddressesMap);
        System.out.println("Running Server" + myPortIndex + " now");
        myServer.run();
    }
}
