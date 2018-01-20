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
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;

/* 
 * 
 * 
 * 
 * 
 * 
 * 
 */
public class Server implements Runnable {

    // TODO Add detailed comments for all instance variables
    private String myId;
    private String leaderId;
    private InetSocketAddress myAddress;
    private HashMap<String, ServerMetadata> otherServersMetadataMap;
    private enum ROLE { FOLLOWER, CANDIDATE, LEADER; }
    private ROLE role;
    
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

    // Debug var
    Date startTime;

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
        role = Server.ROLE.FOLLOWER;

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
    }

    // Startup the server
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

    public void broadcast(Message message) {

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
        System.out.println("[" + myId + " " + (Date.from(Instant.now()).getTime() - startTime.getTime()) + " " + role + "]:" + message);
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
                electionTimeout = ThreadLocalRandom.current().nextInt(1500, 3000 + 1);
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
                electionTimeout = ThreadLocalRandom.current().nextInt(1500, 3000 + 1);
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
        long HEARTBEAT_INTERVAL = 400;
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
