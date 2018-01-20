import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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

    private String myId;
    private InetSocketAddress myAddress;
    private HashMap<String, ServerMetadata> otherServersMetadataMap;
    private ROLE role;
    
    private ServerSocketChannel myListenerChannel;
    private Selector selector;
    
    private enum ROLE { FOLLOWER, CANDIDATE, LEADER; }

    // TODO Add detailed comments for all instance variables
    // Persistent States
    private int currentTerm;
    private String votedFor;
    private String[] log;

    // Volatile States on all servers
    private int commitIndex;
    private int lastApplied;

    // Volatile States on leaders
    private int[] nextIndex;
    private int[] matchIndex;

    public Server(String serverId, HashMap<String, InetSocketAddress> serverAddressesMap) {
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
        
        // TODO initialize other instance vars
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
                        leaderListenAndSendHeartbeats();
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

    // TODO for final submission, replace this with an acceptable logging mechanism
    private void logMessage(Object message) {
        System.out.println("[" + myId + " " + role + "]:" + message);
    }

    private void followerListenAndRespond() throws IOException {
        int readyChannels = 0;
        long timeout = 0;
        Date beforeSelectTime = null;
        Date currTime = null;
        boolean resetTimeout = true;
        while (role==Server.ROLE.FOLLOWER) {
            if(resetTimeout) {
                beforeSelectTime = Date.from(Instant.now());
                timeout = ThreadLocalRandom.current().nextInt(1500, 3000 + 1);
                resetTimeout = false;
            }
            logMessage("about to enter timeout");
            readyChannels = selector.select(timeout);
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
                        if (message instanceof AppendEntriesRequest) {
                            // TODO check leader validity before accepting the request (if-else)
                            // TODO if the request should reset the timeout, then set resetTimeout = true
                            logMessage(message);
                            AppendEntriesReply reply = new AppendEntriesReply(myId, -1, true);
                            RPCUtils.sendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                            resetTimeout = true;
                        } else if (message instanceof RequestVoteRequest) {
                            logMessage(message);
                            // TODO only vote true for the first valid request (update boolean check)
                            // TODO if the request should reset the timeout, then set resetTimeout = true
                            RequestVoteReply reply = null;
                            if (true) {
                                reply = new RequestVoteReply(myId, -1, true);
                                resetTimeout = true;
                            } else {
                                reply = new RequestVoteReply(myId, -1, false);
                            }
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
                    timeout -= currTime.getTime() - beforeSelectTime.getTime();
                    beforeSelectTime = currTime;
                }
            }
        }
    }

    private void candidateRunForElection() throws IOException {
        int readyChannels = 0;
        int votesReceived = 0;
        long timeout = 0;
        Date beforeSelectTime = null;
        Date currTime = null;
        boolean resetTimeout = true;
        while (role==Server.ROLE.CANDIDATE) {
            if(resetTimeout) {
                beforeSelectTime = Date.from(Instant.now());
                timeout = ThreadLocalRandom.current().nextInt(1500, 3000 + 1);
                // Start Election
                currentTerm += 1;
                votesReceived = 1;
                resetTimeout = false;
                broadcast(new RequestVoteRequest(myId, 1338, 0, 0, 0));
            }
            logMessage("about to enter timeout");
            readyChannels = selector.select(timeout);
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
                        if (message instanceof RequestVoteReply) {
                            RequestVoteReply reply = (RequestVoteReply) message;
                            // TODO check term number (need if-else)
                            if (reply.voteGranted) {
                                votesReceived += 1;
                            }
                            if (votesReceived > (otherServersMetadataMap.size()+1)/2) {
                                role = Server.ROLE.LEADER;
                                break;
                            }
                        } else if (message instanceof AppendEntriesRequest) {
                            // TODO check leader validity before accepting it (if-else)
                            // TODO if the request should reset the timeout, then set resetTimeout = true
                            logMessage(message);
                            AppendEntriesReply reply = new AppendEntriesReply(myId, -1, true);
                            RPCUtils.sendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                            role = Server.ROLE.FOLLOWER;
                            break;
                        } else if (message instanceof RequestVoteRequest) {
                            // TODO check leader validity before accepting it (if-else)
                            // TODO if the request should reset the timeout, then set resetTimeout = true
                            logMessage(message);
                            RequestVoteReply reply = new RequestVoteReply(myId, -1, false);
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
                    timeout -= currTime.getTime() - beforeSelectTime.getTime();
                    beforeSelectTime = currTime;
                }
            }
        }
    }

    private void leaderListenAndSendHeartbeats() throws IOException {
        int readyChannels = 0;
        Date lastHeartbeatTime = null;
        Date currTime = null;
        long HEARTBEAT_INTERVAL = 1000;
        while (role==Server.ROLE.LEADER) {
            readyChannels = selector.selectNow();
            if (readyChannels == 0) {
                currTime = Date.from(Instant.now());
                if(lastHeartbeatTime==null || currTime.getTime()-lastHeartbeatTime.getTime()>=HEARTBEAT_INTERVAL) {
                    String[] entries = {};
                    broadcast(new AppendEntriesRequest(myId, 1337, 0, 0, 0, entries, 0));
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
                        
                        if (message instanceof AppendEntriesReply) {
                            // TODO Project 2: write logic to handle AppendEntries message (as leader)
                        } else if (message instanceof RequestVoteRequest) {
                            // TODO check term (if-else)
                        } else if (message instanceof AppendEntriesRequest) {
                            // TODO check leader validity before accepting it (if-else)
                            logMessage(message);
                            AppendEntriesReply reply = new AppendEntriesReply(myId, -1, true);
                            RPCUtils.sendMessage(otherServersMetadataMap.get(message.serverId).address, reply);
                            role = Server.ROLE.FOLLOWER;
                            break;
                        } else {
                            assert(false);
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
