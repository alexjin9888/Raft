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

/* TODO throw exceptions as far as possible.
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
    private String role;
    
    private ServerSocketChannel myListenerChannel;
    private Selector selector;

    // TODO Add detailed comments to instance variables
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
        role = "Follower";

        // Create a server to listen and respond to requests
        try {
            myListenerChannel = ServerSocketChannel.open();
            myListenerChannel.configureBlocking(false);
            myListenerChannel.socket().bind(myAddress);   
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        // TODO initialize other private instance vars
    }

    // Startup the server
    public void run() {
        try {
            while(true) {
                selector = Selector.open();
                myListenerChannel.register(selector, SelectionKey.OP_ACCEPT);
                // 3 While loops for different roles
                switch (role) {
                    case "Follower":
                        followerListenAndRespond();
                        break;
                    case "Candidate": 
                        candidateRunForElection();
                        break;
                    case "Leader":
                        leaderListenAndBroadcast();
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

    public void broadcast(Object message) {

        logMessage("broadcasting");

        for (ServerMetadata meta : otherServersMetadataMap.values()) {
            String[] entries = {};
            try {
                SocketChannel socketChannel = SocketChannel.open(meta.address);
                socketChannel.configureBlocking(false);
                RPCUtils.sendMessage(socketChannel, message);
                socketChannel.register(selector, SelectionKey.OP_READ);
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

    // TODO: replace this with another logging mechanism
    private void logMessage(Object message) {
        System.out.println("[" + myId + " " + role + "]:" + message);
    }

    private void followerListenAndRespond() throws IOException {
        int readyChannels = 0;
        long timeout = 0;
        Date beforeSelectTime = null;
        Date currTime = null;
        boolean resetTimeout = true;
        while (role=="Follower") {
            if(resetTimeout) {
                beforeSelectTime = Date.from(Instant.now());
                timeout = ThreadLocalRandom.current().nextInt(1500, 3000 + 1);
                resetTimeout = false;
            }
            logMessage("about to enter timeout");
            readyChannels = selector.select(timeout);
            if (readyChannels == 0) {
                role = "Candidate";
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

                        Object message = RPCUtils.receiveMessage(channel);
                        if (message instanceof AppendEntriesRequest) {
                            // TODO check leader validity before accepting it (if-else)
                            logMessage(message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            AppendEntriesReply reply = new AppendEntriesReply(myId, -1, true);
                            RPCUtils.sendMessage(channel, reply);
                            resetTimeout = true;
                        } else if (message instanceof RequestVoteRequest) {
                            logMessage(message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            // TODO only vote true for the first valid request (update boolean check)
                            RequestVoteReply reply = null;
                            if (true) {
                                reply = new RequestVoteReply(myId, -1, true);
                                resetTimeout = true;
                            } else {
                                reply = new RequestVoteReply(myId, -1, false);
                            }
                            RPCUtils.sendMessage(channel, reply);
                        } else {
                            assert(false);
                        }
                        channel.close();
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
        while (role=="Candidate") {
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
                        Object message = RPCUtils.receiveMessage(channel);
                        if (message instanceof RequestVoteReply) {
                            RequestVoteReply reply = (RequestVoteReply) message;
                            // TODO check term number
                            if (reply.voteGranted) {
                                votesReceived += 1;
                            }
                            if (votesReceived > (otherServersMetadataMap.size()+1)/2) {
                                role = "Leader";
                                channel.close();
                                break;
                            }
                        } else if (message instanceof AppendEntriesRequest) {
                            // TODO check leader validity before accepting it
                            logMessage(message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            AppendEntriesReply reply = new AppendEntriesReply(myId, -1, true);
                            RPCUtils.sendMessage(channel, reply);
                            role = "Follower";
                            channel.close();
                            break;
                        } else if (message instanceof RequestVoteRequest) {
                            // TODO check leader validity before accepting it
                            logMessage(message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            RequestVoteReply reply = new RequestVoteReply(myId, -1, false);
                            RPCUtils.sendMessage(channel, reply);
                        } else {
                            assert(false);
                        }
                        channel.close();
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

    private void leaderListenAndBroadcast() throws IOException {
        int readyChannels = 0;
        Date lastHeartbeatTime = null;
        Date currTime = null;
        long HEARTBEAT_INTERVAL = 1000;
        while (role=="Leader") {
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
                        Object message = RPCUtils.receiveMessage(channel);
                        if (message instanceof AppendEntriesReply) {
                            // TODO Project 2: write logic to handle AppendEntries message (as leader)
                        } else if (message instanceof RequestVoteRequest) {
                            // TODO check term
                        } else if (message instanceof AppendEntriesRequest) {
                            // TODO check leader validity before accepting it (if-else)
                            logMessage(message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            AppendEntriesReply reply = new AppendEntriesReply(myId, -1, true);
                            RPCUtils.sendMessage(channel, reply);
                            role = "Follower";
                            channel.close();
                            break;
                        } else {
                            assert(false);
                        }
                        channel.close();
                    } else if (key.isWritable()) {
                        logMessage("about to write");
                    }
                    keyIterator.remove();
                }
            }
        }
    }
}
