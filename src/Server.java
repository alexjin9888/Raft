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

/* TODO consider adding throw exceptions for methods/classes.
 * 
 * 
 * 
 * 
 * 
 * 
 */
public class Server implements Runnable {

    InetSocketAddress myAddress;
    InetSocketAddress[] otherAddresses;
    SocketChannel[] socketChannels;
    ByteBuffer[] messagesQueue; // TODO: is this necessary?
    String serverId; // TODO: is this necessary?
    private String role;
    private Selector selector;

    // TODO Add comments
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

    public Server(InetSocketAddress myAddress, InetSocketAddress[] otherAddresses, String serverId) {
        this.myAddress = myAddress;
        this.otherAddresses = otherAddresses;
        this.serverId = serverId;
        socketChannels = new SocketChannel[otherAddresses.length];
        messagesQueue = new ByteBuffer[otherAddresses.length];
        role = "Follower";
        // TODO initialize private states
    }

    // Startup the server
    public void run() {
        try {
            // Create a server to listen and respond to requests
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(myAddress);

            while(true) {
                selector = Selector.open();
                serverChannel.register(selector, SelectionKey.OP_ACCEPT);
                // 3 While loops for different roles
                switch (role) {
                    case "Follower":
                        followerListenAndRespond(serverChannel);
                        break;
                    case "Candidate": 
                        candidateRunForElection(serverChannel);
                        break;
                    case "Leader":
                        leaderListenAndBroadcast(serverChannel);
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

        System.out.println("[" + serverId + " " + role + "]: broadcasting");

        for (int i=0; i<otherAddresses.length; i++) {
            String[] entries = {};
            try {
                SocketChannel socketChannel = SocketChannel.open(otherAddresses[i]);
                socketChannel.configureBlocking(false);
                RPCUtils.sendMessage(socketChannel, message);
                socketChannel.register(selector, SelectionKey.OP_READ);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void acceptConnection(ServerSocketChannel serverChannel) throws IOException {
        System.out.println("[" + serverId + " " + role + "]: about to accept");
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(selector, SelectionKey.OP_READ);
    }

    private void followerListenAndRespond(ServerSocketChannel serverChannel) throws IOException {
        int readyChannels = 0;
        long timeout = 0;
        Date beforeSelectTime = null;
        Date curTime = null;
        boolean resetTimeout = true;
        while (role=="Follower") {
            if(resetTimeout) {
                beforeSelectTime = Date.from(Instant.now());
                timeout = ThreadLocalRandom.current().nextInt(1500, 3000 + 1);
                resetTimeout = false;
            }
            System.out.println("[" + serverId + " " + role + "]: about to enter timeout");
            readyChannels = selector.select(timeout);
            if (readyChannels == 0) {
                role = "Candidate";
                break;
            } else {
                System.out.println("[" + serverId + " " + role + "]: about to iterate over keys");
                Set<SelectionKey> selectedKeys = selector.selectedKeys();

                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while(keyIterator.hasNext()) {

                    SelectionKey key = keyIterator.next();

                    if(key.isAcceptable()) {
                        acceptConnection(serverChannel);
                    } else if (key.isConnectable()) {
                        System.out.println("[" + serverId + " " + role + "]: about to connect");
                        // pass
                    } else if (key.isReadable()) {
                        System.out.println("[" + serverId + " " + role + "]: about to read");
                        SocketChannel channel = (SocketChannel) key.channel();

                        Object message = RPCUtils.receiveMessage(channel);
                        if (message instanceof AppendEntriesRequest) {
                            // TODO check leader validity before accepting it (if-else)
                            System.out.println("[" + serverId + " " + role + "]: " + message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            AppendEntriesReply reply = new AppendEntriesReply(-1, true);
                            RPCUtils.sendMessage(channel, reply);
                            resetTimeout = true;
                        } else if (message instanceof RequestVoteRequest) {
                            System.out.println("[" + serverId + " " + role + "]: " + message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            // TODO only vote true for the first valid request (update boolean check)
                            RequestVoteReply reply = null;
                            if (true) {
                                reply = new RequestVoteReply(-1, true);
                                resetTimeout = true;
                            } else {
                                reply = new RequestVoteReply(-1, false);
                            }
                            RPCUtils.sendMessage(channel, reply);
                        } else {
                            assert(false);
                        }
                        channel.close();
                    } else if (key.isWritable()) {
                        System.out.println("[" + serverId + " " + role + "]: about to write");
                    }

                    keyIterator.remove();
                }
                if (!resetTimeout) {
                    curTime = Date.from(Instant.now());
                    timeout -= curTime.getTime() - beforeSelectTime.getTime();
                    beforeSelectTime = curTime;
                }
            }
        }
    }

    private void candidateRunForElection(ServerSocketChannel serverChannel) throws IOException {
        int votesReceived = 0;
        int readyChannels = 0;
        long timeout = 0;
        Date beforeSelectTime = null;
        Date curTime = null;
        boolean resetTimeout = true;
        while (role=="Candidate") {
            if(resetTimeout) {
                beforeSelectTime = Date.from(Instant.now());
                timeout = ThreadLocalRandom.current().nextInt(1500, 3000 + 1);
                // Start Election
                currentTerm += 1;
                votesReceived = 1;
                resetTimeout = false;
                broadcast(new RequestVoteRequest(1338, 0, 0, 0));
            }
            System.out.println("[" + serverId + " " + role + "]: about to enter timeout");
            readyChannels = selector.select(timeout);
            if (readyChannels == 0) {
                resetTimeout = true;
            } else {
                System.out.println("[" + serverId + " " + role + "]: about to iterate over keys");
                Set<SelectionKey> selectedKeys = selector.selectedKeys();

                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while(keyIterator.hasNext()) {

                    SelectionKey key = keyIterator.next();

                    if(key.isAcceptable()) {
                        acceptConnection(serverChannel);
                    } else if (key.isConnectable()) {
                        System.out.println("[" + serverId + " " + role + "]: about to connect");
                        // pass
                    } else if (key.isReadable()) {
                        System.out.println("[" + serverId + " " + role + "]: about to read");
                        SocketChannel channel = (SocketChannel) key.channel();
                        Object message = RPCUtils.receiveMessage(channel);
                        if (message instanceof RequestVoteReply) {
                            RequestVoteReply reply = (RequestVoteReply) message;
                            // TODO check term number
                            if (reply.voteGranted) {
                                votesReceived += 1;
                            }
                            if (votesReceived > (otherAddresses.length+1)/2) {
                                role = "Leader";
                                channel.close();
                                break;
                            }
                        } else if (message instanceof AppendEntriesRequest) {
                            // TODO check leader validity before accepting it
                            System.out.println("[" + serverId + " " + role + "]: " + message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            AppendEntriesReply reply = new AppendEntriesReply(-1, true);
                            RPCUtils.sendMessage(channel, reply);
                            role = "Follower";
                            channel.close();
                            break;
                        } else if (message instanceof RequestVoteRequest) {
                            // TODO check leader validity before accepting it
                            System.out.println("[" + serverId + " " + role + "]: " + message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            RequestVoteReply reply = new RequestVoteReply(-1, false);
                            RPCUtils.sendMessage(channel, reply);
                        } else {
                            assert(false);
                        }
                        channel.close();
                    } else if (key.isWritable()) {
                        System.out.println("[" + serverId + " " + role + "]: about to write");
                    }

                    keyIterator.remove();
                }
                if (!resetTimeout) {
                    curTime = Date.from(Instant.now());
                    timeout -= curTime.getTime() - beforeSelectTime.getTime();
                    beforeSelectTime = curTime;
                }
            }
        }
    }

    private void leaderListenAndBroadcast(ServerSocketChannel serverChannel) throws IOException {
        int readyChannels = 0;
        Date lastHeartbeatTime = null;
        Date curTime = null;
        long HEARTBEAT_INTERVAL = 1000;
        while (role=="Leader") {
            // System.out.println("[" + serverId + " " + role + "]: about to enter timeout");
            readyChannels = selector.selectNow();
            if (readyChannels == 0) {
                // System.out.println("[" + serverId+ "]: has " + selector.keys().size() + " keys");
                curTime = Date.from(Instant.now());
                if(lastHeartbeatTime==null || curTime.getTime()-lastHeartbeatTime.getTime()>=HEARTBEAT_INTERVAL) {
                    String[] entries = {};
                    broadcast(new AppendEntriesRequest(1337, 0, 0, 0, entries, 0));
                    // if (lastHeartbeatTime!=null)
                    //     System.out.println(curTime.getTime()-lastHeartbeatTime.getTime());
                    lastHeartbeatTime = Date.from(Instant.now());
                }
            } else {
                System.out.println("[" + serverId + " " + role + "]: about to iterate over keys");
                Set<SelectionKey> selectedKeys = selector.selectedKeys();

                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while(keyIterator.hasNext()) {

                    SelectionKey key = keyIterator.next();

                    if(key.isAcceptable()) {
                        acceptConnection(serverChannel);
                    } else if (key.isConnectable()) {
                        System.out.println("[" + serverId + " " + role + "]: about to connect");
                        // pass
                    } else if (key.isReadable()) {
                        System.out.println("[" + serverId + " " + role + "]: about to read");
                        SocketChannel channel = (SocketChannel) key.channel();
                        Object message = RPCUtils.receiveMessage(channel);
                        if (message instanceof AppendEntriesReply) {
                            // TODO Project 2: write logic to handle AppendEntries message (as leader)
                        } else if (message instanceof RequestVoteRequest) {
                            // TODO check term
                        } else if (message instanceof AppendEntriesRequest) {
                            // TODO check leader validity before accepting it
                            System.out.println("[" + serverId + " " + role + "]: " + message);
                            // TODO: if the request should reset the timeout, then set resetTimeout = true
                            AppendEntriesReply reply = new AppendEntriesReply(-1, true);
                            RPCUtils.sendMessage(channel, reply);
                            role = "Follower";
                            channel.close();
                            break;
                        } else {
                            assert(false);
                        }
                        channel.close();
                    } else if (key.isWritable()) {
                        System.out.println("[" + serverId + " " + role + "]: about to write");
                    }
                    keyIterator.remove();
                }
            }
        }
    }
}
