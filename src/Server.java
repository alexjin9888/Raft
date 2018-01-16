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
        role = serverId == "Server3" ? "Leader" : "Follower";
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
                if (role=="Follower") {
                    followerListenAndRespond(serverChannel);
                } else if (role == "Candidate") {
                    while (true) {
                        System.out.println("[" + serverId + " " + role + "]: says hi");
                        role = "Follower";
                        break;
                    }
                } else if (role == "Leader") {
                    System.out.println("[" + serverId + " " + role + "]: says hi");
                    leaderListenAndBroadcast(serverChannel);
                } else {
                    assert(false);
                }
                selector.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void broadcast() {

        System.out.println("[" + serverId + "]: broadcasting");

        for (int i=0; i<otherAddresses.length; i++) {
            String[] entries = {};
            AppendEntriesRequest message = new AppendEntriesRequest(1337, 0, 0, 0, entries, 0);
            try {
                SocketChannel socketChannel = SocketChannel.open(otherAddresses[i]);
                socketChannel.configureBlocking(false);
                RPCUtils.sendMessage(socketChannel, message);
                socketChannel.register(selector, SelectionKey.OP_READ);
                // Remember to close
                // socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void acceptConnection(ServerSocketChannel serverChannel) throws IOException {
        System.out.println("[" + serverId + "]: about to accept");
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
        while (true) {
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
                System.out.println("[" + serverId + "]: about to iterate over keys");
                Set<SelectionKey> selectedKeys = selector.selectedKeys();

                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
                System.out.println("[" + serverId+ "]: has " + selectedKeys.size() + " selected keys");

                while(keyIterator.hasNext()) {

                    SelectionKey key = keyIterator.next();

                    if(key.isAcceptable()) {
                        acceptConnection(serverChannel);
                    } else if (key.isConnectable()) {
                        System.out.println("[" + serverId + "]: about to connect");
                        // pass
                    } else if (key.isReadable()) {
                        System.out.println("[" + serverId + "]: about to read");
                        SocketChannel channel = (SocketChannel) key.channel();
                        AppendEntriesRequest request = (AppendEntriesRequest) RPCUtils.receiveMessage(channel);
                        System.out.println("[" + serverId + "]: " + request);
                        // TODO: if the request should reset the timeout, then set resetTimeout = true
                        AppendEntriesReply reply = new AppendEntriesReply(-1, true);
                        RPCUtils.sendMessage(channel, reply);
                        channel.close();
                        // TODO: write logic to send AppendEntries reply (as follower)
                        // TODO: write logic to handle AppendEntries reply (as leader)
                    } else if (key.isWritable()) {
                        System.out.println("[" + serverId + "]: about to write");
                        // SocketChannel clientChannel = (SocketChannel) key.channel();
                        // ByteBuffer messageBuffer = (ByteBuffer) key.attachment();
                        // while(messageBuffer.hasRemaining()) {
                        //     clientChannel.write(messageBuffer);
                        // }
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
        while (true) {
            // System.out.println("[" + serverId + " " + role + "]: about to enter timeout");
            readyChannels = selector.selectNow();
            if (readyChannels == 0) {
                // System.out.println("[" + serverId+ "]: has " + selector.keys().size() + " keys");
                curTime = Date.from(Instant.now());
                if(lastHeartbeatTime==null || curTime.getTime()-lastHeartbeatTime.getTime()>=HEARTBEAT_INTERVAL) {
                    broadcast();
                    // if (lastHeartbeatTime!=null)
                    //     System.out.println(curTime.getTime()-lastHeartbeatTime.getTime());
                    lastHeartbeatTime = Date.from(Instant.now());
                }
            } else {
                System.out.println("[" + serverId + "]: about to iterate over keys");
                Set<SelectionKey> selectedKeys = selector.selectedKeys();

                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
                System.out.println("[" + serverId+ "]: has " + selectedKeys.size() + " selected keys");

                while(keyIterator.hasNext()) {

                    SelectionKey key = keyIterator.next();

                    if(key.isAcceptable()) {
                        acceptConnection(serverChannel);
                    } else if (key.isConnectable()) {
                        System.out.println("[" + serverId + "]: about to connect");
                        // pass
                    } else if (key.isReadable()) {
                        System.out.println("[" + serverId + "]: about to read");
                        SocketChannel channel = (SocketChannel) key.channel();
                        AppendEntriesReply reply = (AppendEntriesReply) RPCUtils.receiveMessage(channel);
                        System.out.println("[" + serverId + "]: " + reply);
                        // TODO: if the reply should reset the timeout, then set resetTimeout = true
                        channel.close();
                        // TODO: write logic to send AppendEntries reply (as follower)
                        // TODO: write logic to handle AppendEntries reply (as leader)
                    } else if (key.isWritable()) {
                        System.out.println("[" + serverId + "]: about to write");
                        // SocketChannel clientChannel = (SocketChannel) key.channel();
                        // ByteBuffer messageBuffer = (ByteBuffer) key.attachment();
                        // while(messageBuffer.hasRemaining()) {
                        //     clientChannel.write(messageBuffer);
                        // }
                    }

                    keyIterator.remove();
                }
            }
        }
    }
}
