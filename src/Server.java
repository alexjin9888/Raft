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
        this.role = "Follower";
        // TODO initialize private states
    }

    // Startup the server
    public void run() {
        int readyChannels = 0;

        
        try {
            Selector selector = Selector.open();

            // Create a server to listen and respond to requests
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(myAddress);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            
            while(true) {
                // 3 While loops for different roles
                if (this.role=="Follower") {
                    long timeout = 0;
                    Date beforeSelectTime = null;
                    Date curTime = null;
                    boolean resetTimeout = true;
                    while (true) {
                        if(resetTimeout) {
                            beforeSelectTime = Date.from(Instant.now());
                            timeout = ThreadLocalRandom.current().nextInt(150, 1000 + 1);
                            resetTimeout = false;
                        }
                        System.out.println("[" + serverId + " " + this.role + "]: about to enter timeout");
                        readyChannels = selector.select(timeout);
                        // System.out.println(serverId+ " has " + readyChannels + " readyChannels ");
                        if (readyChannels == 0) {
                            this.role = "Candidate";
                            break;
                        } else {
                            System.out.println("[" + serverId + "]: about to iterate over keys");
                            Set<SelectionKey> selectedKeys = selector.selectedKeys();

                            Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
                            System.out.println("[" + serverId+ "]: has " + selectedKeys.size() + " selected keys");

                            while(keyIterator.hasNext()) {

                              SelectionKey key = keyIterator.next();

                              if(key.isAcceptable()) {
                                  System.out.println("[" + serverId + "]: about to accept");
                                  SocketChannel clientChannel = serverChannel.accept();
                                  clientChannel.configureBlocking(false);
                                  clientChannel.register(selector, SelectionKey.OP_READ);
                              } else if (key.isConnectable()) {
                                  System.out.println("[" + serverId + "]: about to connect");
                                  // pass
                              } else if (key.isReadable()) {
                                  System.out.println("[" + serverId + "]: about to read");
                                  SocketChannel channel = (SocketChannel) key.channel();
                                  AppendEntriesRequest message = (AppendEntriesRequest) RPCUtils.receiveMessage(channel);
                                  System.out.println("[" + serverId + "]: " + message);
                                  // TODO: if the message should reset the timeout, then set resetTimeout = true
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
                } else if (this.role == "Candidate") {
                    while (true) {
                        System.out.println("[" + serverId + " " + this.role + "]: says hi");
                        this.role = "Follower";
                        break;
                    }
                } else if (this.role == "Leader") {
                    while (true) {
                        System.out.println("[" + serverId + " " + this.role + "]: says hi");
                        this.role = "Follower";
                        break;
                    }
                } else {
                    assert(false);
                }
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
                RPCUtils.sendMessage(otherAddresses[i], message);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
