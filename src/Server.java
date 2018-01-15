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

    public Server(InetSocketAddress myAddress, InetSocketAddress[] otherAddresses, String serverId) {
        this.myAddress = myAddress;
        this.otherAddresses = otherAddresses;
        this.serverId = serverId;
        socketChannels = new SocketChannel[otherAddresses.length];
        messagesQueue = new ByteBuffer[otherAddresses.length];
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
                System.out.println("[" + serverId + "]: about to enter timeout");
                long timeout = ThreadLocalRandom.current().nextInt(150, 300 + 1);
                readyChannels = selector.select(timeout);
//                System.out.println(serverId+ " has " + readyChannels + " readyChannels ");
       
                if (readyChannels == 0) {
                    // TODO: change server mode
                    broadcast();
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
                          // TODO: not sure if right behavior to close channel after reading
                          channel.close();
                          // TODO: write logic to send AppendEntries reply (as follower)
                          // TODO: write logic to handle AppendEntries reply (as leader)
                      } else if (key.isWritable()) {
                          System.out.println("[" + serverId + "]: about to write");
//                          SocketChannel clientChannel = (SocketChannel) key.channel();
//                          ByteBuffer messageBuffer = (ByteBuffer) key.attachment();
//                          while(messageBuffer.hasRemaining()) {
//                              clientChannel.write(messageBuffer);
//                          }
                      }

                      keyIterator.remove();
                    }                    
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
