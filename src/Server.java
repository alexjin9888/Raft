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

/* TODO add throw exceptions for methods
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

            // Create a buffer to store request data
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            boolean done = false;
            
            while(!done) {
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
                          SocketChannel clientChannel = (SocketChannel) key.channel();
                          int bytesRead = clientChannel.read(buffer);
                          while (bytesRead != -1) {
                              System.out.println("[" + serverId + "]: read " + bytesRead + " bytes");
                              buffer.flip();
                              ByteArrayOutputStream messageBytes = new ByteArrayOutputStream();
                              while(buffer.hasRemaining()) {
//                                  System.out.println("Buffer has remaining");
//                                  System.out.print((char) buffer.get());
                                  messageBytes.write(buffer.get());
                              }
                              ByteArrayInputStream bis = new ByteArrayInputStream(messageBytes.toByteArray());
                              ObjectInput in = new ObjectInputStream(bis);
                              AppendEntriesRequest message = null;
                              try {
                                  message = (AppendEntriesRequest) in.readObject();
                              } catch (ClassNotFoundException e) {
                                  e.printStackTrace();
                              }
                              System.out.println("[" + serverId + "]: " + message);
                              buffer.clear();
                              bytesRead = clientChannel.read(buffer);
                          }
                          // TODO: not sure if right behavior to close channel after reading
                          clientChannel.close();
                          // TODO: need to write logic to respond to client
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
        
        ByteBuffer buffer = ByteBuffer.allocate(256);
        
        for (int i=0; i<otherAddresses.length; i++) {
            String[] entries = {};
            RPCUtils.sendAppendEntriesRPC(otherAddresses[i], 0, 0, 0, 0, entries, 0);
        }
    }
}
