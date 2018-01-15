import java.io.IOException;
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

/* 
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
            ByteBuffer buffer = ByteBuffer.allocate(256);
            boolean done = false;

            // TODO: not sure if this code is needed
//            for (int i=0; i<otherAddresses.length; i++) {
//                socketChannels[i].register(selector, SelectionKey.OP_READ, messagesQueue[i]);
//            }

            while(!done) {
                System.out.println(serverId + " about to enter timeout");
                long timeout = ThreadLocalRandom.current().nextInt(150, 300 + 1);
                readyChannels = selector.select(timeout);
//                System.out.println(serverId+ " has " + readyChannels + " readyChannels ");
       
                if (readyChannels == 0) {
                    // TODO: change server mode
                    broadcast();
                } else {
                    System.out.println(serverId + " about to iterate over keys");
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();

                    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
                    System.out.println(serverId+ " has " + selectedKeys.size() + " selected keys");

                    while(keyIterator.hasNext()) {

                      SelectionKey key = keyIterator.next();

                      if(key.isAcceptable()) {
                          System.out.println(serverId + " about to accept");
                          SocketChannel clientChannel = serverChannel.accept();
                          clientChannel.configureBlocking(false);
                          clientChannel.register(selector, SelectionKey.OP_READ);
                      } else if (key.isConnectable()) {
                          System.out.println(serverId + " about to connect");
                          // pass
                      } else if (key.isReadable()) {
                          System.out.println(serverId + " about to read");
                          SocketChannel clientChannel = (SocketChannel) key.channel();
                          int bytesRead = clientChannel.read(buffer);
                          while (bytesRead != -1) {
                              System.out.println(serverId + " read " + bytesRead);
                              buffer.flip();
                              while(buffer.hasRemaining()) {
//                                  System.out.println("Buffer has remaining");
                                  System.out.print((char) buffer.get());
                              }
                              buffer.clear();
                              bytesRead = clientChannel.read(buffer);
                          }
                          // TODO: not sure if right behavior to close channel after reading
                          clientChannel.close();
                          // TODO: need to write logic to respond to client
                      } else if (key.isWritable()) {
                          System.out.println(serverId + " about to write");
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
 

        System.out.println(serverId + " broadcasting");
        
        ByteBuffer buffer = ByteBuffer.allocate(256);
        
        for (int i=0; i<otherAddresses.length; i++) {
            try {
                socketChannels[i] = SocketChannel.open(otherAddresses[i]);
                socketChannels[i].configureBlocking(false);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
//            messagesQueue[i] = ByteBuffer.allocate(256);
//            try {
//                // pass
//                socketChannels[i].connect(otherAddresses[i]);
//                //TODO remove while loop
//                while(!socketChannels[i].finishConnect()) {
//                    // pass
//                }
//            } catch (IOException e1) {
//                // TODO Auto-generated catch block
//                e1.printStackTrace();
//            }
            buffer.clear();
            buffer.put(("Hello World from " + serverId + "\n").getBytes());
            buffer.flip();
            while(buffer.hasRemaining()) {
                try {
                    socketChannels[i].write(buffer);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            try {
                socketChannels[i].close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
