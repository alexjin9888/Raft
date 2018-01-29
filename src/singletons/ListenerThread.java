package singletons;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.Iterator;

/**
 * A thread that handles accepts
 * When a server boots up, it creates a server socket channel using this class
 * and polls for incoming connections. After establishing a connection, it
 * exposes a read selector which we can use to query read-ready channels that
 * have data to read.
 */
public class ListenerThread extends Thread {
    // acceptChannel  The channel for incoming messages
    private ServerSocketChannel acceptChannel;
    // acceptSelector Selector for all accepts
    private Selector acceptSelector;
    // readSelector   Register channels to read from using this selector
    private Selector readSelector;
    
    /**
     * The constructor creates a channel and listens for incoming connections
     * (nonblocking mode) using this.
     * @param address The address that server listens at
     * @throws IOException
     */
    public ListenerThread(InetSocketAddress address) throws IOException {
        acceptChannel = ServerSocketChannel.open();
        acceptChannel.configureBlocking(false);
        acceptChannel.socket().bind(address);
        
        acceptSelector = Selector.open();
        acceptChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
        readSelector = Selector.open();
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     * Starts an event loop on the main thread where we:
     * 1) Continuously poll of incoming connections.
     * 2) Register read-ready channel when a connection is established.
     */
    public void run() {
        try {
            while(true) {
                acceptSelector.select();
                Set<SelectionKey> selectedKeys = acceptSelector.selectedKeys();
    
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
    
                while(keyIterator.hasNext()) {
                    keyIterator.next();
                    acceptConnection();
                    keyIterator.remove();
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
    
    /**
     * Getter for read-events selector. Threads can check this selector
     * to see if there are any channels that are ready to send data.
     * @return a reference to the thread's read-events selector.
     */
    public Selector getReadSelector() {
        return readSelector;
    }

    /**
     * Helper function to accept an incoming connection in non-blocking mode
     * @throws IOException
     */
    private void acceptConnection() throws IOException {
        SocketChannel clientChannel = acceptChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(readSelector, SelectionKey.OP_READ);
    }
}
