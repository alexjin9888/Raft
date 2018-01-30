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
 * A thread that starts up and manages a connection listener channel.
 * It accepts connections and then exposes a selector which can be queried for
 * read-ready events.
 */
public class ListenerThread extends Thread {
    // The channel that will handle incoming messages
    private ServerSocketChannel acceptChannel;
    // Selector that can be queried for accept-ready events
    private Selector acceptSelector;
    // Selector for which we register channels on to later read from
    private Selector readSelector;
    
    /**
     * The constructor creates a channel and listens for incoming connections
     * using this channel. Note that this constructor does not block.
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

    /* 
     * Starts an event loop on the main thread where we:
     * 1) Accept incoming connections as they arrive.
     * 2) Register channels to later read from when a connection is established.
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
     * Helper function to accept an incoming connection in non-blocking mode.
     * It registers the associated channel on the read-events selector.
     * @throws IOException
     */
    private void acceptConnection() throws IOException {
        SocketChannel clientChannel = acceptChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(readSelector, SelectionKey.OP_READ);
    }
}
