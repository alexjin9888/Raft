package singletons;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.Iterator;

public class ListenerThread extends Thread {
    private ServerSocketChannel acceptChannel;
    private Selector acceptSelector;
    // register channels to read from using this selector
    private Selector readSelector;
    
    public ListenerThread(InetSocketAddress address) throws IOException {
        acceptChannel = ServerSocketChannel.open();
        acceptChannel.configureBlocking(false);
        acceptChannel.socket().bind(address);
        
        acceptSelector = Selector.open();
        acceptChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
        readSelector = Selector.open();
    }

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

    // Helper function to accept an incoming connection in non-blocking mode
    // We then get ready to read the incoming message
    private void acceptConnection() throws IOException {
        SocketChannel clientChannel = acceptChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(readSelector, SelectionKey.OP_READ);
    }
}
