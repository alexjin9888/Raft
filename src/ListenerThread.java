import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.Iterator;
import org.apache.logging.log4j.Logger;

public class ListenerThread extends Thread {
    private ServerSocketChannel acceptChannel;
    private Selector acceptSelector;
    public Selector readSelector; // register channels to read from using this selector
    
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
                    SelectionKey key = keyIterator.next();
                    acceptConnection();
                    keyIterator.remove();
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    // Helper function to accept an incoming connection in non-blocking mode
    // We then get ready to read the incoming message
    private void acceptConnection() throws IOException {
        SocketChannel clientChannel = acceptChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(readSelector, SelectionKey.OP_READ);
    }
}
