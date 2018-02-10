import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import utils.ObjectUtils;

public class SerializableSender {
    
    private HashMap<InetSocketAddress, SocketChannel> addressToPeerChannelMap;
    
    /**
     * ExecutorService instance manages a thread pool for us, which
     * we use to send concurrent requests to other servers.
     */
    private ExecutorService threadPoolService;
    
    /**
     * Tracing and debugging logger;
     * see: https://logging.apache.org/log4j/2.x/manual/api.html
     */
    private static final Logger myLogger = LogManager.getLogger();
    
    
    public SerializableSender() {
        addressToPeerChannelMap = new HashMap<InetSocketAddress, SocketChannel>();        
        threadPoolService = Executors.newCachedThreadPool();
    }

    public void send(InetSocketAddress recipientAddress, Serializable object) {        
        // create a serialized copy of the object before we send the
        // serialized copy to the recipient in another thread.
        // This ensures that no one modifies the object while sending.
        Serializable objectCopy = ObjectUtils.deepClone(object);
        
        threadPoolService.submit(() -> {
            SocketChannel peerChannel = null;
            try {
                synchronized(addressToPeerChannelMap) {
                    peerChannel = addressToPeerChannelMap.get(recipientAddress);

                    if (peerChannel != null && !peerChannel.isConnected()) {
                        addressToPeerChannelMap.remove(recipientAddress);
                        peerChannel = null;
                    }

                    if (peerChannel == null) {
                        peerChannel = SocketChannel.open(recipientAddress);
                        addressToPeerChannelMap.put(recipientAddress, peerChannel);
                    }
                }
                try (ObjectOutputStream oos = new ObjectOutputStream(peerChannel.socket().getOutputStream())) {
                    oos.writeObject(objectCopy);
                    myLogger.info("Sent " + objectCopy.toString() + " to " + recipientAddress);
                }
            } catch (IOException e) {
                myLogger.info("Failed to send " + objectCopy.toString() + " to " + recipientAddress);
                if (peerChannel != null) {
                    try {
                        peerChannel.close();
                    } catch (IOException e1) {
                        // We silently ignore the error since we already report the failed sending above.
                    }
                }
            }
        }); 
    }
}
