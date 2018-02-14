import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import utils.ObjectUtils;

public class SerializableSender {
        
    private HashMap<InetSocketAddress, SocketOOSTuple> addressToSocketOOS;
    
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
        addressToSocketOOS = new HashMap<InetSocketAddress, SocketOOSTuple>();        
        threadPoolService = Executors.newCachedThreadPool();
    }
    
    public synchronized void send(InetSocketAddress recipientAddress, Serializable object) {        
        // create a serialized copy of the object before we send the
        // serialized copy to the recipient in another thread.
        // This ensures that no one modifies the object while sending.
        Serializable objectCopy = ObjectUtils.deepClone(object);
        SocketOOSTuple socketOOSTuple = addressToSocketOOS.get(recipientAddress);
        if (socketOOSTuple == null) {
            socketOOSTuple = new SocketOOSTuple();
            addressToSocketOOS.put(recipientAddress, socketOOSTuple);
            socketOOSTuple.socket = new Socket();
            try {
                socketOOSTuple.socket.connect(recipientAddress);
                socketOOSTuple.oos = new ObjectOutputStream(socketOOSTuple.socket.getOutputStream());
                myLogger.info("Now connected to " + recipientAddress);
            } catch (IOException e) {
                processSendFailure(recipientAddress, objectCopy);
                return;
            }
        }
        final SocketOOSTuple recipientSocketOOSTuple = socketOOSTuple; 
        threadPoolService.submit(() -> {
            try {
                recipientSocketOOSTuple.oos.writeObject(objectCopy);
                myLogger.info("Sent " + objectCopy.toString() + " to " + recipientAddress);
            } catch (IOException e) {
                processSendFailure(recipientAddress, objectCopy);
            }
        }); 
    }
    
    private synchronized void processSendFailure(InetSocketAddress recipientAddress, Serializable object) {
        myLogger.info("Failed to send " + object.toString() + " to " + recipientAddress);
        
        SocketOOSTuple socketOOSTuple = addressToSocketOOS.get(recipientAddress);
        if (socketOOSTuple == null) {
            return;
        }
        addressToSocketOOS.remove(recipientAddress);
        try {
            if (socketOOSTuple.oos != null) {
                socketOOSTuple.oos.close();
            }
            if (socketOOSTuple.socket != null) {
                socketOOSTuple.socket.close();
            }
        } catch (IOException e1) {
            // We silently ignore the error since we already report the failed
            // sending above.
        }
    }
    
    public class SocketOOSTuple {
        Socket socket;
        ObjectOutputStream oos;
    }
}
