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
        
    class SocketInfo {
        Socket socket;
        ObjectOutputStream oos;
    }
    private HashMap<InetSocketAddress, SocketInfo> addressToSocketInfo;
    
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
        addressToSocketInfo = new HashMap<InetSocketAddress, SocketInfo>();        
        threadPoolService = Executors.newCachedThreadPool();
    }
    
    public synchronized void send(InetSocketAddress recipientAddress, Serializable object) {        
        // create a serialized copy of the object before we send the
        // serialized copy to the recipient in another thread.
        // This ensures that no one modifies the object while sending.
        Serializable objectCopy = ObjectUtils.deepClone(object);
        SocketInfo socketInfo = addressToSocketInfo.get(recipientAddress);
        if (socketInfo == null) {
            socketInfo = new SocketInfo();
            addressToSocketInfo.put(recipientAddress, socketInfo);
            socketInfo.socket = new Socket();
            try {
                socketInfo.socket.connect(recipientAddress);
                socketInfo.oos = new ObjectOutputStream(socketInfo.socket.getOutputStream());
                myLogger.info("Now connected to " + recipientAddress);
            } catch (IOException e) {
                processSendFailure(recipientAddress, objectCopy);
                return;
            }
        }
        final SocketInfo recipientSocketInfo = socketInfo; 
        threadPoolService.submit(() -> {
            try {
                recipientSocketInfo.oos.writeObject(objectCopy);
                myLogger.info("Sent " + objectCopy.toString() + " to " + recipientAddress);
            } catch (IOException e) {
                processSendFailure(recipientAddress, objectCopy);
            }
        }); 
    }
    
    private synchronized void processSendFailure(InetSocketAddress recipientAddress, Serializable object) {
        myLogger.info("Failed to send " + object.toString() + " to " + recipientAddress);
        
        SocketInfo socketInfo = addressToSocketInfo.get(recipientAddress);
        if (socketInfo == null) {
            return;
        }
        addressToSocketInfo.remove(recipientAddress);
        try {
            if (socketInfo.oos != null) {
                socketInfo.oos.close();
            }
            if (socketInfo.socket != null) {
                socketInfo.socket.close();
            }
        } catch (IOException e1) {
            // We silently ignore the error since we already report the failed
            // sending above.
        }
    }
}
