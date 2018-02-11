import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import utils.ObjectUtils;

public class SerializableSender {
    
    private Map<InetSocketAddress, SocketOOSTuple> addressToSocketOOS;
    
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
        addressToSocketOOS = Collections.synchronizedMap(new HashMap<InetSocketAddress, SocketOOSTuple>());        
        threadPoolService = Executors.newCachedThreadPool();
    }

    public void send(InetSocketAddress recipientAddress, Serializable object) {        
        // create a serialized copy of the object before we send the
        // serialized copy to the recipient in another thread.
        // This ensures that no one modifies the object while sending.
        Serializable objectCopy = ObjectUtils.deepClone(object);
        
        threadPoolService.submit(() -> {
            Socket socket = null;
            ObjectOutputStream oos = null;
            SocketOOSTuple socketOOSTuple = null;
            try {
                synchronized(addressToSocketOOS) {
                    socketOOSTuple = addressToSocketOOS.get(recipientAddress);
                    if (socketOOSTuple == null) {
                        socket = new Socket();
                        socket.connect(recipientAddress);
                        oos = new ObjectOutputStream(socket.getOutputStream());
                        socketOOSTuple = new SocketOOSTuple(socket, oos);
                        addressToSocketOOS.put(recipientAddress, socketOOSTuple);
                    }
                }
                socket = socketOOSTuple.socket;
                oos = socketOOSTuple.oos;
                oos.writeObject(objectCopy);
                myLogger.info("Sent " + objectCopy.toString() + " to " + recipientAddress);
            } catch (IOException e) {
                myLogger.info("Failed to send " + objectCopy.toString() + " to " + recipientAddress);
                try {
                    if (socket != null) {
                        socket.close();
                    }
                    if (oos != null) {
                        oos.close();
                    }
                } catch (IOException e1) {
                    // We silently ignore the error since we already report
                    // the failed sending above.
                }
                addressToSocketOOS.remove(recipientAddress);
            }
        }); 
    }
    
    public class SocketOOSTuple {
        Socket socket;
        ObjectOutputStream oos;
        public SocketOOSTuple(Socket socket, ObjectOutputStream oos) {
            this.socket = socket;
            this.oos = oos;
        }
    }
}
