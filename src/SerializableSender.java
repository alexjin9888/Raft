import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import misc.CheckingCancelTimerTask;
import utils.ObjectUtils;

public class SerializableSender {
        
    class SocketInfo {
        InetSocketAddress address;
        Socket socket;
        ObjectOutputStream oos;
        CheckingCancelTimerTask removeSocketTask;
    }
    private HashMap<InetSocketAddress, SocketInfo> addressToSocketInfo;
    
    /**
     * ExecutorService instance manages a thread pool for us, which
     * we use to send concurrent requests to other servers.
     */
    private ExecutorService threadPoolService;
    
    private Timer myTimer;
    
    /**
     * Tracing and debugging logger;
     * see: https://logging.apache.org/log4j/2.x/manual/api.html
     */
    private static final Logger myLogger = LogManager.getLogger();
    
    
    public SerializableSender() {
        addressToSocketInfo = new HashMap<InetSocketAddress, SocketInfo>();
        myTimer = new Timer();
        threadPoolService = Executors.newCachedThreadPool();
    }
    
    public synchronized void send(InetSocketAddress recipientAddress, Serializable object) {
        // Assumes that the receiver uses one object input stream for
        // the lifetime of their corresponding socket, which is
        // generally the convention for persistent connections when
        // messages come in the form of serializable objects.
        
        // Create a serialized copy of the object before we send the
        // serialized copy to the recipient in another thread.
        // This ensures that no one modifies the object while the object
        // is queued for sending.
        Serializable objectCopy = ObjectUtils.deepClone(object);
        SocketInfo socketInfo = addressToSocketInfo.get(recipientAddress);
        if (socketInfo == null) {
            socketInfo = new SocketInfo();
            addressToSocketInfo.put(recipientAddress, socketInfo);
            socketInfo.address = recipientAddress;
            socketInfo.socket = new Socket();
            try {
                socketInfo.socket.connect(recipientAddress);
                socketInfo.oos = new ObjectOutputStream(socketInfo.socket.getOutputStream());
                rescheduleRemoveSocketTask(socketInfo);
            } catch (IOException e) {
                myLogger.info("Failed to send " + object.toString() + " to " + recipientAddress);
                removeSocket(recipientAddress);
                return;
            }
        }
        final SocketInfo writeSocketInfo = socketInfo; 
        threadPoolService.submit(() -> {
            try {
                writeSocketInfo.oos.writeObject(objectCopy);
                rescheduleRemoveSocketTask(writeSocketInfo);
            } catch (IOException e) {
                myLogger.info("Failed to send " + object.toString() + " to " + recipientAddress);
                removeSocket(recipientAddress);
            }
        });
    }
    
    /**
     * Reschedules (or schedules) the remove socket task. 
     * TODO: finish writing comments for this method.
     * @param socketInfo
     */
    private synchronized void rescheduleRemoveSocketTask(SocketInfo socketInfo) {
        if (socketInfo.removeSocketTask != null) {
            socketInfo.removeSocketTask.cancel();
        }
        
        socketInfo.removeSocketTask = new CheckingCancelTimerTask() {
            public void run() {
                synchronized(SerializableSender.this) {
                    if (this.isCancelled) {
                        return;
                    }
                    removeSocket(socketInfo.address);
                }
            }
        };
        // TODO: change the number to READ_WRITE_TIMEOUT
        myTimer.schedule(socketInfo.removeSocketTask, 5000);
    }
    
    private synchronized void removeSocket(InetSocketAddress address) {
        SocketInfo socketInfo = addressToSocketInfo.get(address);
        if (socketInfo == null) {
            return;
        }
        addressToSocketInfo.remove(address);
        
        if (socketInfo.removeSocketTask != null) {
            socketInfo.removeSocketTask.cancel();
        }
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
