import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import misc.CheckingCancelTimerTask;
import utils.ObjectUtils;

public class NetworkManager {
    
    // Amount of time elapsed without reading from or writing to a socket
    // before we decide to close the socket.
    private static final int READ_WRITE_TIMEOUT_MS = 300000;
    
    // Sender-specific attributes and resources
    class WriteSocketInfo {
        InetSocketAddress address;
        Socket socket;
        ObjectOutputStream oos;
        CheckingCancelTimerTask removeSocketTask;
    }
    /**
     * Map that we use to lookup socket info. for sending messages.
     */
    private HashMap<InetSocketAddress, WriteSocketInfo> addrToWriteSocketInfo;
    
    private Timer removeWriteSocketTimer;
    
    // Receiver-specific attributes and resources
    public interface SerializableHandler {
        public void handleSerializable(Serializable object);
    }
    
    private ServerSocket myListenerSocket;
    
    // Shared attributes and resources
    /**
     * Thread pool that we can use to send and receive messages on different
     * threads.
     */
    private ExecutorService threadPoolService;
    
    /**
     * Tracing and debugging logger;
     * see: https://logging.apache.org/log4j/2.x/manual/api.html
     */
    private static final Logger myLogger = LogManager.getLogger();
    
    
    public NetworkManager(InetSocketAddress myAddress, SerializableHandler serializableHandler) {
        addrToWriteSocketInfo = new HashMap<InetSocketAddress, WriteSocketInfo>();
        removeWriteSocketTimer = new Timer();
        
        try {
            myListenerSocket = new ServerSocket();
            myListenerSocket.bind(myAddress);
        } catch (IOException e) {
            // Throw exceptions that are a subclass of IOException
            // and/or RuntimeException.
            // Maybe it can be a custom subclass.
            // Also, intercept the IOException subclass corresponding to
            // address is in-use error.
            myLogger.info(e.getMessage());
            System.exit(1);
        }
        
        threadPoolService = Executors.newCachedThreadPool();

        (new Thread(() -> {
            while(true) {
                try {
                    Socket socket = myListenerSocket.accept();
                    threadPoolService.submit(() -> {
                        // Uses one object input stream for the lifetime of
                        // the socket, which is generally the convention.
                        try (Socket readSocket = socket;
                                InputStream is = readSocket.getInputStream();
                                ObjectInputStream ois = new ObjectInputStream(is)) {
                            readSocket.setSoTimeout(READ_WRITE_TIMEOUT_MS);
                            // We only exit the while loop below when an
                            // I/O error or read timeout errors.
                            while (true) {
                                // We block until a serializable object is read or an I/O error occurs.
                                serializableHandler.handleSerializable((Serializable) ois.readObject());
                            }
                        } catch (SocketTimeoutException|EOFException e) {
                            // Sender stopped talking to us so we close
                            // socket resources and continue.
                        } catch (IOException e) {
                            // TODO: think about whether or not to print
                            // any further messages here.
                            myLogger.info(myAddress + " received the following I/O error message while trying to read: " + e.getMessage());
                            // e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            // TODO: figure out what to print in the case of
                            // error being of type ClassNotFoundException.
                        }
                    });
                } catch (IOException e) {
                    // error during accept call (blocking call)
                    // TODO: handle this properly
                    System.exit(1);
                }
            }
        })).start();
    }
    
    public synchronized void sendSerializable(InetSocketAddress recipientAddress, Serializable object) {
        // Assumes that the receiver uses one object input stream for
        // the lifetime of their corresponding socket.
        
        // Create a serialized copy of the object before we send the
        // serialized copy to the recipient in another thread.
        // This ensures that no one modifies the object while the object
        // is queued for sending.
        Serializable objectCopy = ObjectUtils.deepClone(object);
        WriteSocketInfo socketInfo = addrToWriteSocketInfo.get(recipientAddress);
        if (socketInfo == null) {
            socketInfo = new WriteSocketInfo();
            addrToWriteSocketInfo.put(recipientAddress, socketInfo);
            socketInfo.address = recipientAddress;
            socketInfo.socket = new Socket();
            try {
                socketInfo.socket.connect(recipientAddress);
                socketInfo.oos = new ObjectOutputStream(socketInfo.socket.getOutputStream());
                rescheduleRemoveWriteSocket(socketInfo);
            } catch (IOException e) {
                myLogger.info("Failed to send " + object.toString() + " to " + recipientAddress);
                removeWriteSocket(recipientAddress);
                return;
            }
        }
        final WriteSocketInfo writeSocketInfo = socketInfo; 
        threadPoolService.submit(() -> {
            try {
                writeSocketInfo.oos.writeObject(objectCopy);
                rescheduleRemoveWriteSocket(writeSocketInfo);
            } catch (IOException e) {
                myLogger.info("Failed to send " + object.toString() + " to " + recipientAddress);
                removeWriteSocket(recipientAddress);
            }
        });
    }
    
    /**
     * Reschedules (or schedules) the remove socket task. 
     * TODO: finish writing comments for this method.
     * @param socketInfo
     */
    private synchronized void rescheduleRemoveWriteSocket(WriteSocketInfo socketInfo) {
        if (socketInfo.removeSocketTask != null) {
            socketInfo.removeSocketTask.cancel();
        }
        
        socketInfo.removeSocketTask = new CheckingCancelTimerTask() {
            public void run() {
                synchronized(NetworkManager.this) {
                    if (this.isCancelled) {
                        return;
                    }
                    removeWriteSocket(socketInfo.address);
                }
            }
        };
        removeWriteSocketTimer.schedule(socketInfo.removeSocketTask, READ_WRITE_TIMEOUT_MS);
    }
    
    private synchronized void removeWriteSocket(InetSocketAddress address) {
        WriteSocketInfo socketInfo = addrToWriteSocketInfo.get(address);
        if (socketInfo == null) {
            return;
        }
        addrToWriteSocketInfo.remove(address);
        
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
