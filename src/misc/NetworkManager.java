package misc;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Server and client programs use this class to manager their networking
 * connections to send and receive messages.
 */
public class NetworkManager {
    
    /**
     * Amount of time elapsed without reading from or writing to a socket
     * before we decide to close the socket.
     */
    private static final int READ_WRITE_TIMEOUT_MS = 300000;
    
    /**
     * Sender-specific attributes and resources
     */
    class WriteSocketInfo {
        InetSocketAddress address;
        Socket socket;
        ObjectOutputStream oos;
        CheckingCancelTimerTask removeSocketTask;
    }
    /**
     * Map that we use to lookup socket info for sending messages.
     */
    private HashMap<InetSocketAddress, WriteSocketInfo> addrToWriteSocketInfo;
    
    /**
     * Timer instance used to schedule a removal of write socket after
     * READ_WRITE_TIMEOUT_MS.
     */
    private Timer removeWriteSocketTimer;
    
    // Attributes and resources for both read and write
    /**
     * Thread pool that we can use to send and receive messages on different
     * threads.
     */
    private ExecutorService networkIOService;
    
    /**
     * Tracing and debugging logger;
     * see: https://logging.apache.org/log4j/2.x/manual/api.html
     */
    private static final Logger myLogger = LogManager.getLogger();
    
    
    public NetworkManager(InetSocketAddress myAddress, Consumer<Serializable> handleSerializableCb) {
        this(myAddress, handleSerializableCb, null);
    }
    
    // Only calls the `handleSerializableCb` callback function if there is a
    // valid serializable object received by the server.
    // ERROR2DO: Document that this class throws `NetworkManagerExceptions` in
    // threads that it controls.
    // ERROR2DO: Mention in comments: `ueh` can be used to handle any uncaught
    // exceptions that calling handleSerializableCb might throw. `ueh` may also
    // be used to handle uncaught NetworkManager exceptions when trying to bind
    // to a port or while trying to accept a connection from any other server.
    public NetworkManager(InetSocketAddress myAddress, Consumer<Serializable> handleSerializableCb, UncaughtExceptionHandler ueh) {
        addrToWriteSocketInfo = new HashMap<InetSocketAddress, WriteSocketInfo>();
        removeWriteSocketTimer = new Timer();

        Thread listenerThread = new Thread(() -> {
            ServerSocket listenerSocket = null;
            try {
                listenerSocket = new ServerSocket();
                listenerSocket.bind(myAddress);
            } catch (IllegalArgumentException e) {
                throw new NetworkManagerException("Failed to bind to address"
                        + ": " + myAddress + ". Received error: " + e);
            } catch (IOException e) {
                throw new NetworkManagerException("Failed to create listener"
                        + " socket. Received error: " + e);
            }
            
            while(true) {
                try {
                    Socket socket = listenerSocket.accept();
                    networkIOService.execute(() -> {
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
                                handleSerializableCb.accept((Serializable) ois.readObject());
                            }
                        } catch (SocketTimeoutException|EOFException e) {
                            // Sender stopped talking to us so we close
                            // socket resources and continue.
                            myLogger.info(myAddress +" connection timeout.");
                        } catch (IOException e) {
                            myLogger.info(myAddress + " received the "
                                    + "following I/O error message while "
                                    + "trying to read: " + e);
                        } catch (ClassNotFoundException e) {
                            myLogger.info(myAddress + " failed to determine "
                                    + "the class of a serialized object "
                                    + "while trying to read: " + e);
                        }
                    });
                } catch (IOException e) {
                    throw new NetworkManagerException(myAddress + " failed "
                            + "to accept connections. Received error: " + e);
                }
            }
        });
        
        if (ueh == null) {
            networkIOService = Executors.newCachedThreadPool();
        } else {
            networkIOService = Executors.newCachedThreadPool(new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    final Thread t = new Thread(r);
                    t.setUncaughtExceptionHandler(ueh);
                    return t;
                }
            });
            
            listenerThread.setUncaughtExceptionHandler(ueh);
        }
        
        listenerThread.start();
    }
    
    /**
     * Sends (a copy of) a serializable object to a recipient.
     * Assumes that the receiver uses one object input stream for the
     * lifetime of their corresponding socket.
     * @param recipientAddress Address to send the object to.
     * @param object Object to send.
     */
    public synchronized void sendSerializable(InetSocketAddress recipientAddress, Serializable object) {

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
        networkIOService.execute(() -> {
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
     * @param writeSocketInfo A WriteSocketInfo object that contains the
     * info of a write socket.
     */
    private synchronized void rescheduleRemoveWriteSocket(WriteSocketInfo writeSocketInfo) {
        if (writeSocketInfo.removeSocketTask != null) {
            writeSocketInfo.removeSocketTask.cancel();
        }
        
        writeSocketInfo.removeSocketTask = new CheckingCancelTimerTask() {
            public void run() {
                synchronized(NetworkManager.this) {
                    if (this.isCancelled) {
                        return;
                    }
                    removeWriteSocket(writeSocketInfo.address);
                }
            }
        };
        removeWriteSocketTimer.schedule(writeSocketInfo.removeSocketTask, READ_WRITE_TIMEOUT_MS);
    }
    
    /**
     * Removes the WriteSocketInfo object in the map that contains socket
     * info for sending messages.
     * @param address Address corresponding to a WriteSocketInfo object that
     * we want to remove.
     */
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
            // sending above (see sendSerializable).
        }
    }
}
