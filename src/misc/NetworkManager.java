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
import java.net.BindException;
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
 * This class facilitates the sending and receiving of serializable objects.
 */
public class NetworkManager {
    
    /**
     * Amount of time elapsed without reading from or writing to a socket
     * before we decide to close the socket.
     */
    private static final int READ_WRITE_TIMEOUT_MS = 300000;
    
    /* Start of listing of attributes relating to sending */
    
    /**
     * Information container class to facilitate writing to a socket for
     * multiple messages.
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
     * Timer instance used to schedule a removal of write socket.
     */
    private Timer removeWriteSocketTimer;
    
    /* End of listing of attributes relating to sending */
    
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
    


    /**
     * @param myAddress my address for listening for incoming connections
     * @param handleSerializableCb Callback that is called with a valid
     * serializable object any time the network manager receives such an object
     * from a sender.
     * @param ueh Handler for uncaught exceptions that may arise either from
     * fatal errors in network manager operation (NetworkManagerException) or
     * from executing the passed-in `handleSerializableCb` callback.
     */
    public NetworkManager(InetSocketAddress myAddress, Consumer<Serializable> handleSerializableCb, UncaughtExceptionHandler ueh) {
        addrToWriteSocketInfo = new HashMap<InetSocketAddress, WriteSocketInfo>();
        removeWriteSocketTimer = new Timer();

        @SuppressWarnings("resource")
        // We suppress warnings for not closing the listener socket because any
        // I/O Exception involving the listener socket is fatal and causes the
        // listener thread to terminate.
        Thread listenerThread = new Thread(() -> {
            ServerSocket listenerSocket = null;
            try {
                listenerSocket = new ServerSocket();
                listenerSocket.bind(myAddress);
            } catch (BindException e) {
                throw new NetworkManagerException("Failed to bind to address"
                        + ": " + myAddress + ". Received exception: " + e);
            } catch (IOException e) {
                throw new NetworkManagerException("Failed to create listener"
                        + " socket. Received exception: " + e);
            }
            
            while(true) {
                try {
                    Socket socket = listenerSocket.accept();
                    myLogger.debug("Accepted connection from " + socket.getRemoteSocketAddress());
                    networkIOService.execute(() -> {
                        // Uses one object input stream for the lifetime of
                        // the socket, which is generally the convention.
                        try (Socket readSocket = socket;
                                InputStream is = readSocket.getInputStream();
                                ObjectInputStream ois = new ObjectInputStream(is)) {
                            readSocket.setSoTimeout(READ_WRITE_TIMEOUT_MS);
                            // We only exit the while loop below when a read
                            // timeout or some I/O exception occurs.
                            while (true) {
                                handleSerializableCb.accept((Serializable) ois.readObject());
                            }
                        } catch (SocketTimeoutException|EOFException e) {
                            // Sender stopped talking to us so we close
                            // socket resources and continue.
                            myLogger.debug(socket.getRemoteSocketAddress() + 
                                    " has stopped transmitting data to us. "
                                    + "Received exception: " + e);
                        } catch (IOException e) {
                            myLogger.info(myAddress + " received the "
                                    + "following I/O exception while trying to " 
                                    + "read: " + e);
                        } catch (ClassNotFoundException e) {
                            myLogger.info(myAddress + " failed to match the "
                                    + "received data with a known class " +
                                    "template for deserialization. Received"
                                    + " exception: " + e);
                        }
                    });
                } catch (IOException e) {
                    throw new NetworkManagerException(myAddress + " experienced "
                            + "an I/O exception while trying to accept"
                            + "connection(s). Received exception: " + e);
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
     * Assumes that the receiver uses one object input stream for the lifetime
     * of their corresponding socket.
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
                myLogger.debug("Successfully connected to " + recipientAddress);
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
                myLogger.debug("Successfully sent " + object.toString() + " to " + recipientAddress);
                rescheduleRemoveWriteSocket(writeSocketInfo);
            } catch (IOException e) {
                myLogger.info("Failed to send " + object.toString() + " to " + recipientAddress);
                removeWriteSocket(recipientAddress);
            }
        });
    }
    
    /**
     * Reschedules (or schedules) the remove socket task.
     * @param writeSocketInfo Object that contains useful information for
     * scheduling the removal of a socket task.
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
     * Removes all resources associated with a write socket that we used
     * previously for sending one or more messages.
     * @param address Address corresponding to the write socket that we want to
     * remove.
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
        } catch (IOException e1) {
            // We silently ignore the error since we already report the failed
            // sending above (see sendSerializable).
        }
        try {
            if (socketInfo.socket != null) {
                socketInfo.socket.close();
            }
        } catch (IOException e1) {
            // See above
        }
    }
}
