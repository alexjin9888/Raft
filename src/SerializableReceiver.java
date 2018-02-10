import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SerializableReceiver {
    
    // Amount of time (in ms) that a read socket is willing to block without
    // receiving any data before timing out and throwing an exception
    private static final int READ_TIMEOUT = 3000;
    
    public interface SerializableHandler {
        public void handleSerializable(Serializable object);
    }
    
    private ServerSocketChannel listenerChannel;
    
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
    
    public SerializableReceiver(InetSocketAddress address, SerializableHandler serializableHandler) {
        try {
            listenerChannel = ServerSocketChannel.open();
            listenerChannel.bind(address);         
        } catch (IOException e) {
            // Throw exceptions that are a subclass of IOException
            // and/or RuntimeException.
            // Maybe it can be a custom subclass.
            // Also, intercept the IOException subclass corresponding to
            // address is in-use error.
            System.exit(1);
        }
        
        threadPoolService = Executors.newCachedThreadPool();
                
        (new Thread() {
            public void run() {
                while(true) {
                    try {
                        SocketChannel peerChannel = listenerChannel.accept();
                        // peerChannel.socket().setSoTimeout(READ_TIMEOUT);
                        threadPoolService.submit(() -> {
                            try {
                                try (ObjectInputStream ois = new ObjectInputStream(peerChannel.socket().getInputStream())) {
                                    // We only exit the while loop below when an
                                    // I/O error or read timeout errors.
                                    while (true) {
                                     // We block until a serializable object is read or an I/O error occurs.
                                        serializableHandler.handleSerializable((Serializable) ois.readObject());
                                    }
                                }
                            } catch (IOException e) {
                                // TODO: think about whether or not to print
                                // any further messages here.
                                myLogger.info(e.getMessage());
                                e.printStackTrace();
                            } catch (ClassNotFoundException e) {
                                // TODO: figure out what to print in the case of
                                // error being of type ClassNotFoundException.
                            }
                            
                            try {
                                peerChannel.close();
                            } catch (IOException e1) {
                                // TODO: We silently ignore the error since
                                // the sender can start a new connection
                                // with us later if they have data to send.
                            }
                        });
                    } catch (IOException e) {
                        // error during accept call (blocking call)
                        // TODO: handle this properly
                        System.exit(1);
                    }
                }
            }
        }).start();
    }
}
