import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SerializableReceiver {

    // Amount of time that a read socket is willing to block without receiving
    // any data before timing out and throwing an exception
    private static final int READ_TIMEOUT_MS = 20000;

    public interface Handler {
        public void handleSerializable(Serializable object);
    }

    private ServerSocket myListenerSocket;

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

    public SerializableReceiver(InetSocketAddress myAddress, Handler serializableHandler) {
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

        (new Thread() {
            public void run() {
                while(true) {
                    try {
                        Socket socket = myListenerSocket.accept();
                        myLogger.info(myAddress + " accepted connection from " + socket.getInetAddress());
                        threadPoolService.submit(() -> {
                            // Uses one object input stream for the lifetime of
                            // the socket, which is generally the convention.
                            try (Socket peerSocket = socket;
                                    InputStream is = peerSocket.getInputStream();
                                    ObjectInputStream ois = new ObjectInputStream(is)) {
                                peerSocket.setSoTimeout(READ_TIMEOUT_MS);
                                // We only exit the while loop below when an
                                // I/O error or read timeout errors.
                                while (true) {
                                    // We block until a serializable object is read or an I/O error occurs.
                                    serializableHandler.handleSerializable((Serializable) ois.readObject());
                                }
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
            }
        }).start();
    }
}
