package utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import messages.RaftMessage;

/**
 * Handles the sending and receiving of messages over socket channels
 */
public abstract class NetworkUtils {

    /**
     * Size of buffer for reading/writing from/to a server (in bytes)
     * Proj2: currently, all messages are <= 1024 bytes. Figure out a better
     * way to determine the right buffer size once we start sending log entries
     * with our messages.
     */
    private static final int BUFFER_SIZE = 1024;

    /**
     * Sends a full serializable object to the specified address.
     * @param address      Recipient address
     * @param message      message to be sent
     * @throws IOException May be thrown while trying to establish a
     *                     connection (e.g., recipient server is down), or
     *                     while writing (e.g., recipient server goes down
     *                     while we're transmitting data to them)
     */
    public static void sendSerializable(InetSocketAddress address, Serializable object) 
        throws IOException {

        SocketChannel socketChannel = SocketChannel.open(address);
        socketChannel.configureBlocking(true);
        
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        
        buffer.clear();
        buffer.put(SerializationUtils.serialize(object));
        buffer.flip();
        while(buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }
        
        // TODO: Figure out where to close the channel that we opened above in
        // event that an exception is thrown before the next line is run.
        socketChannel.close();
    }
    
    /**
     * Reads a full serializable object from the specified channel.
     * @param channel      Channel from which there may be a message to read.
     * @throws IOException May be thrown while reading (e.g., requester goes
     *                     down while transmitting data to us)
     */
    public static Serializable receiveSerializable(SocketChannel channel)
            throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        ByteArrayOutputStream messageBytesStream = new ByteArrayOutputStream();

        int bytesRead = channel.read(buffer);
        while (bytesRead > 0) {
            buffer.flip();
            while(buffer.hasRemaining()) {
                messageBytesStream.write(buffer.get());
            }
            buffer.clear();
            bytesRead = channel.read(buffer);
        }
        messageBytesStream.flush();

        byte[] messageBytes =  messageBytesStream.toByteArray();
        messageBytesStream.close();
        
        return SerializationUtils.deserialize(messageBytes);
    }
}
