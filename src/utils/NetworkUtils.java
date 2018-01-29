package utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import messages.Message;

/**
 * Handles sending and receiving messages over socket channels
 */
public class NetworkUtils {

    /**
     * size of buffer for reading/writing from/to a server (in bytes)
     * Proj2: currently, we assume all messages are <= 1024 bytes
     * Figure out a better way to determine the right buffer size
     */
    private static final int BUFFER_SIZE = 1024;

    /**
     * @param address      Recipient address
     * @param message      message to be sent
     * @throws IOException
     * Sends a full message from a channel
     * If we receive an IOException while trying to establish a connection
     * (e.g., target server is down), or while writing (e.g., target server goes
     * down while transmitting data), then we throw the error for the method
     * caller to handle
     */
    public static void sendMessage(InetSocketAddress address, Message message) 
        throws IOException {

        /**
         * Create a channel to send message
         */
        SocketChannel socketChannel = SocketChannel.open(address);
        socketChannel.configureBlocking(false);
        
       /**
        * Create a buffer to store serialized message
        */
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        
        buffer.clear();
        buffer.put(ObjectUtils.serializeObject(message));
        buffer.flip();
        while(buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }
        
        socketChannel.close();
    }
    
    /**
     * @param channel      Read a message from channel
     * @param closeChannel Whether to close the channel after reading
     * @return             Message read
     * @throws IOException
     * Reads a full message from a channel
     * Most of the time, we will want to close the channel after
     * reading the full message.
     * If we receive an IOException while reading (e.g., requester goes down 
     * while receiving data), then we throw the error for the method caller to
     * handle
     */
    public static Message receiveMessage(SocketChannel channel, 
        boolean closeChannel) throws IOException {
       /**
        *  Create a buffer to store request data and write the incoming
        *  message to messageBytesStream
        */
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
        /**
         *  Create a byte array to get the stream ready for deserialization
         */
        byte[] messageBytes =  messageBytesStream.toByteArray();
        messageBytesStream.close();
        
        if (closeChannel) {
            channel.close();
        }
        
        return (Message) ObjectUtils.deserializeObject(messageBytes);
    }
}
