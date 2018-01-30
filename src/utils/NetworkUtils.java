package utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import messages.Message;

/**
 * Handles the sending and receiving of messages over socket channels
 */
public class NetworkUtils {

    /**
     * Size of buffer for reading/writing from/to a server (in bytes)
     * Proj2: currently, all messages are <= 1024 bytes. Figure out a better
     * way to determine the right buffer size once we start sending log entries
     * with our messages.
     */
    private static final int BUFFER_SIZE = 1024;

    /**
     * Sends a full message to the specified address.
     * If we receive an IOException while 
     * @param address      Recipient address
     * @param message      message to be sent
     * @throws IOException May be thrown while trying to establish a
     *                     connection (e.g., recipient server is down), or
     *                     while writing (e.g., recipient server goes down
     *                     while we're transmitting data to them)
     */
    public static void sendMessage(InetSocketAddress address, Message message) 
        throws IOException {

        SocketChannel socketChannel = SocketChannel.open(address);
        socketChannel.configureBlocking(false);
        
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
     * Reads a full message from the specified channel.
     * Most of the time, users will want to close the channel after reading
     * the full message.
     * @param channel      Channel from which there may be a message to read.
     * @param closeChannel Whether to close the channel after reading
     * @return             Sender message object
     * @throws IOException May be thrown while reading (e.g., requester goes
     *                     down while transmitting data to us)
     */
    public static Message receiveMessage(SocketChannel channel, 
        boolean closeChannel) throws IOException {
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
        
        if (closeChannel) {
            channel.close();
        }
        
        return (Message) ObjectUtils.deserializeObject(messageBytes);
    }
}
