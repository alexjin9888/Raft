import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class NetworkUtils {

    // size of buffer for reading/writing from/to a server (in bytes)
    // Proj2: currently, we assume all messages are <= 1024 bytes
    // Figure out a better way to determine the right buffer size
    private static final int BUFFER_SIZE = 1024;

    // Reads a full message from a channel
    // Most of the time, we will want to close the channel after
    // reading the full message.
    // If we receive an IOException while reading (e.g., requester goes down while receiving data),
    // then we throw the error for the method caller to handle
    public static Message receiveMessage(SocketChannel channel, boolean closeChannel) throws IOException {
        // Create a buffer to store request data
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

    // Sends a full message from a channel
    // Closes the channel afterwards
    // If we receive an IOException while trying to establish a connection (e.g., target server is down),
    // or while writing (e.g., target server goes down while transmitting data), then we throw the
    // error for the method caller to handle
    public static void sendMessage(InetSocketAddress address, Message message) throws IOException {

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
}
