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

public class RPCUtils {

    // Reads a full message from a channel
    // Closes the channel afterwards
    public static Object receiveMessage(SocketChannel channel) throws IOException {
        Object message = null;
        // Create a buffer to store request data
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteArrayOutputStream messageBytes = new ByteArrayOutputStream();

        int bytesRead = channel.read(buffer);
        while (bytesRead > 0) {
            buffer.flip();
            while(buffer.hasRemaining()) {
                messageBytes.write(buffer.get());
            }
            buffer.clear();
            bytesRead = channel.read(buffer);
        }
        messageBytes.flush();
        ByteArrayInputStream bis = new ByteArrayInputStream(messageBytes.toByteArray());
        messageBytes.close();

        ObjectInputStream in = new ObjectInputStream(bis);
        try {
            message = in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        in.close();
        bis.close();
        
        channel.close();
        
        return message;
    }

    public static void sendMessage(InetSocketAddress address, Object message) throws IOException {

        SocketChannel socketChannel = SocketChannel.open(address);
        socketChannel.configureBlocking(false);
        
        //TODO figure how to allocate sufficient bytes
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        byte[] messageByteArray = null;
        out = new ObjectOutputStream(bos);
        out.writeObject(message);
        out.flush();
        messageByteArray = bos.toByteArray();
        out.close();
        bos.close();

        buffer.clear();
        buffer.put(messageByteArray);
        buffer.flip();
        // TODO Consider appropriate behavior if the recipient is down
        // before we write the entire contents of the buffer
        while(buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }
        
        socketChannel.close();
    }
}
