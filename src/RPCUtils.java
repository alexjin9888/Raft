import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class RPCUtils {
    static void sendAppendEntriesRPC(InetSocketAddress destAddress, int term,
            int leaderId, int prevLogIndex, int prevLogTerm, String[] entries,
            int leaderCommit) {
        AppendEntriesRequest message = new AppendEntriesRequest(term, leaderId,
                prevLogIndex, prevLogTerm, entries, leaderCommit);
        sendRPC(destAddress, message);
    }
    
    private static void sendRPC(InetSocketAddress destAddress, Object message) {
        //TODO how to allocate enough bytes
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] messageByteArray = null;
        SocketChannel socketChannel = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(message);
            out.flush();
            messageByteArray = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            socketChannel = SocketChannel.open(destAddress);
            socketChannel.configureBlocking(false);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        buffer.clear();
        buffer.put(messageByteArray);
        buffer.flip();
        while(buffer.hasRemaining()) {
            try {
                socketChannel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
