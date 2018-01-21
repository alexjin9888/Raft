import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/* 
 * A class that manages the persistent state of a server  
 */
public class PersistentState {
    private String myId; // Unique identification (Id) per server
    // Persistent State
    // * Latest term server has seen (initialized to 0 on first boot, increases
    //   monotonically)
    private int currentTerm;
    // * candidateId that received vote in current term (or null if none)
    private String votedFor;
    // * log entries; each entry contains command for state machine, and term
    //   when entry was received by leader (first index is 0)
    private List<LogEntry> log;

    private static final String BASE_LOG_DIR = "./ServerLogs/";
    private static final String LOG_EXT = ".log";

    // size of buffer for reading/writing from/to a server (in bytes)
    private static final int BUFFER_SIZE = 1024;

    // Class constructor
    // Creates an object that manages persistent state for server with ID=myId
    // During instantiation, it will do 1 of 2 things:
    //   1) If persistent state file exists, load it
    //   2) Otherwise initialize persistent state variables
    public PersistentState(String myId) {
    }

    // Check to see if there is a file on disk corresponding to the server's
    // persistent state
    public boolean checkStorageExists() {
        return Files.exists(Paths.get(BASE_LOG_DIR + myId + LOG_EXT));
    }

    // Load from file the persistent state of this server
    //  * invariant: the file with persistent state exists
    public void load() {

    }

    // Writes to file the current persistent state of this server
    public void save() throws IOException {
        // FileChannel fileChannel = FileChannel.open(Paths.get(BASE_LOG_DIR + myId + LOG_EXT));
        // fileChannel.configureBlocking(false);
        
        // ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        byte[] stateByteArray = null;
        out = new ObjectOutputStream(bos);
        out.writeObject(this);
        out.flush();
        stateByteArray = bos.toByteArray();
        out.close();
        bos.close();

        // FileOutputStream stream = new FileOutputStream(path);
        // try {
        //  stream.write(bytes);
        // } finally {
        //  stream.close();
        // }
        Files.write(Paths.get(BASE_LOG_DIR + myId + LOG_EXT), stateByteArray);


        // buffer.clear();
        // buffer.put(messageByteArray);
        // buffer.flip();
        // while(buffer.hasRemaining()) {
        //     fileChannel.write(buffer);
        // }
        
        // fileChannel.close();
    }
    // public static Message receiveMessage(SocketChannel channel, boolean closeChannel) throws IOException {
    //     Object message = null;
    //     // Create a buffer to store request data
    //     ByteBuffer buffer = ByteBuffer.allocate(1024);
    //     ByteArrayOutputStream messageBytes = new ByteArrayOutputStream();

    //     int bytesRead = channel.read(buffer);
    //     while (bytesRead > 0) {
    //         buffer.flip();
    //         while(buffer.hasRemaining()) {
    //             messageBytes.write(buffer.get());
    //         }
    //         buffer.clear();
    //         bytesRead = channel.read(buffer);
    //     }
    //     messageBytes.flush();
    //     ByteArrayInputStream bis = new ByteArrayInputStream(messageBytes.toByteArray());
    //     messageBytes.close();

    //     ObjectInputStream in = new ObjectInputStream(bis);
    //     try {
    //         message = in.readObject();
    //     } catch (ClassNotFoundException e) {
    //         e.printStackTrace();
    //     }

    //     in.close();
    //     bis.close();
        
    //     if (closeChannel) {
    //         channel.close();
    //     }
        
    //     return (Message) message;
    // }
}
