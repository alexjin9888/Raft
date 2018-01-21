import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/* 
 * A class that manages the persistent state of a server  
 */
public class PersistentState implements Serializable {
    private String myId; // Unique identification (Id) per server
    // Persistent State
    // * Latest term server has seen (initialized to 0 on first boot, increases
    //   monotonically)
    public int currentTerm;
    // * candidateId that received vote in current term (or null if none)
    public String votedFor;
    // * log entries; each entry contains command for state machine, and term
    //   when entry was received by leader (first index is 0)
    public List<LogEntry> log;

    private static final String BASE_LOG_DIR = System.getProperty("user.dir").toString();
    private static final String LOG_EXT = ".log";

    // Class constructor
    // Creates an object that manages persistent state for server with ID=myId
    // During instantiation, it will do 1 of 2 things:
    //   1) If persistent state file exists, load it
    //   2) Otherwise initialize persistent state variables
    public PersistentState(String myId) {
        this.myId = myId;
        if (!checkStorageExists()) {
            this.currentTerm = 0;
            this.votedFor = null;
            this.log = new ArrayList<LogEntry>();
        } else {
            try {
                load();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Check to see if there is a file on disk corresponding to the server's
    // persistent state
    public boolean checkStorageExists() {
        return Files.exists(Paths.get(BASE_LOG_DIR, myId + LOG_EXT));
    }

    // Load from file the persistent state of this server
    //  * invariant: the file with persistent state exists
    public void load() throws IOException {
        byte[] persistentStateBytes = Files.readAllBytes(Paths.get(BASE_LOG_DIR, myId + LOG_EXT));

        PersistentState loadedPersistentState = (PersistentState) ObjectUtils.deserializeObject(persistentStateBytes);
        this.currentTerm = loadedPersistentState.currentTerm;
        this.votedFor = loadedPersistentState.votedFor;
        this.log = loadedPersistentState.log;
    }

    // Writes to file the current persistent state of this server
    public void save() throws IOException {
        Files.write(Paths.get(BASE_LOG_DIR, myId + LOG_EXT), ObjectUtils.serializeObject(this));
    }
}
