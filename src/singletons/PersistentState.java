package singletons;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import misc.LogEntry;
import utils.ObjectUtils;

/* 
 * A class that manages the persistent state of a server  
 */
@SuppressWarnings("serial")
public class PersistentState implements Serializable {
    private static final String BASE_LOG_DIR = System.getProperty("user.dir").toString();
    private static final String LOG_EXT = ".log";
    
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

    // Class constructor
    // Creates an object that manages persistent state for server with ID=myId
    // During instantiation, it will do 1 of 2 things:
    //   1) If persistent state file exists, load it
    //   2) Otherwise initialize persistent state variables
    public PersistentState(String myId) throws IOException {
        this.myId = myId;
        if (!existsState()) {
            this.currentTerm = 0;
            this.votedFor = null;
            this.log = new ArrayList<LogEntry>();
        } else {
            load();
        }
    }

    // Writes to file the current persistent state of this server
    public void save() throws IOException {
        Files.write(Paths.get(BASE_LOG_DIR, myId + LOG_EXT), ObjectUtils.serializeObject(this));
    }

    // Load from file the persistent state of this server
    //  * invariant: the file with persistent state exists
    private void load() throws IOException {
        byte[] persistentStateBytes = Files.readAllBytes(Paths.get(BASE_LOG_DIR, myId + LOG_EXT));

        PersistentState loadedPersistentState = (PersistentState) ObjectUtils.deserializeObject(persistentStateBytes);
        this.currentTerm = loadedPersistentState.currentTerm;
        this.votedFor = loadedPersistentState.votedFor;
        this.log = loadedPersistentState.log;
    }
    
    // Check to see if there is a file on disk corresponding to the server's
    // persistent state
    private boolean existsState() {
        return Files.exists(Paths.get(BASE_LOG_DIR, myId + LOG_EXT));
    }
}
