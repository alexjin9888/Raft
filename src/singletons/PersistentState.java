package singletons;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import misc.LogEntry;
import utils.ObjectUtils;

/**
 * An instance of this class is used to manage the persistent state of a server
 * See RAFT figure 2
 */
public class PersistentState implements Serializable {
    
    // Class versioning to support state serialization/deserialization
    private static final long serialVersionUID = 1L;
    // Tells us where logs are stored
    private static final String BASE_LOG_DIR =
        System.getProperty("user.dir").toString();
    private static final String LOG_EXT = ".log";
    
    // Unique identification (Id) per server
    private String myId;
    // term of current leader
    public int currentTerm;
    // serverID that I voted for during the current election term
    public String votedFor;
    // replicated logs on my server
    public List<LogEntry> log;

    /**
     * @param myId
     * @throws IOException
     * Creates an object that manages persistent state for server with ID=myId
     * During instantiation, it will do 1 of 2 things:
     *   1) If persistent state file exists, load it
     *   2) Otherwise initialize persistent state variables
     */
    public PersistentState(String myId) throws IOException {
        this.myId = myId;
        if (!Files.exists(Paths.get(BASE_LOG_DIR, myId + LOG_EXT))) {
            this.currentTerm = 0;
            this.votedFor = null;
            this.log = new ArrayList<LogEntry>();
        } else {
            load();
        }
    }

    /**
     * @throws IOException
     * Writes to file the current persistent state of this server
     */
    public void save() throws IOException {
        Files.write(Paths.get(BASE_LOG_DIR, myId + LOG_EXT), 
            ObjectUtils.serializeObject(this));
    }

    /**
     * @throws IOException
     * Load from file the persistent state of this server
     *  * invariant: the file with persistent state exists
     *     * precondition: the file with persistent state exists
     */
    private void load() throws IOException {
        byte[] persistentStateBytes = Files.readAllBytes(Paths.get(BASE_LOG_DIR,
            myId + LOG_EXT));

        PersistentState loadedPersistentState = (PersistentState) 
            ObjectUtils.deserializeObject(persistentStateBytes);
        this.currentTerm = loadedPersistentState.currentTerm;
        this.votedFor = loadedPersistentState.votedFor;
        this.log = loadedPersistentState.log;
    }
}
