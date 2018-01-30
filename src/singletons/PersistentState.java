package singletons;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import units.LogEntry;
import utils.ObjectUtils;

/**
 * An instance of this class is used to manage the persistent state of a server
 */
public class PersistentState implements Serializable {
    
    // Class versioning to support state serialization/deserialization
    private static final long serialVersionUID = 1L;
    
    // Directory used to store the persistent state files
    private static final String BASE_PS_DIR =
        System.getProperty("user.dir").toString();
    // Extension used for persistent state files
    private static final String PS_EXT = ".log";
    
    // Unique identification (Id) of server
    private String myId;
    // term of current leader
    public int currentTerm;
    // serverID that this server voted for during the current election term
    public String votedFor;
    // List of the server's log entries
    public List<LogEntry> log;

    /**
     * Creates an object that manages persistent state for the server.
     * Loads the persistent state of the server if it exists.
     * Otherwise, initializes the persistent state from scratch.
     * @param myId
     * @throws IOException
     */
    public PersistentState(String myId) throws IOException {
        this.myId = myId;
        if (!Files.exists(Paths.get(BASE_PS_DIR, myId + PS_EXT))) {
            this.currentTerm = 0;
            this.votedFor = null;
            this.log = new ArrayList<LogEntry>();
        } else {
            load();
        }
    }

    /**
     * Writes to file the current persistent state of this server
     * @throws IOException
     */
    public void save() throws IOException {
        Files.write(Paths.get(BASE_PS_DIR, myId + PS_EXT), 
            ObjectUtils.serializeObject(this));
    }

    /**
     * Load from file the persistent state of this server.
     * Precondition: the file with persistent state exists.
     * @throws IOException
     */
    private void load() throws IOException {
        byte[] persistentStateBytes = Files.readAllBytes(Paths.get(BASE_PS_DIR,
            myId + PS_EXT));

        PersistentState loadedPersistentState = (PersistentState) 
            ObjectUtils.deserializeObject(persistentStateBytes);
        this.currentTerm = loadedPersistentState.currentTerm;
        this.votedFor = loadedPersistentState.votedFor;
        this.log = loadedPersistentState.log;
    }
}
