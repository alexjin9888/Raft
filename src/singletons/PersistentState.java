package singletons;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import units.LogEntry;
import utils.SerializationUtils;

/**
 * An instance of this class is used to manage the persistent state of a server
 */
public class PersistentState implements Serializable {
    
    // term of current leader
    public int currentTerm;
    // serverID that this server voted for during the current election term
    public String votedFor;
    // List of the server's log entries
    public List<LogEntry> log;
        
    // Unique identification (Id) of server
    private String myId;
    
    /**
     * In-memory object that mirrors the persistent state on disk.
     * Facilitates making sure that we write to disk only if a dirty write
     * to the persistent state in-memory has occurred.
     */
    private PersistentState persistentStateOnDisk;

    /**
     * Class versioning to support instance serialization/deserialization
     */
    private static final long serialVersionUID = 1L;
    
    // Directory used to store the persistent state files
    private static final String BASE_PS_DIR =
        System.getProperty("user.dir").toString();
    // Extension used for persistent state files
    private static final String PS_EXT = ".log";

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
            this.persistentStateOnDisk = null;
        } else {
            load();
        }
    }

    /**
     * Writes to file the current persistent state of this server if the state
     * has changed since the last write to disk.
     * @throws IOException
     */
    // TODO Think about how to modify this function so that it doesn't write
    // the full log to disk during every save (instead, only writes updates).
    public void save() throws IOException {
        if (this.equals(persistentStateOnDisk)) {
            return;
        }
        byte[] persistentStateBytes = SerializationUtils.serialize(this);
        Files.write(Paths.get(BASE_PS_DIR, myId + PS_EXT), persistentStateBytes);
        this.persistentStateOnDisk = (PersistentState) 
                SerializationUtils.deserialize(persistentStateBytes);        
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
            SerializationUtils.deserialize(persistentStateBytes);
        this.currentTerm = loadedPersistentState.currentTerm;
        this.votedFor = loadedPersistentState.votedFor;
        this.log = loadedPersistentState.log;
        
        this.persistentStateOnDisk = (PersistentState) 
                SerializationUtils.deserialize(persistentStateBytes);
    }

    // TODO test that this function works as intended, esp. w.r.t.
    // comparison of the log.
    // TODO consider whether we have to modify this function once we stop
    // writing the full log to disk during every save.
    /**
     * Checks whether or not two persistent states are semantically equivalent.
     * @param obj another persistent state object
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        PersistentState other = (PersistentState) obj;
        
        if (currentTerm != other.currentTerm)
            return false;
        
        if (votedFor == null && other.votedFor != null) {
            return false;
        } else if (!votedFor.equals(other.votedFor))
            return false;
        
        if (!log.equals(other.log)) {
            return false;
        }

        return true;
    }
}
