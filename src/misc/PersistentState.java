package misc;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import units.LogEntry;
import utils.ObjectUtils;

/**
 * An instance of this class is used to manage the persistent state of a server
 */
public class PersistentState implements Serializable {
    
    /**
     * term of current leader
     */
    public int currentTerm;
    /**
     * server ID that this server voted for during the current election term
     */
    public String votedFor;
    /**
     * List of the server's log entries
     */
    public List<LogEntry> log;
        
    /**
     * Unique server identification and lookup ID for persistent state on disk
     */
    private String myId;

    /**
     * Attempt to load persistent state data from disk.
     * If the state does not exist on disk, initialize.
     * @param myId See top of class file
     * @throws PersistentStateException If the load fails for any reason other
     * than persistent state not existing on disk.
     */
    public PersistentState(String myId) throws PersistentStateException {
    }

    /**
     * Set current term and then write to persistent state on disk.
     * @param currentTerm See top of class file
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public void setTerm(int currentTerm) throws PersistentStateException {
        
    }

    /**
     * Set voted-for-ID and then write to persistent state on disk.
     * @param votedFor See top of class file
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public void setVotedFor(String votedFor) throws PersistentStateException {
        
    }
    
    /**
     * Truncate the log starting at specified index (inclusive).
     * Persist updated portion of log state to disk.
     * @param index start location of where we truncate
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public void truncateAtIndex(int index) throws PersistentStateException {
        
    }
    
    /**
     * Append new entry to log.
     * Persist updated portion of log state to disk.
     * @param newEntry log entry to be appended
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public void appendLogEntry(LogEntry newEntry) throws PersistentStateException {
        
    }
}
