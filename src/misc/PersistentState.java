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
 * Manages the persistent state of a Raft server.
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
     * index of highest log entry applied to state machine
     * (initialized to -1, increases monotonically)
     */
     public int lastApplied;
        
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
    public PersistentState(String myId) {
        this.myId = myId;
        this.currentTerm = 0;
        this.votedFor = null;
        this.lastApplied = -1;
        this.log = new ArrayList<LogEntry>();
        
        // A2DO: load list of log entries from disk using the second LogEntry
        // constructor. See LogEntry.java for details.
    }

    /**
     * Set current term and then write to persistent state on disk.
     * @param currentTerm See top of class file
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void setTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    /**
     * Set id of server you voted for and then write to persistent state on disk
     * @param votedFor See top of class file
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }
    
    /**
     * Truncate the log starting at specified index (inclusive).
     * Persist updated portion of log state to disk.
     * @param index start location of where we truncate
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void truncateAt(int index) {
        // A2DO: if we truncate at an index that is NOT a valid idx, don't do
        // anything (e.g., don't do any disk I/O) and return.
        
        this.log.subList(index, this.log.size()).clear();
    }
    
    /**
     * Append new entries to log.
     * Persist updated portion of log state to disk.
     * @param newEntry log entry to be appended
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void appendLogEntries(ArrayList<LogEntry> newEntries) {
        // A2DO: make sure that this function handles things efficiently in the
        // case that the caller passes in an empty list.

        this.log.addAll(newEntries);
    }

    /**
     * Increment the last applied index variable and persist state to disk.
     */
    public synchronized void incrementLastApplied() {
        this.lastApplied += 1;
    }
}
