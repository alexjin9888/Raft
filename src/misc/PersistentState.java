package misc;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Manages the persistent state of a Raft server.
 */
public class PersistentState {
    
    private static final String VOTED_FOR_SENTINEL_VALUE = "null";
    
    // Extension used for persistent state files
    private static final String PS_EXT = ".log";
    
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
    public ArrayList<LogEntry> log;
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
     * List that maintains the running cumulative length of logs
     */
    private ArrayList<Integer> runningLogLengths;

    
    private Path currentTermPath;
    private Path votedForPath;
    private Path lastAppliedPath;    
    private RandomAccessFile logFile;

    
    
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
        this.runningLogLengths = new ArrayList<Integer>();
        
        Path baseDirPath = Paths.get(System.getProperty("user.dir"), myId);
        currentTermPath = Paths.get(baseDirPath.toString(), "current-term" + PS_EXT);
        votedForPath = Paths.get(baseDirPath.toString(), "voted-for" + PS_EXT);
        lastAppliedPath = Paths.get(baseDirPath.toString(), "last-applied" + PS_EXT);
        Path logFilePath = Paths.get(baseDirPath.toString(), "log-entries" + PS_EXT);
     
        if (Files.isDirectory(baseDirPath)) {
            try {
                currentTerm = Integer.parseInt(Files.readAllLines(currentTermPath).get(0));
                String votedForReadValue = Files.readAllLines(votedForPath).get(0);
                votedFor = votedForReadValue.equals(VOTED_FOR_SENTINEL_VALUE) ? null : votedForReadValue;
                lastApplied = Integer.parseInt(Files.readAllLines(lastAppliedPath).get(0));
                
                logFile = new RandomAccessFile(logFilePath.toString(), "rw");
                String line;
                int runningLogLength = 0;
                while ((line = logFile.readLine()) != null) {
                    LogEntry logEntry = new LogEntry(line);
                    log.add(logEntry);
                    runningLogLength += (logEntry.toString() + "\n").getBytes().length;
                    runningLogLengths.add(runningLogLength);
                }
            } catch (IOException | NumberFormatException e) {
                // TODO: add more logging
                // e.g., consider catching on FileNotFoundException
                throw new PersistentStateException("Cannot successfully load "
                        + "persistent state from path: " + baseDirPath + "."
                                + "Received error: " + e);
            }
        } else {
            try {
                Files.createDirectories(baseDirPath);
                overwriteFileContents(currentTermPath, Integer.toString(currentTerm));
                overwriteFileContents(votedForPath, VOTED_FOR_SENTINEL_VALUE + votedFor);
                overwriteFileContents(lastAppliedPath, Integer.toString(lastApplied));
                overwriteFileContents(logFilePath, stringifyLogs(log));
                
                logFile = new RandomAccessFile(logFilePath.toString(), "rw");
            } catch (IOException e) {
                throw new PersistentStateException("Cannot create persistent "
                        + "state for specified path: " + baseDirPath + "."
                                + "Received error: " + e);
            }


        }
    }
    

    private String stringifyLogs(ArrayList<LogEntry> logEntries) {
        return logEntries.stream()
                         .map(LogEntry::toString)
                         .collect(Collectors.joining("\n"));
    }

    
    /**
     * Precondition: Cannot write `null` to file.
     * TODO: see whether the precondition still holds with `Files.write`.
     * @param data
     * @param file
     * @param append
     */
    private synchronized void overwriteFileContents(Path filePath, String contents) {
        try {
            // TODO: make sure the output is human-readable
            Files.write(filePath, contents.getBytes());
        } catch (IOException e) {
            throw new PersistentStateException("Cannot persist to file path: "
                    + filePath + ". Received error: " + e);
        }
    }

    /**
     * Set current term and then write to persistent state on disk.
     * @param currentTerm See top of class file
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void setTerm(int currentTerm) {
        this.currentTerm = currentTerm;
        overwriteFileContents(currentTermPath, Integer.toString(currentTerm));
    }
    
    public boolean logHasIndex(int index) {
        return index >= 0 && index < this.log.size();
    }

    /**
     * Set id of server you voted for and then write to persistent state on disk
     * @param votedFor See top of class file
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
        overwriteFileContents(votedForPath, this.votedFor == null ? VOTED_FOR_SENTINEL_VALUE : this.votedFor);
    }
    
    /**
     * Truncate the log starting at specified index (inclusive).
     * Persist updated portion of log state to disk.
     * @param index start location of where we truncate
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void truncateAt(int index) {
        if (!logHasIndex(index)) {
            return;
        }
        
        try {
            logFile.setLength(index == 0 ? 0 : runningLogLengths.get(index - 1));
        } catch (IOException e) {
            throw new PersistentStateException("Cannot truncate logs. Received "
                    + "error: " + e);
        }

        this.log.subList(index, this.log.size()).clear();
    }
    
    /**
     * Append new entries to log.
     * Persist updated portion of log state to disk.
     * @param newEntry log entry to be appended
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void appendLogEntries(ArrayList<LogEntry> newEntries) {
        this.log.addAll(newEntries);
        
        int currentLogLength = runningLogLengths.size() == 0 ? 0 : runningLogLengths.get(runningLogLengths.size() - 1);
        
        
        
        for (LogEntry logEntry : newEntries) {
            byte[] logEntryBytes = (logEntry.toString() + "\n").getBytes();
            runningLogLengths.add(currentLogLength + logEntryBytes.length);
            try {
                logFile.write(logEntryBytes);
                // `RandomAccessFile` doesn't maintain a buffer, so we don't need
                // to flush.
            } catch (IOException e) {
                // TODO: more specific exception catch?
                throw new PersistentStateException("Cannot persist logs to "
                        + "disk. Received error: " + e);
            }
        }    
    }

    /**
     * Increment the last applied index variable and persist state to disk.
     */
    public synchronized void incrementLastApplied() {
        this.lastApplied += 1;
        overwriteFileContents(lastAppliedPath, Integer.toString(lastApplied));
    }
}
