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
 * Each Raft server will instantiate an instance of this class to persist
 * data to disk. Methods in this class ensures atomic changes to the server's
 * persistent state and allows for recovery after crashes.
 */
public class PersistentState {
    
    /**
     * Value representing that the server has not voted for anyone for the
     * current election term.
     */
    private static final String VOTED_FOR_SENTINEL_VALUE = "null";
    /**
     * Extension used for persistent state files
     */
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
     * List that maintains the running log size in bytes
     */
    private ArrayList<Integer> runningLogSizeInBytes;

    /**
     * Path to the file that contains the value of currentTerm.
     */
    private Path currentTermPath;
    /**
     * Path to the file that contains the value of votedFor.
     */
    private Path votedForPath;
    /**
     * Path to the file that contains the value of lastApplied.
     */
    private Path lastAppliedPath;    
    /**
     * A RandomAccessFile instance that points to the file containing all
     * log entries.
     */
    private RandomAccessFile logFile;

    /**
     * Attempt to load persistent state data from disk.
     * If the state does not exist on disk, initialize with default values.
     * @param myId See top of class file.
     */
    public PersistentState(String myId) {
        this.currentTerm = 0;
        this.votedFor = null;
        this.lastApplied = -1;
        this.log = new ArrayList<LogEntry>();
        this.runningLogSizeInBytes = new ArrayList<Integer>();
        
        Path baseDirPath = Paths.get(System.getProperty("user.dir"), myId);
        currentTermPath = Paths.get(baseDirPath.toString(), "current-term" + PS_EXT);
        votedForPath = Paths.get(baseDirPath.toString(), "voted-for" + PS_EXT);
        lastAppliedPath = Paths.get(baseDirPath.toString(), "last-applied" + PS_EXT);
        Path logFilePath = Paths.get(baseDirPath.toString(), "log-entries" + PS_EXT);
     
        // Testing for existence of server persistent state directory is
        // sufficient to tell us whether persistent state for Raft server
        // already exists. If the directory is missing a file, then we treat
        // that as the Raft server's persistent state being corrupted.

        if (Files.isDirectory(baseDirPath)) {
            try {
                currentTerm = Integer.parseInt(Files.readAllLines(currentTermPath).get(0));
                String votedForReadValue = Files.readAllLines(votedForPath).get(0);
                votedFor = votedForReadValue.equals(VOTED_FOR_SENTINEL_VALUE) ? null : votedForReadValue;                
                lastApplied = Integer.parseInt(Files.readAllLines(lastAppliedPath).get(0));
                
                logFile = new RandomAccessFile(logFilePath.toString(), "rw");
                String line;
                int logSizeInBytes = 0;
                while ((line = logFile.readLine()) != null) {
                    LogEntry logEntry = new LogEntry(line);
                    log.add(logEntry);
                    logSizeInBytes += (logEntry.toString() + "\n").getBytes().length;
                    runningLogSizeInBytes.add(logSizeInBytes);
                }
            } catch (IOException | NumberFormatException e) {
                throw new PersistentStateException("Cannot successfully load "
                        + "persistent state from path: " + baseDirPath + "."
                                + "Received exception: " + e);
            }
        } else {
            try {
                Files.createDirectories(baseDirPath);
                writeOutFileContents(currentTermPath, Integer.toString(currentTerm));
                writeOutFileContents(votedForPath, VOTED_FOR_SENTINEL_VALUE);
                writeOutFileContents(lastAppliedPath, Integer.toString(lastApplied));
                writeOutFileContents(logFilePath, "");
                
                logFile = new RandomAccessFile(logFilePath.toString(), "rw");
            } catch (IOException e) {
                throw new PersistentStateException("Cannot successfully create"
                        + "persistent state for specified path: " + baseDirPath
                        + ".Received exception: " + e);
            }


        }
    }
    
    /**
     * Writes a string to disk. Throws PersistentStateException upon failure
     * because our persistent state is no longer consistent.
     * Precondition: Cannot write out `null` to file. Some non-null sentinel
     * value has to take its place instead.
     * @param filePath Path to which we write the string.
     * @param contents String to write.
     */
    private synchronized void writeOutFileContents(Path filePath, String contents) {
        try {
            Files.write(filePath, contents.getBytes());
        } catch (IOException e) {
            throw new PersistentStateException("Cannot persist state to file"
                    + "path: " + filePath + ". Received exception: " + e);
        }
    }

    /**
     * Set current term and then write to persistent state on disk.
     * @param currentTerm See top of class file
     */
    public synchronized void setTerm(int currentTerm) {
        this.currentTerm = currentTerm;
        writeOutFileContents(currentTermPath, Integer.toString(currentTerm));
    }
    
    /**
     * Tells you whether there exists a log entry with the specified index.
     * @param index specified log entry index to check existence of.
     * @return true iff our log contains a log entry at the given index.
     */
    public boolean logHasIndex(int index) {
        return index >= 0 && index < this.log.size();
    }

    /**
     * Set id of server you voted for and then write to persistent state on disk
     * @param votedFor See top of class file
     */
    public synchronized void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
        writeOutFileContents(votedForPath, this.votedFor == null ? VOTED_FOR_SENTINEL_VALUE : this.votedFor);
    }
    
    /**
     * Increment the last applied index variable and persist state to disk.
     */
    public synchronized void incrementLastApplied() {
        this.lastApplied += 1;
        writeOutFileContents(lastAppliedPath, Integer.toString(lastApplied));
    }
    
    /**
     * Truncate the log starting at specified index (inclusive).
     * Persist updated portion of log state to disk.
     * @param index start location of where we truncate
     */
    public synchronized void truncateAt(int index) {
        if (!logHasIndex(index)) {
            return;
        }
        
        try {
            logFile.setLength(index == 0 ? 0 : runningLogSizeInBytes.get(index - 1));
        } catch (IOException e) {
            throw new PersistentStateException("Cannot perform truncate "
                    + "procedure on logs. Received exception: " + e);
        }

        this.log.subList(index, this.log.size()).clear();
    }
    
    /**
     * Append new entries to log.
     * Persist updated portion of log state to disk.
     * @param newEntry log entry to be appended
     */
    public synchronized void appendLogEntries(ArrayList<LogEntry> newEntries) {
        
        if (newEntries.size() == 0) {
            return;
        }
        
        this.log.addAll(newEntries);
        
        int currentLogSize = runningLogSizeInBytes.size() == 0 ? 0 : runningLogSizeInBytes.get(runningLogSizeInBytes.size() - 1);
        String stringifiedLogEntries = "";

        for (LogEntry logEntry : newEntries) {
            String stringifiedLogEntry = logEntry.toString() + "\n";
            stringifiedLogEntries += stringifiedLogEntry;
            runningLogSizeInBytes.add(currentLogSize +
                    stringifiedLogEntry.getBytes().length);
        }

        try {
            logFile.write(stringifiedLogEntries.getBytes());
            // `RandomAccessFile` doesn't maintain a buffer, so we don't
            // need to flush.
        } catch (IOException e) {
            throw new PersistentStateException("Cannot persist new log entries"
                    + " to disk. Received exception: " + e);
        }
    }
}
