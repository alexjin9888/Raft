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
     * Unique directory used to store persistent state on disk
     */
    private String baseDir;

    // TODO comment
    private File persistentStateCurrentTerm;
    private File persistentStateVotedFor;
    private File persistentStateLastApplied;
    private File persistentStateLog;

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
        this.baseDir = "./" + myId;
        if (Files.exists(Paths.get(baseDir))) {
            loadPersistentStateFromDisk();
        } else {
            createNewPersistentStateStorage();
            persistToDisk(Integer.toString(currentTerm),
                    persistentStateCurrentTerm, false);
            persistToDisk(votedFor, persistentStateVotedFor, false);
            persistToDisk(Integer.toString(lastApplied),
                    persistentStateLastApplied, false);
            persistToDisk(stringifyLogs(log),
                    persistentStateLog, false);
        }
    }

    /**
     * Create a new directory and files to store persistent state on disk.
     */
    private synchronized void createNewPersistentStateStorage() {
        try {
            Files.createDirectories(Paths.get(baseDir));
            persistentStateCurrentTerm = new File(baseDir + "/" + "CurrentTerm.log");
            persistentStateCurrentTerm.createNewFile();
            persistentStateVotedFor = new File(baseDir + "/" + "VotedFor.log");
            persistentStateVotedFor.createNewFile();
            persistentStateLastApplied = new File(baseDir + "/" + "LastApplied.log");
            persistentStateLastApplied.createNewFile();
            persistentStateLog = new File(baseDir + "/" + "Log.log");
            persistentStateLog.createNewFile();
        } catch (IOException e) {
            throw new PersistentStateException(e.getMessage()+
                    "\nCannot create new persistent state: " + baseDir);
        }
    }

    /**
     * Load persistent state on disk and check that they are well-formed.
     */
    private synchronized void loadPersistentStateFromDisk() {
        try {
            persistentStateCurrentTerm = new File(baseDir + "/" + "CurrentTerm.log");
            persistentStateVotedFor = new File(baseDir + "/" + "VotedFor.log");
            persistentStateLastApplied = new File(baseDir + "/" + "LastApplied.log");
            persistentStateLog = new File(baseDir + "/" + "Log.log");
            BufferedReader reader;
            reader = new BufferedReader( new InputStreamReader(
                    new FileInputStream(persistentStateCurrentTerm)));
            currentTerm = Integer.parseInt(reader.readLine());
            reader.close();
            reader = new BufferedReader( new InputStreamReader(
                    new FileInputStream(persistentStateVotedFor)));
            votedFor = reader.readLine();
            reader.close();
            votedFor = votedFor == "NULL" ? null : votedFor;
            reader = new BufferedReader( new InputStreamReader(
                    new FileInputStream(persistentStateLastApplied)));
            lastApplied = Integer.parseInt(reader.readLine());
            reader.close();
            reader = new BufferedReader( new InputStreamReader(
                    new FileInputStream(persistentStateLog)));
            readlogs(reader);
            reader.close();
        } catch (IOException | NumberFormatException e) {
            throw new PersistentStateException(e.getMessage() +
                    "\nCannot load persistent state from disk: " + baseDir);
        }
    }
    
    private synchronized void readlogs(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            log.add(new LogEntry(line));
        }
    }
    
    private String stringifyLogs(ArrayList<LogEntry> logEntries) {
        String stringifiedLogs = "";
        for (LogEntry logEntry: logEntries) {
            stringifiedLogs += logEntry.toString() + "\n";
        }
        return stringifiedLogs.substring(0, stringifiedLogs.length()-1);
    }

    private synchronized void persistToDisk(String data, File file, boolean append) {
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file, append)));
            writer.write(data == null ? "NULL" : data);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new PersistentStateException(e.getMessage() +
                    "\nCannot persist to disk: "+baseDir);
        }
    }

    /**
     * Set current term and then write to persistent state on disk.
     * @param currentTerm See top of class file
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void setTerm(int currentTerm) {
        this.currentTerm = currentTerm;
        persistToDisk(Integer.toString(currentTerm), persistentStateCurrentTerm,
                false);
    }

    /**
     * Set id of server you voted for and then write to persistent state on disk
     * @param votedFor See top of class file
     * @throws PersistentStateException If the state fails to persist to disk
     */
    public synchronized void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
        persistToDisk(votedFor, persistentStateVotedFor, false);
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

        int numLogsToTruncate = this.log.size() - index;
        this.log.subList(index, this.log.size()).clear();
        RandomAccessFile f;
        try {
            f = new RandomAccessFile(persistentStateLog, "rw");
            long length = f.length() - 1;
            byte b;
            while (numLogsToTruncate > 0) {
                do {
                    length -= 1;
                    f.seek(length);
                    b = f.readByte();
                  } while(b != 10);
                numLogsToTruncate --;
            }
            f.setLength(length+1);
            f.close();
        } catch (IOException e) {
            throw new PersistentStateException(e.getMessage()+
                    "\nCannot truncate logs: "+baseDir);
        }
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
        persistToDisk(stringifyLogs(newEntries), persistentStateLog,
                true);
    }

    /**
     * Increment the last applied index variable and persist state to disk.
     */
    public synchronized void incrementLastApplied() {
        this.lastApplied += 1;
        persistToDisk(Integer.toString(lastApplied), persistentStateLastApplied,
                false);
    }
}
