package misc;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A log entry in a server's command log.
 */
public class LogEntry implements Serializable {
    /**
     * Index of log entry in the log.
     */
    public int index;
    /**
     * Term of the leader when it sent this log.
     */
    public int term;
    /**
     * Command to be executed on each server.
     */
    public String command;
    
    /**
     * A Pattern instance that contains the regular expression used to parse
     * a log entry string.
     */
    private final static Pattern entryPattern = Pattern.compile(
            "LogEntry \\[index=(\\d+), term=(\\d+), command=(.*)\\]");

    /**
     * @param index   see top of class file
     * @param command see top of class file
     * @param term    see top of class file
     */
    public LogEntry(int index, int term, String command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }
    
    /**
     * An alternative constructor that populates a LogEntry instance by
     * parsing a string representation of a log entry.
     * @param stringifiedLogEntry String that represents a log entry.
     */
    public LogEntry(String stringifiedLogEntry) {
        Matcher m = entryPattern.matcher(stringifiedLogEntry);
        if (!m.matches()) {
            throw new PersistentStateException(
                    "Cannot parse log entry: " + stringifiedLogEntry);
        }
        this.index = Integer.parseInt(m.group(1));
        this.term = Integer.parseInt(m.group(2));
        this.command = m.group(3);
    }
    
    @Override
    public String toString() {
        return "LogEntry [index=" + index + ", term=" + term + ", command="
                + command + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        
        if (!(obj instanceof LogEntry)) {
            return false;
        }
        
        LogEntry other = (LogEntry) obj;
        return this.index == other.index && this.term == other.term;
    }
    
}
